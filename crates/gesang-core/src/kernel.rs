use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use gesang_api::{ApiProblem, CommandAuthorization, CommandReceipt, CommandSubmission};
use jiff::{SignedDuration, Timestamp};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AdoptionBoundary, Configuration, ConfigurationAccess, ConfigurationCenter, ConfigurationScope,
    CoordinationStore, Dispatch, EngineBridge, EnginePreparation, Error, ExecutionResponse,
    InstanceObservation, InstancePhase, InstanceProvider, InstancePurpose, Owner,
    PreparationProgress, PreviousWriter, RequiredAuthority, Result, Runtime, Store,
};

/// The source of a failure while recovering persisted work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryLane {
    /// The host's published runtime policy could not be resolved.
    Policy,
    /// A persisted command could not be delivered or reconciled.
    Delivery,
    /// A due execution instance could not be resumed.
    Wake,
    /// A prior execution instance could not be verified retired.
    Retirement,
}

/// Explicit deployment timing policy; no hidden lease, retry, or history limits are supplied.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimePolicy {
    /// Controller lease and corresponding provider retention interval.
    #[serde(rename = "leaseMs", with = "duration_ms")]
    pub lease: SignedDuration,
    /// Maximum ownership interval for one delivery or wake attempt before reconciliation.
    #[serde(rename = "deliveryLeaseMs", with = "duration_ms")]
    pub delivery_lease: SignedDuration,
    /// Earliest next attempt after uncertain transport or provider results.
    #[serde(rename = "retryDelayMs", with = "duration_ms")]
    pub retry_delay: SignedDuration,
    /// Persistent recovery deadline, shorter than the controller lease.
    #[serde(rename = "recoveryIntervalMs", with = "duration_ms")]
    pub recovery_interval: SignedDuration,
}

impl RuntimePolicy {
    /// Rejects nonpositive timing policy and recovery deadlines that exceed the lease.
    pub fn validate(&self) -> Result<()> {
        for duration in [
            self.lease,
            self.delivery_lease,
            self.retry_delay,
            self.recovery_interval,
        ] {
            if duration.as_millis() <= 0
                || duration.subsec_nanos() % 1_000_000 != 0
                || Timestamp::now().checked_add(duration).is_err()
            {
                return Err(Error::InvalidCommand(
                    "runtime policy durations must be positive exact milliseconds with representable deadlines".into(),
                ));
            }
        }
        if self.recovery_interval >= self.lease {
            return Err(Error::InvalidCommand(
                "recovery interval must be shorter than the runtime lease".into(),
            ));
        }
        Ok(())
    }
}

impl Configuration for RuntimePolicy {
    const COMPONENT: &'static str = "host.runtime";
    const SCHEMA_VERSION: u32 = 1;
    const ADOPTION_BOUNDARY: AdoptionBoundary = AdoptionBoundary::Live;

    fn validate(
        &self,
        scope: &ConfigurationScope,
        secrets: &std::collections::BTreeSet<String>,
    ) -> Result<()> {
        if scope != &ConfigurationScope::Deployment || !secrets.is_empty() {
            return Err(Error::Forbidden);
        }
        self.validate()
    }
}

mod duration_ms {
    use jiff::SignedDuration;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(
        value: &SignedDuration,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let millis = i64::try_from(value.as_millis()).map_err(serde::ser::Error::custom)?;
        if millis == i64::MIN {
            return Err(serde::ser::Error::custom("duration is out of range"));
        }
        serializer.serialize_i64(millis)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<SignedDuration, D::Error> {
        let value = i64::deserialize(deserializer)?;
        if value == i64::MIN {
            return Err(serde::de::Error::custom("duration is out of range"));
        }
        Ok(SignedDuration::from_millis(value))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod timing_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn runtime_policy_keeps_signed_integer_milliseconds() {
        let mut fixture = json!({
            "leaseMs": 60000,
            "deliveryLeaseMs": 10000,
            "retryDelayMs": -1,
            "recoveryIntervalMs": 5000,
        });
        let policy: RuntimePolicy = serde_json::from_value(fixture.clone()).unwrap();
        assert_eq!(policy.retry_delay, SignedDuration::from_millis(-1));
        assert_eq!(serde_json::to_value(&policy).unwrap(), fixture);
        assert!(policy.validate().is_err());

        fixture["retryDelayMs"] = json!(i64::MIN);
        assert!(serde_json::from_value::<RuntimePolicy>(fixture.clone()).is_err());

        fixture["retryDelayMs"] = json!(250);
        let mut policy: RuntimePolicy = serde_json::from_value(fixture.clone()).unwrap();
        assert!(policy.validate().is_ok());
        policy.retry_delay += SignedDuration::from_nanos(1);
        assert!(policy.validate().is_err());
        assert_eq!(serde_json::to_value(policy).unwrap(), fixture);
    }
}

#[derive(Debug)]
enum PolicySource {
    Fixed(RuntimePolicy),
    Published {
        center: ConfigurationCenter,
        access: ConfigurationAccess,
    },
}

impl PolicySource {
    async fn get(&self, consumer_id: Uuid) -> Result<RuntimePolicy> {
        match self {
            Self::Fixed(policy) => Ok(policy.clone()),
            Self::Published { center, access } => Ok(center
                .current_for_consumer::<RuntimePolicy>(
                    access,
                    &ConfigurationScope::Deployment,
                    consumer_id,
                )
                .await?
                .values),
        }
    }
}

/// Shared authorization, delivery, and runtime coordination used by independent hosts.
pub struct Kernel<S: CoordinationStore + ?Sized = Store> {
    store: Arc<S>,
    provider: Arc<dyn InstanceProvider>,
    engine: Arc<dyn EngineBridge>,
    controller_id: Uuid,
    policy: PolicySource,
    admission_locks: Mutex<HashMap<Uuid, Weak<tokio::sync::Mutex<()>>>>,
}

impl<S: CoordinationStore + ?Sized> std::fmt::Debug for Kernel<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Kernel")
            .field("controller_id", &self.controller_id)
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl Kernel {
    /// Constructs one controller with the built-in coordination store and explicit policy.
    pub fn new(
        store: Store,
        provider: Arc<dyn InstanceProvider>,
        engine: Arc<dyn EngineBridge>,
        policy: RuntimePolicy,
    ) -> Result<Self> {
        Self::with_coordination_store(Arc::new(store), provider, engine, policy)
    }

    /// Atomically verifies an external caller's authority and accepts platform delivery.
    ///
    /// The external check and the kernel's own owner and delegation checks use
    /// the command's admission transaction.
    pub async fn submit_checked(
        &self,
        owner: &Owner,
        agent_id: Uuid,
        command: &CommandSubmission,
        check: &dyn crate::CommandAdmissionCheck,
    ) -> Result<CommandReceipt> {
        let requirements = self
            .engine
            .authorize_command(&command.command, &command.authorization)?;
        self.store
            .enqueue_checked(owner, agent_id, command, &requirements, Some(check))
            .await
    }

    /// Uses the built-in store and records each controller's published policy adoption.
    pub async fn with_configuration(
        store: Store,
        provider: Arc<dyn InstanceProvider>,
        engine: Arc<dyn EngineBridge>,
        center: ConfigurationCenter,
        access: ConfigurationAccess,
    ) -> Result<Self> {
        Self::with_coordination_store_and_configuration(
            Arc::new(store),
            provider,
            engine,
            center,
            access,
        )
        .await
    }
}

impl<S: CoordinationStore + ?Sized> Kernel<S> {
    /// Constructs a controller with host-supplied durable coordination and fixed policy.
    pub fn with_coordination_store(
        store: Arc<S>,
        provider: Arc<dyn InstanceProvider>,
        engine: Arc<dyn EngineBridge>,
        policy: RuntimePolicy,
    ) -> Result<Self> {
        policy.validate()?;
        Ok(Self::assemble(
            store,
            provider,
            engine,
            Uuid::new_v4(),
            PolicySource::Fixed(policy),
        ))
    }

    /// Uses host-supplied durable coordination with published runtime settings.
    pub async fn with_coordination_store_and_configuration(
        store: Arc<S>,
        provider: Arc<dyn InstanceProvider>,
        engine: Arc<dyn EngineBridge>,
        center: ConfigurationCenter,
        access: ConfigurationAccess,
    ) -> Result<Self> {
        let controller_id = Uuid::new_v4();
        let policy = PolicySource::Published { center, access };
        policy.get(controller_id).await?;
        Ok(Self::assemble(
            store,
            provider,
            engine,
            controller_id,
            policy,
        ))
    }

    fn assemble(
        store: Arc<S>,
        provider: Arc<dyn InstanceProvider>,
        engine: Arc<dyn EngineBridge>,
        controller_id: Uuid,
        policy: PolicySource,
    ) -> Self {
        Self {
            store,
            provider,
            engine,
            controller_id,
            policy,
            admission_locks: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the store supplied by the host.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Returns the effective host timing policy from the same publication source used by coordination.
    pub async fn runtime_policy(&self) -> Result<RuntimePolicy> {
        self.policy.get(self.controller_id).await
    }

    /// Validates an upstream command and durably accepts platform delivery.
    pub async fn submit(
        &self,
        owner: &Owner,
        agent_id: Uuid,
        command: &CommandSubmission,
    ) -> Result<CommandReceipt> {
        let requirements = self
            .engine
            .authorize_command(&command.command, &command.authorization)?;
        self.store
            .enqueue(owner, agent_id, command, &requirements)
            .await
    }

    /// Serializes local admission, verifies liveness, and requires independent replacement fencing.
    pub async fn ensure_runtime(&self, agent_id: Uuid) -> Result<Runtime> {
        let policy = self.policy.get(self.controller_id).await?;
        self.ensure_runtime_with_policy(agent_id, &policy).await
    }

    /// Resolves the current runtime only within the authenticated owner's agent.
    /// Missing and foreign agents return `Error::NotFound` before allocation.
    /// Availability and fencing errors propagate from `Self::ensure_runtime`.
    pub async fn ensure_owner_runtime(&self, owner: &Owner, agent_id: Uuid) -> Result<Runtime> {
        self.store.agent(owner, agent_id).await?;
        self.ensure_runtime(agent_id).await
    }

    async fn ensure_runtime_with_policy(
        &self,
        agent_id: Uuid,
        policy: &RuntimePolicy,
    ) -> Result<Runtime> {
        let admission_lock = {
            let mut locks = self
                .admission_locks
                .lock()
                .map_err(|_| Error::Uncertain("admission coordination is unavailable".into()))?;
            if let Some(lock) = locks.get(&agent_id).and_then(Weak::upgrade) {
                lock
            } else {
                locks.retain(|_, lock| lock.strong_count() > 0);
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(agent_id, Arc::downgrade(&lock));
                lock
            }
        };
        let _guard = admission_lock.lock().await;
        let claim = self
            .store
            .claim_runtime(agent_id, self.controller_id, policy.lease)
            .await?;
        let mut intent = match self.store.instance_intent(&claim).await? {
            Some(intent) => intent,
            None => {
                let plan = self.provider.plan(&claim).await?;
                self.store.begin_instance(&claim, &plan).await?
            }
        };
        loop {
            self.store.verify_instance_intent(&intent).await?;
            match intent.phase {
                InstancePhase::Allocating => {
                    let admission = self.provider.allocate(&intent).await?;
                    intent = self.store.admit_instance(&intent, &admission).await?;
                }
                InstancePhase::Starting
                | InstancePhase::Migrating
                | InstancePhase::Initializing => {
                    let runtime = intent.runtime.as_ref().ok_or(Error::FencingRequired)?;
                    self.provider
                        .retain_until(runtime, claim.expires_at)
                        .await?;
                    match self.provider.activate(&intent).await? {
                        PreparationProgress::Pending => return Err(Error::RuntimeNotReady),
                        PreparationProgress::ExecutionAvailable
                            if intent.plan.purpose == InstancePurpose::Execution =>
                        {
                            intent = self
                                .store
                                .transition_instance(&intent, InstancePhase::Unlocking)
                                .await?;
                        }
                        PreparationProgress::MigrationComplete(head)
                            if intent.plan.purpose == InstancePurpose::Migration =>
                        {
                            if self.provider.observe(runtime).await? != InstanceObservation::Stopped
                            {
                                return Err(Error::FencingRequired);
                            }
                            intent = self
                                .store
                                .finish_instance(
                                    &intent,
                                    &PreviousWriter::Terminated(intent.id),
                                    Some(&head),
                                    None,
                                )
                                .await?;
                        }
                        PreparationProgress::InitializationComplete(head)
                            if intent.plan.purpose == InstancePurpose::Initialization =>
                        {
                            if self.provider.observe(runtime).await? != InstanceObservation::Stopped
                            {
                                return Err(Error::FencingRequired);
                            }
                            intent = self
                                .store
                                .finish_instance(
                                    &intent,
                                    &PreviousWriter::Terminated(intent.id),
                                    Some(&head),
                                    None,
                                )
                                .await?;
                        }
                        PreparationProgress::Stopped(problem) => {
                            if self.provider.observe(runtime).await? != InstanceObservation::Stopped
                            {
                                return Err(Error::FencingRequired);
                            }
                            intent = self
                                .store
                                .finish_instance(
                                    &intent,
                                    &PreviousWriter::Terminated(intent.id),
                                    None,
                                    Some(&problem),
                                )
                                .await?;
                        }
                        _ => {
                            return Err(Error::InvalidCommand(
                                "provider returned progress for another instance purpose".into(),
                            ));
                        }
                    }
                }
                InstancePhase::Unlocking => {
                    let runtime = intent.runtime.as_ref().ok_or(Error::FencingRequired)?;
                    match self.provider.observe(runtime).await? {
                        InstanceObservation::Stopped => {
                            intent = self
                                .store
                                .finish_instance(
                                    &intent,
                                    &PreviousWriter::Terminated(intent.id),
                                    None,
                                    None,
                                )
                                .await?;
                            continue;
                        }
                        InstanceObservation::Unknown => {
                            return Err(Error::Uncertain(
                                "preparing instance liveness is unknown".into(),
                            ));
                        }
                        InstanceObservation::Live(_) => {}
                    }
                    self.provider
                        .retain_until(runtime, claim.expires_at)
                        .await?;
                    match self.engine.prepare(runtime).await? {
                        EnginePreparation::Pending => return Err(Error::RuntimeNotReady),
                        EnginePreparation::Ready => {
                            if !matches!(
                                self.provider.observe(runtime).await?,
                                InstanceObservation::Live(_)
                            ) {
                                return Err(Error::Uncertain(
                                    "prepared instance liveness is unknown".into(),
                                ));
                            }
                            intent = self
                                .store
                                .transition_instance(&intent, InstancePhase::Ready)
                                .await?;
                        }
                    }
                }
                InstancePhase::Ready => {
                    let runtime = intent.runtime.as_ref().ok_or(Error::FencingRequired)?;
                    match self.provider.observe(runtime).await? {
                        InstanceObservation::Live(_) => {
                            self.provider
                                .retain_until(runtime, claim.expires_at)
                                .await?;
                            self.store.verify_generation(runtime).await?;
                            return Ok(runtime.clone());
                        }
                        InstanceObservation::Stopped => {
                            intent = self
                                .store
                                .finish_instance(
                                    &intent,
                                    &PreviousWriter::Terminated(intent.id),
                                    None,
                                    None,
                                )
                                .await?;
                        }
                        InstanceObservation::Unknown => {
                            return Err(Error::Uncertain("runtime liveness is unknown".into()));
                        }
                    }
                }
                InstancePhase::Draining => {
                    let runtime = intent.runtime.as_ref().ok_or(Error::FencingRequired)?;
                    self.provider.stop(runtime).await?;
                    if self.provider.observe(runtime).await? != InstanceObservation::Stopped {
                        return Err(Error::FencingRequired);
                    }
                    intent = self
                        .store
                        .finish_instance(
                            &intent,
                            &PreviousWriter::Terminated(intent.id),
                            None,
                            None,
                        )
                        .await?;
                }
                InstancePhase::Stopped => {
                    let mut stopped = intent.claim.clone();
                    stopped.previous = intent.runtime.clone().or(stopped.previous);
                    self.store.advance_fenced_generation(&stopped).await?;
                    return Err(Error::RuntimeNotReady);
                }
            }
        }
    }

    /// Drives durable delivery, wake and retirement until the host signals shutdown.
    ///
    /// An initial policy must be available. Later failures are reported and retried
    /// according to the last adopted policy. Shutdown finishes the current pass before
    /// returning; the host owns the provider and execution connection shutdown.
    pub async fn run_recovery(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        mut report: impl FnMut(RecoveryLane, &Error) + Send,
    ) -> Result<()> {
        if recovery_stopped(&shutdown) {
            return Ok(());
        }
        let mut policy = self.runtime_policy().await?;
        while !recovery_stopped(&shutdown) {
            match self.runtime_policy().await {
                Ok(current) => {
                    policy = current;
                    if recovery_stopped(&shutdown) {
                        break;
                    }
                    let (delivery, wake, retirement) = tokio::join!(
                        self.dispatch_one_with_policy(&policy),
                        self.wake_one_with_policy(&policy),
                        self.retire_one_with_policy(&policy)
                    );
                    let mut progress = false;
                    let mut failed = false;
                    for (lane, outcome) in [
                        (RecoveryLane::Delivery, delivery),
                        (RecoveryLane::Wake, wake),
                        (RecoveryLane::Retirement, retirement),
                    ] {
                        match outcome {
                            Ok(worked) => progress |= worked,
                            Err(error) => {
                                failed = true;
                                report(lane, &error);
                            }
                        }
                    }
                    if progress && !failed {
                        tokio::task::yield_now().await;
                        continue;
                    }
                }
                Err(error) => report(RecoveryLane::Policy, &error),
            }
            let delay = Duration::try_from(policy.retry_delay.min(policy.recovery_interval))
                .map_err(|_| Error::InvalidCommand("recovery delay is out of range".into()))?;
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                change = shutdown.changed() => {
                    if change.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Processes one durable retirement, including launches that never reached runtime admission.
    pub async fn retire_one(&self) -> Result<bool> {
        let policy = self.runtime_policy().await?;
        self.retire_one_with_policy(&policy).await
    }

    async fn retire_one_with_policy(&self, policy: &RuntimePolicy) -> Result<bool> {
        let Some(retirement) = self
            .store
            .claim_retirement(Uuid::new_v4(), policy.delivery_lease)
            .await?
        else {
            return Ok(false);
        };
        self.store.verify_retirement(&retirement).await?;
        self.provider.retire(&retirement).await?;
        self.store.finish_retirement(&retirement).await?;
        Ok(true)
    }

    /// Reconciles and delivers at most one command, preserving uncertainty and request identity.
    pub async fn dispatch_one(&self) -> Result<bool> {
        let policy = self.runtime_policy().await?;
        self.dispatch_one_with_policy(&policy).await
    }

    async fn dispatch_one_with_policy(&self, policy: &RuntimePolicy) -> Result<bool> {
        let claimant = Uuid::new_v4();
        let Some(command) = self
            .store
            .claim_command(claimant, policy.delivery_lease)
            .await?
        else {
            return Ok(false);
        };
        let result = self.deliver(&command, policy).await;
        if result.is_err() {
            self.store
                .defer_command(&command, Timestamp::now() + policy.retry_delay)
                .await?;
        }
        result?;
        Ok(true)
    }

    async fn deliver(&self, command: &crate::PendingCommand, policy: &RuntimePolicy) -> Result<()> {
        let runtime = self
            .ensure_runtime_with_policy(command.agent_id, policy)
            .await?;
        if let Some(receipt) = self.engine.request(&runtime, command.request_id).await? {
            return self.store.accept_command(command, &receipt).await;
        }
        let admitted = async {
            let requirements = self
                .engine
                .authorize_command(&command.command, &command.authorization)?;
            match command.authorization {
                CommandAuthorization::Owner
                    if requirements.authority == RequiredAuthority::OwnerControl
                        && requirements.scopes.is_empty() =>
                {
                    Ok(())
                }
                CommandAuthorization::Grant { id, version }
                    if requirements.authority == RequiredAuthority::TaskGrant =>
                {
                    self.store
                        .authorize(command.agent_id, id, version, &requirements.scopes)
                        .await
                        .map(|_| ())
                }
                _ => Err(Error::Forbidden),
            }
        }
        .await;
        match admitted {
            Ok(_) => (),
            Err(Error::Forbidden) => {
                return self
                    .store
                    .reject_command(
                        command,
                        &ApiProblem {
                            code: "grant_revoked".into(),
                            message: "Task authorization is no longer valid".into(),
                        },
                    )
                    .await;
            }
            Err(error) => return Err(error),
        }
        self.store.verify_generation(&runtime).await?;
        self.store
            .schedule(
                command.agent_id,
                Timestamp::now() + policy.recovery_interval,
            )
            .await?;
        let dispatch = Dispatch {
            request_id: command.request_id,
            runtime,
            authorization: command.authorization.clone(),
            command: command.command.clone(),
        };
        match self.engine.execute(&dispatch).await? {
            ExecutionResponse::Accepted(receipt) => {
                self.store.accept_command(command, &receipt).await
            }
            ExecutionResponse::Rejected(problem) => {
                self.store.reject_command(command, &problem).await
            }
        }
    }

    /// Resumes one persistently due agent and conditionally settles its exact wake revision.
    pub async fn wake_one(&self) -> Result<bool> {
        let policy = self.runtime_policy().await?;
        self.wake_one_with_policy(&policy).await
    }

    async fn wake_one_with_policy(&self, policy: &RuntimePolicy) -> Result<bool> {
        let Some(claim) = self
            .store
            .claim_wake(Uuid::new_v4(), policy.delivery_lease)
            .await?
        else {
            return Ok(false);
        };
        let result = self.resume_agent(claim.agent_id, policy).await;
        match result {
            Ok(next) => self.store.settle_wake(&claim, next).await?,
            Err(error) => {
                self.store
                    .settle_wake(&claim, Some(Timestamp::now() + policy.retry_delay))
                    .await?;
                return Err(error);
            }
        }
        Ok(true)
    }

    async fn resume_agent(
        &self,
        agent_id: Uuid,
        policy: &RuntimePolicy,
    ) -> Result<Option<Timestamp>> {
        let runtime = self.ensure_runtime_with_policy(agent_id, policy).await?;
        self.store.verify_generation(&runtime).await?;
        self.engine.resume(&runtime).await?;
        let wake = self.engine.wake(&runtime).await?;
        let recovery = Timestamp::now() + policy.recovery_interval;
        let next = if wake.active {
            Some(wake.at.map_or(recovery, |at| at.min(recovery)))
        } else {
            wake.at.map(|at| at.min(recovery))
        };
        Ok(next)
    }

    /// Reads the authenticated owner's goal directly from the execution authority.
    pub async fn snapshot(
        &self,
        owner: &Owner,
        agent_id: Uuid,
        goal_id: &str,
    ) -> Result<serde_json::Value> {
        let runtime = self.ensure_owner_runtime(owner, agent_id).await?;
        self.engine.snapshot(&runtime, goal_id).await
    }
}

fn recovery_stopped(shutdown: &tokio::sync::watch::Receiver<bool>) -> bool {
    *shutdown.borrow() || shutdown.has_changed().is_err()
}
