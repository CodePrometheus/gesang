use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use gesang_api::{Agent, ApiProblem, CapabilityScope, CommandReceipt, CommandSubmission, Grant};
use gesang_core::{
    Admission, AnchorHead, CommandRequirements, CoordinationStore, InstanceIntent, InstancePhase,
    InstancePlan, Owner, PendingCommand, PreviousWriter, Result, Runtime, RuntimeClaim,
    RuntimeRetirement, Store, WakeClaim,
};
use serde_json::Value;
use uuid::Uuid;

pub(super) struct HostCoordination {
    pub(super) store: Store,
    pub(super) verified: Mutex<Option<Runtime>>,
}

macro_rules! forward_coordination {
    ($($name:ident($($argument:ident: $kind:ty),* $(,)?) -> $result:ty;)*) => {
        #[async_trait]
        impl CoordinationStore for HostCoordination {
            $(
                async fn $name(&self, $($argument: $kind),*) -> $result {
                    self.store.$name($($argument),*).await
                }
            )*

            async fn verify_generation(&self, runtime: &Runtime) -> Result<()> {
                self.store.verify_generation(runtime).await?;
                *self.verified.lock().unwrap() = Some(runtime.clone());
                Ok(())
            }
        }
    };
}

forward_coordination! {
    agent(owner: &Owner, id: Uuid) -> Result<Agent>;
    enqueue(
        owner: &Owner,
        agent_id: Uuid,
        submission: &CommandSubmission,
        requirements: &CommandRequirements,
    ) -> Result<CommandReceipt>;
    claim_command(
        claimant: Uuid,
        duration: TimeDelta,
    ) -> Result<Option<PendingCommand>>;
    accept_command(command: &PendingCommand, receipt: &Value) -> Result<()>;
    reject_command(command: &PendingCommand, problem: &ApiProblem) -> Result<()>;
    defer_command(command: &PendingCommand, due_at: DateTime<Utc>) -> Result<()>;
    authorize(
        agent_id: Uuid,
        grant_id: Uuid,
        version: i64,
        scopes: &[CapabilityScope],
    ) -> Result<Grant>;
    claim_runtime(
        agent_id: Uuid,
        controller_id: Uuid,
        duration: TimeDelta,
    ) -> Result<RuntimeClaim>;
    instance_intent(claim: &RuntimeClaim) -> Result<Option<InstanceIntent>>;
    begin_instance(
        claim: &RuntimeClaim,
        plan: &InstancePlan,
    ) -> Result<InstanceIntent>;
    verify_instance_intent(intent: &InstanceIntent) -> Result<()>;
    admit_instance(
        intent: &InstanceIntent,
        admission: &Admission,
    ) -> Result<InstanceIntent>;
    transition_instance(
        intent: &InstanceIntent,
        next: InstancePhase,
    ) -> Result<InstanceIntent>;
    finish_instance(
        intent: &InstanceIntent,
        proof: &PreviousWriter,
        completed_head: Option<&AnchorHead>,
        problem: Option<&ApiProblem>,
    ) -> Result<InstanceIntent>;
    advance_fenced_generation(claim: &RuntimeClaim) -> Result<RuntimeClaim>;
    claim_retirement(
        claimant: Uuid,
        duration: TimeDelta,
    ) -> Result<Option<RuntimeRetirement>>;
    verify_retirement(retirement: &RuntimeRetirement) -> Result<()>;
    finish_retirement(retirement: &RuntimeRetirement) -> Result<()>;
    claim_wake(claimant: Uuid, duration: TimeDelta) -> Result<Option<WakeClaim>>;
    schedule(agent_id: Uuid, due_at: DateTime<Utc>) -> Result<()>;
    settle_wake(claim: &WakeClaim, next: Option<DateTime<Utc>>) -> Result<()>;
}
