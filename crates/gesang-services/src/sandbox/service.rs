mod state;
mod store;

use std::{collections::BTreeMap, num::NonZeroU32, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use gesang_core::{InstalledPlugin, ResourceBinding, Store};
use gesang_plugins::ServiceEndpoint;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, watch};
use uuid::Uuid;

use crate::{Caller, Error, Result, ServiceAccess};

use super::{
    ContainerConnection, ContainerId, ContainerProfile, ContainerRuntime, ContainerState,
    CreateSandbox, OperationStatus, Sandbox, SandboxOperation, SandboxState, SandboxView,
    Termination, create_scope, observe_scope, sandbox_interface, stop_scope,
};
use state::{Action, Instance, Launch, Removal};

/// Nonsecret provider settings stored in an immutable installation revision.
#[derive(Clone, Debug, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContainerSettings {
    /// The original protected runtime connection and provider identity.
    pub connection: ContainerConnection,
    /// Exact profiles exposed by this installed version.
    pub profiles: BTreeMap<String, ContainerProfile>,
}

impl ContainerSettings {
    /// Returns the schema derived from the same types used to open the provider.
    #[must_use]
    pub fn schema() -> serde_json::Value {
        use schemars::transform::Transform;

        let mut schema = schemars::schema_for!(Self);
        schemars::transform::RestrictFormats::default().transform(&mut schema);
        schema.to_value()
    }
}

/// A durable sandbox service bound to one immutable provider configuration.
///
/// API calls commit intent. A host-owned [`Self::run`] drives provider effects
/// independently of those callers. Each allocation and start is issued once;
/// uncertain outcomes are observed under their original physical identity.
#[derive(Debug)]
pub struct ContainerSandbox {
    platform: Store,
    access: ServiceAccess,
    runtime: ContainerRuntime,
    endpoint: ServiceEndpoint,
    profiles: BTreeMap<String, ContainerProfile>,
    wake: Notify,
}

impl ContainerSandbox {
    /// Opens an installed provider and validates every configured image and profile.
    ///
    /// The host must apply platform and service migrations before opening. This
    /// method starts no background tasks and provisions no user resources.
    ///
    /// # Errors
    ///
    /// Invalid profiles, inaccessible images, and provider identity changes reject
    /// initialization. A successfully constructed service still needs its runner.
    pub async fn open(
        platform: Store,
        installation: &InstalledPlugin,
        service: &str,
    ) -> Result<Self> {
        let endpoint = installation
            .endpoint(service, &sandbox_interface())
            .map_err(|_| {
                Error::Invalid("the installation does not provide the sandbox contract".into())
            })?;
        let settings: ContainerSettings =
            serde_json::from_value(installation.settings().values.clone())?;
        let ContainerSettings {
            connection,
            profiles,
        } = settings;
        if profiles.is_empty() || profiles.keys().any(String::is_empty) {
            return Err(Error::Invalid(
                "at least one named sandbox profile is required".into(),
            ));
        }
        let runtime = ContainerRuntime::connect(connection, endpoint.clone()).await?;
        for profile in profiles.values() {
            runtime.validate_profile(profile).await?;
        }
        let access = ServiceAccess::new(platform.clone());
        Ok(Self {
            platform,
            access,
            runtime,
            endpoint,
            profiles,
            wake: Notify::new(),
        })
    }

    /// Drives durable work until the host requests shutdown.
    ///
    /// The host owns this future and must observe its result. Work committed while
    /// no caller is connected is found by the recovery scan. `batch_size` controls
    /// each database page; scanning continues until the source returns no rows.
    ///
    /// # Errors
    ///
    /// Storage and invalid persisted-state errors stop the runner. Provider
    /// uncertainty is recorded on the operation and retried through observation.
    ///
    /// # Cancel safety
    ///
    /// Shutdown is checked between resources. Dropping this future during an
    /// external request leaves its issued marker intact for the next runner.
    pub async fn run(
        &self,
        interval: Duration,
        batch_size: NonZeroU32,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        if interval.is_zero() {
            return Err(Error::Invalid("recovery interval must be positive".into()));
        }
        loop {
            let mut after = None;
            loop {
                if *shutdown.borrow() {
                    return Ok(());
                }
                let page =
                    store::pending(self.platform.pool(), &self.endpoint, after, batch_size).await?;
                if page.is_empty() {
                    break;
                }
                for id in page {
                    if *shutdown.borrow() {
                        return Ok(());
                    }
                    self.reconcile(id).await?;
                    after = Some(id);
                }
            }
            tokio::select! {
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                () = self.wake.notified() => {}
                () = tokio::time::sleep(interval) => {}
            }
        }
    }

    /// Advances an original resource using durable issuance and positive observations.
    ///
    /// This is a trusted host recovery entry point. It rechecks the recorded grant
    /// before every new effect, while allowing observations after revocation.
    /// Calling it concurrently never grants another allocation or start attempt.
    pub async fn reconcile(&self, id: Uuid) -> Result<()> {
        loop {
            let instance = store::read(self.platform.pool(), id).await?;
            if instance.endpoint != self.endpoint {
                return Err(Error::NotFound);
            }
            if instance.removal == Removal::Stopped {
                return Ok(());
            }
            let progress = if instance.stop_requested {
                self.remove(&instance).await?
            } else {
                self.launch(&instance).await?
            };
            if !progress {
                return Ok(());
            }
        }
    }

    async fn route(&self, caller: &Caller, proposed: &ResourceBinding) -> Result<()> {
        let actual = self
            .platform
            .resource_binding(caller.authority(), proposed.id)
            .await?;
        if actual != *proposed
            || actual.endpoint != self.endpoint
            || actual.interface != sandbox_interface()
        {
            return Err(Error::NotFound);
        }
        Ok(())
    }

    fn profile(&self, name: &str) -> Result<&ContainerProfile> {
        self.profiles.get(name).ok_or(Error::NotFound)
    }

    async fn launch(&self, instance: &Instance) -> Result<bool> {
        let profile = self.profile(&instance.profile)?;
        match instance.launch {
            Launch::Queued => {
                if !self.claim_launch(instance, Launch::Issued).await? {
                    return Ok(false);
                }
                match self.runtime.allocate(instance.id, profile).await {
                    Ok(observed) => self.allocated(instance.id, observed.id).await,
                    Err(_) => self.unknown(instance.id, Action::Create).await,
                }
            }
            Launch::Issued => match self.runtime.find(instance.id, profile).await {
                Ok(Some(observed)) => self.allocated(instance.id, observed.id).await,
                Ok(None) | Err(_) => self.unknown(instance.id, Action::Create).await,
            },
            Launch::Allocated => {
                let physical = instance.physical_id.as_ref().ok_or(Error::InvalidState)?;
                if !self.claim_launch(instance, Launch::Starting).await? {
                    return Ok(false);
                }
                match self.runtime.start(physical, instance.id, profile).await {
                    Ok(observed) if observed.started_at.is_some() => {
                        self.started(instance.id).await
                    }
                    Ok(_) | Err(_) => self.unknown(instance.id, Action::Create).await,
                }
            }
            Launch::Starting => {
                let physical = instance.physical_id.as_ref().ok_or(Error::InvalidState)?;
                match self.runtime.observe(physical, instance.id, profile).await {
                    Ok(Some(observed)) if observed.started_at.is_some() => {
                        self.started(instance.id).await
                    }
                    Ok(_) | Err(_) => self.unknown(instance.id, Action::Create).await,
                }
            }
            Launch::Started | Launch::Failed => Ok(false),
        }
    }

    /// Commits one issuance after authorization, using the same credential-to-instance lock order as admission.
    async fn claim_launch(&self, observed: &Instance, next: Launch) -> Result<bool> {
        let required = create_scope(&self.endpoint, &observed.profile)?;
        let mut transaction = self.platform.begin().await?;
        let permission = self
            .access
            .resume_in(&mut transaction, &observed.creator, &required)
            .await;
        let authorized = match permission {
            Ok(_) => true,
            Err(Error::Forbidden | Error::Unauthorized) => false,
            Err(error) => return Err(error),
        };
        let mut current = store::lock(&mut transaction, observed.id).await?;
        if current.launch != observed.launch || current.stop_requested {
            return Ok(false);
        }
        if authorized {
            current.launch = next;
        } else {
            current.launch = Launch::Failed;
            store::finish(
                &mut transaction,
                current.id,
                Action::Create,
                OperationStatus::Failed,
                Some("authorization_unavailable"),
                None,
            )
            .await?;
        }
        store::save(&mut transaction, &current).await?;
        transaction.commit().await?;
        Ok(current.launch == next)
    }

    async fn allocated(&self, id: Uuid, physical: ContainerId) -> Result<bool> {
        let mut transaction = self.platform.begin().await?;
        let mut current = store::lock(&mut transaction, id).await?;
        if let Some(existing) = current.physical_id {
            return if existing == physical {
                Ok(false)
            } else {
                Err(Error::Conflict)
            };
        }
        if current.launch != Launch::Issued {
            return Err(Error::InvalidState);
        }
        current.physical_id = Some(physical);
        current.launch = Launch::Allocated;
        store::save(&mut transaction, &current).await?;
        transaction.commit().await?;
        Ok(true)
    }

    async fn started(&self, id: Uuid) -> Result<bool> {
        let mut transaction = self.platform.begin().await?;
        let mut current = store::lock(&mut transaction, id).await?;
        if current.launch != Launch::Starting || current.stop_requested {
            return Ok(false);
        }
        current.launch = Launch::Started;
        store::save(&mut transaction, &current).await?;
        store::finish(
            &mut transaction,
            id,
            Action::Create,
            OperationStatus::Succeeded,
            None,
            None,
        )
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    async fn remove(&self, instance: &Instance) -> Result<bool> {
        let profile = self.profile(&instance.profile)?;
        let Some(physical) = &instance.physical_id else {
            if matches!(instance.launch, Launch::Queued | Launch::Failed) {
                return self.stopped(instance.id).await;
            }
            return match self.runtime.find(instance.id, profile).await {
                Ok(Some(observed)) => self.allocated(instance.id, observed.id).await,
                Ok(None) | Err(_) => self.unknown(instance.id, Action::Stop).await,
            };
        };
        if instance.removal == Removal::Issued
            && matches!(
                self.runtime
                    .terminated(physical, instance.id, profile)
                    .await,
                Ok(Termination::Confirmed)
            )
        {
            return self.stopped(instance.id).await;
        }
        if !self.claim_removal(instance).await? {
            return Ok(false);
        }
        match self.runtime.terminate(physical, instance.id, profile).await {
            Ok(Termination::Confirmed) => self.stopped(instance.id).await,
            Ok(Termination::Unknown) | Err(_) => self.unknown(instance.id, Action::Stop).await,
        }
    }

    async fn claim_removal(&self, instance: &Instance) -> Result<bool> {
        let mut after = None;
        while let Some(operation) =
            store::next_stop(self.platform.pool(), instance.id, after).await?
        {
            after = Some(operation.request_id);
            let recorded = &operation.caller.0;
            if recorded.binding != instance.creator.binding
                || recorded.principal != instance.creator.principal
            {
                return Err(Error::InvalidState);
            }
            let mut transaction = self.platform.begin().await?;
            match self
                .access
                .resume_in(&mut transaction, recorded, &stop_scope())
                .await
            {
                Ok(_) => {
                    let mut current = store::lock(&mut transaction, instance.id).await?;
                    if current.removal == Removal::Stopped {
                        return Ok(false);
                    }
                    if current.physical_id != instance.physical_id || !current.stop_requested {
                        return Err(Error::InvalidState);
                    }
                    current.removal = Removal::Issued;
                    current.stopper = Some(recorded.clone());
                    store::save(&mut transaction, &current).await?;
                    transaction.commit().await?;
                    return Ok(true);
                }
                Err(Error::Forbidden | Error::Unauthorized) => {
                    let current = store::lock(&mut transaction, instance.id).await?;
                    if current.removal == Removal::Stopped {
                        return Ok(false);
                    }
                    let status = if current.removal == Removal::Issued {
                        OperationStatus::Unknown
                    } else {
                        OperationStatus::Failed
                    };
                    store::finish(
                        &mut transaction,
                        instance.id,
                        Action::Stop,
                        status,
                        Some("authorization_unavailable"),
                        Some(operation.request_id),
                    )
                    .await?;
                    transaction.commit().await?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    }

    async fn stopped(&self, id: Uuid) -> Result<bool> {
        let mut transaction = self.platform.begin().await?;
        let mut current = store::lock(&mut transaction, id).await?;
        if current.removal == Removal::Stopped {
            return Ok(false);
        }
        if current.physical_id.is_none()
            && !matches!(current.launch, Launch::Queued | Launch::Failed)
        {
            return Err(Error::InvalidState);
        }
        current.removal = Removal::Stopped;
        if current.launch != Launch::Started {
            current.launch = Launch::Failed;
            store::finish(
                &mut transaction,
                id,
                Action::Create,
                OperationStatus::Failed,
                Some("stopped_before_ready"),
                None,
            )
            .await?;
        }
        store::save(&mut transaction, &current).await?;
        store::finish(
            &mut transaction,
            id,
            Action::Stop,
            OperationStatus::Succeeded,
            None,
            None,
        )
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    async fn unknown(&self, id: Uuid, action: Action) -> Result<bool> {
        let mut transaction = self.platform.begin().await?;
        store::finish(
            &mut transaction,
            id,
            action,
            OperationStatus::Unknown,
            Some("provider_outcome_unknown"),
            None,
        )
        .await?;
        transaction.commit().await?;
        Ok(false)
    }
}

#[async_trait]
impl Sandbox for ContainerSandbox {
    async fn create(
        &self,
        caller: &Caller,
        binding: &ResourceBinding,
        request: &CreateSandbox,
    ) -> Result<SandboxOperation> {
        self.route(caller, binding).await?;
        if binding.request_id != request.request_id || binding.request_digest != request.digest() {
            return Err(Error::Conflict);
        }
        self.profile(&request.profile)?;
        let mut transaction = self.platform.begin().await?;
        self.access
            .authorize_in(
                &mut transaction,
                caller,
                &create_scope(&self.endpoint, &request.profile)?,
            )
            .await?;
        let recorded = caller.stored();
        store::insert(&mut transaction, binding, &request.profile, &recorded).await?;
        let instance = store::lock(&mut transaction, binding.id).await?;
        if instance.endpoint != binding.endpoint || instance.profile != request.profile {
            return Err(Error::Conflict);
        }
        let operation = store::admit(
            &mut transaction,
            binding.id,
            request.request_id,
            Action::Create,
            &recorded,
        )
        .await?;
        transaction.commit().await?;
        self.wake.notify_one();
        operation.try_into()
    }

    async fn stop(
        &self,
        caller: &Caller,
        binding: &ResourceBinding,
        request_id: Uuid,
    ) -> Result<SandboxOperation> {
        self.route(caller, binding).await?;
        let mut transaction = self.platform.begin().await?;
        self.access
            .authorize_in(&mut transaction, caller, &stop_scope())
            .await?;
        let mut instance = store::lock(&mut transaction, binding.id).await?;
        let recorded = caller.stored();
        let operation = store::admit(
            &mut transaction,
            binding.id,
            request_id,
            Action::Stop,
            &recorded,
        )
        .await?;
        instance.stop_requested = true;
        instance.stopper = Some(recorded);
        if instance.removal == Removal::None {
            instance.removal = Removal::Requested;
        }
        store::save(&mut transaction, &instance).await?;
        if instance.removal == Removal::Stopped {
            store::finish(
                &mut transaction,
                binding.id,
                Action::Stop,
                OperationStatus::Succeeded,
                None,
                None,
            )
            .await?;
        }
        transaction.commit().await?;
        self.wake.notify_one();
        if instance.removal == Removal::Stopped {
            return store::operation(self.platform.pool(), binding.id, request_id)
                .await?
                .try_into();
        }
        operation.try_into()
    }

    async fn observe(&self, caller: &Caller, binding: &ResourceBinding) -> Result<SandboxView> {
        self.route(caller, binding).await?;
        let mut transaction = self.platform.begin().await?;
        self.access
            .authorize_in(&mut transaction, caller, &observe_scope())
            .await?;
        transaction.commit().await?;
        let instance = store::read(self.platform.pool(), binding.id).await?;
        let state = if instance.removal == Removal::Stopped {
            SandboxState::Stopped
        } else if let Some(physical) = &instance.physical_id {
            match self
                .runtime
                .observe(physical, instance.id, self.profile(&instance.profile)?)
                .await
            {
                Ok(Some(observed)) => match observed.state {
                    ContainerState::Created => SandboxState::Allocated,
                    ContainerState::Running => SandboxState::Running,
                    ContainerState::Exited => SandboxState::Exited,
                    ContainerState::Unknown => SandboxState::Unknown,
                },
                Ok(None) | Err(_) => SandboxState::Unknown,
            }
        } else if instance.launch == Launch::Queued {
            SandboxState::Pending
        } else {
            SandboxState::Unknown
        };
        Ok(SandboxView {
            id: binding.id,
            profile: instance.profile,
            state,
            stopping: instance.stop_requested && instance.removal != Removal::Stopped,
            observed_at: Utc::now(),
        })
    }

    async fn operation(
        &self,
        caller: &Caller,
        binding: &ResourceBinding,
        request_id: Uuid,
    ) -> Result<SandboxOperation> {
        self.route(caller, binding).await?;
        let mut transaction = self.platform.begin().await?;
        self.access
            .authorize_in(&mut transaction, caller, &observe_scope())
            .await?;
        transaction.commit().await?;
        store::operation(self.platform.pool(), binding.id, request_id)
            .await?
            .try_into()
    }
}
