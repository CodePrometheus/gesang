//! Command admission shares the host transaction; original observations never renew execution.

mod credentials;

use std::{num::NonZeroU64, sync::Arc};

use async_trait::async_trait;
use chopin_job_broker::{
    Action, Caller, Credential, HostAuthority, JobId, Request, Source, WorkerAction, WorkerCaller,
};
use gesang_api::{CapabilityScope, GrantPurpose};
use gesang_core::{Error, GrantRequirement, InstancePurpose, InstanceRecord, Result, Store};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

pub use credentials::JobControlKey;

/// Host-owned validation of a command's actual resources and published execution policy.
#[async_trait]
pub trait JobPolicy: Send + Sync {
    /// Checks command arguments, mounts, environment, source and pinned policy before release.
    ///
    /// Use the supplied transaction for authoritative metadata. Resolve provider
    /// network operations outside it; actual worker placement belongs to the worker
    /// directory. An improvement source must never inherit ordinary tool authority.
    async fn requirements(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        instance: &InstanceRecord,
        request: &Request,
    ) -> Result<Vec<CapabilityScope>>;
}

/// Shared broker authority for independent and embedded hosts.
///
/// The command domain is the kernel's immutable allocation ID. The worker provider
/// must independently bind it to the original physical protection domain; the ID
/// alone is not evidence that a process exists or has stopped.
pub struct JobAuthority {
    store: Store,
    key: Arc<JobControlKey>,
    policy: Arc<dyn JobPolicy>,
    retry_after_ms: NonZeroU64,
}

impl std::fmt::Debug for JobAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobAuthority").finish_non_exhaustive()
    }
}

impl JobAuthority {
    /// Uses a dedicated persistent credential key and explicit host recovery policy.
    pub fn new(
        store: Store,
        key: Arc<JobControlKey>,
        policy: Arc<dyn JobPolicy>,
        retry_after_ms: NonZeroU64,
    ) -> Self {
        Self {
            store,
            key,
            policy,
            retry_after_ms,
        }
    }

    fn failure(&self, error: Error) -> chopin_job_broker::Error {
        match error {
            Error::Forbidden
            | Error::NotFound
            | Error::StaleGeneration
            | Error::RuntimeNotReady
            | Error::FencingRequired
            | Error::InvalidCommand(_) => chopin_job_broker::Error::Forbidden,
            Error::DurabilityRequired => chopin_job_broker::Error::Durability,
            _ => chopin_job_broker::Error::Unavailable {
                retry_after_ms: self.retry_after_ms.get(),
            },
        }
    }

    async fn original(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        caller: &Caller,
    ) -> Result<InstanceRecord> {
        let instance = self
            .store
            .recorded_instance_in(tx, caller.scope, caller.runtime)
            .await?;
        if caller.domain != instance.runtime.id
            || i64::try_from(caller.generation).ok() != Some(instance.runtime.generation)
            || caller.owner != instance.runtime.engine_owner
            || instance.plan.purpose != InstancePurpose::Execution
        {
            return Err(Error::Forbidden);
        }
        Ok(instance)
    }

    async fn execution(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        caller: &Caller,
        request: &Request,
    ) -> chopin_job_broker::Result<()> {
        request.validate()?;
        if request.domain != caller.domain {
            return Err(chopin_job_broker::Error::Forbidden);
        }
        let instance = self
            .original(tx, caller)
            .await
            .map_err(|e| self.failure(e))?;
        let scopes = self
            .policy
            .requirements(tx, &instance, request)
            .await
            .map_err(|e| self.failure(e))?;
        let required = GrantRequirement {
            id: request.grant.id,
            version: i64::try_from(request.grant.version)
                .map_err(|_| chopin_job_broker::Error::Forbidden)?,
            purpose: match request.source {
                Source::Tool { .. } => GrantPurpose::Task,
                Source::Learning { .. } => GrantPurpose::Improvement,
            },
            scopes: &scopes,
        };
        self.store
            .authorize_runtime_in(tx, &instance.runtime, &required)
            .await
            .map_err(|e| self.failure(e))?;
        Ok(())
    }
}

#[async_trait]
impl HostAuthority for JobAuthority {
    async fn authorize_scope(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        caller: &Caller,
    ) -> chopin_job_broker::Result<()> {
        let instance = self
            .original(tx, caller)
            .await
            .map_err(|error| self.failure(error))?;
        self.store
            .verify_writer_in(tx, &instance.runtime)
            .await
            .map_err(|error| self.failure(error))
    }

    async fn authenticate(&self, credential: &str) -> chopin_job_broker::Result<Caller> {
        self.key.engine(credential)
    }

    async fn authenticate_worker(
        &self,
        credential: &str,
    ) -> chopin_job_broker::Result<WorkerCaller> {
        self.key.worker(credential)
    }

    async fn authorize(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        caller: &Caller,
        request: &Request,
        action: Action,
    ) -> chopin_job_broker::Result<()> {
        if action == Action::Submit {
            return self.execution(tx, caller, request).await;
        }
        let current = self
            .original(tx, caller)
            .await
            .map_err(|e| self.failure(e))?;
        self.store
            .verify_writer_in(tx, &current.runtime)
            .await
            .map_err(|e| self.failure(e))?;
        if request.domain != caller.domain {
            let original = self
                .store
                .recorded_instance_in(tx, caller.scope, request.domain)
                .await
                .map_err(|e| self.failure(e))?;
            if original.plan.purpose != InstancePurpose::Execution
                || original.runtime.engine_owner != caller.owner
            {
                return Err(chopin_job_broker::Error::Forbidden);
            }
        }
        Ok(())
    }

    async fn authorize_worker(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        caller: &WorkerCaller,
        request: &Request,
        action: WorkerAction,
    ) -> chopin_job_broker::Result<()> {
        if caller.operation.is_nil()
            || caller.worker.is_nil()
            || request.domain != caller.caller.domain
        {
            return Err(chopin_job_broker::Error::Forbidden);
        }
        if action == WorkerAction::Release {
            return self.execution(tx, &caller.caller, request).await;
        }
        self.original(tx, &caller.caller)
            .await
            .map_err(|e| self.failure(e))?;
        Ok(())
    }

    fn worker_credential(
        &self,
        caller: &Caller,
        id: JobId,
        worker: Uuid,
    ) -> chopin_job_broker::Result<Credential> {
        self.key.issue_worker(caller, id, worker)
    }
}
