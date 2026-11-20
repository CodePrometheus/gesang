//! Command admission shares the host transaction; original observations never renew execution.

mod credentials;

use std::{num::NonZeroU64, sync::Arc};

use async_trait::async_trait;
use chopin_job_broker::{
    Action, Caller, Credential, HostAuthority, JobId, Request, Source, StoreTransaction,
    TransactionalHostAuthority, WorkerAction, WorkerCaller,
};
use gesang_api::{CapabilityScope, GrantPurpose};
use gesang_core::{
    CoordinationTransaction, Error, GrantRequirement, InstancePurpose, InstanceRecord, Result,
    Store,
};
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

/// Host policy evaluated inside the shared coordination transaction on either backend.
#[async_trait]
pub trait TransactionalJobPolicy: Send + Sync {
    /// Checks immutable execution arguments and host resources using this transaction.
    ///
    /// Resolve provider network operations before acquiring the transaction. A
    /// policy denial aborts command admission or process release.
    async fn requirements(
        &self,
        tx: &mut CoordinationTransaction<'_, '_>,
        instance: &InstanceRecord,
        request: &Request,
    ) -> Result<Vec<CapabilityScope>>;
}

enum Policy {
    Postgres(Arc<dyn JobPolicy>),
    Transactional(Arc<dyn TransactionalJobPolicy>),
}

/// Shared broker authority for independent and embedded hosts.
///
/// The command domain is the kernel's immutable allocation ID. The worker provider
/// must independently bind it to the original physical protection domain; the ID
/// alone is not evidence that a process exists or has stopped.
pub struct JobAuthority {
    store: Store,
    key: Arc<JobControlKey>,
    policy: Policy,
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
            policy: Policy::Postgres(policy),
            retry_after_ms,
        }
    }

    /// Uses a host policy that can join either backend's command transaction.
    pub fn with_transactional_policy(
        store: Store,
        key: Arc<JobControlKey>,
        policy: Arc<dyn TransactionalJobPolicy>,
        retry_after_ms: NonZeroU64,
    ) -> Self {
        Self {
            store,
            key,
            policy: Policy::Transactional(policy),
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
        tx: &mut CoordinationTransaction<'_, '_>,
        caller: &Caller,
    ) -> Result<InstanceRecord> {
        let instance = self
            .store
            .recorded_instance_coordination_in(tx, caller.scope, caller.runtime)
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
        tx: &mut CoordinationTransaction<'_, '_>,
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
        let scopes = match &self.policy {
            Policy::Postgres(policy) => match tx {
                CoordinationTransaction::Postgres(tx) => {
                    policy.requirements(tx, &instance, request).await
                }
                _ => Err(Error::StorageBackend),
            },
            Policy::Transactional(policy) => policy.requirements(tx, &instance, request).await,
        }
        .map_err(|error| self.failure(error))?;
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
            .authorize_runtime_coordination_in(tx, &instance.runtime, &required)
            .await
            .map_err(|e| self.failure(e))?;
        Ok(())
    }

    async fn check_scope(
        &self,
        tx: &mut CoordinationTransaction<'_, '_>,
        caller: &Caller,
    ) -> chopin_job_broker::Result<()> {
        let instance = self
            .original(tx, caller)
            .await
            .map_err(|error| self.failure(error))?;
        self.store
            .verify_writer_coordination_in(tx, &instance.runtime)
            .await
            .map_err(|error| self.failure(error))
    }

    async fn check_request(
        &self,
        tx: &mut CoordinationTransaction<'_, '_>,
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
            .verify_writer_coordination_in(tx, &current.runtime)
            .await
            .map_err(|e| self.failure(e))?;
        if request.domain != caller.domain {
            let original = self
                .store
                .recorded_instance_coordination_in(tx, caller.scope, request.domain)
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

    async fn check_worker(
        &self,
        tx: &mut CoordinationTransaction<'_, '_>,
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
}

#[async_trait]
impl HostAuthority for JobAuthority {
    async fn authenticate(&self, credential: &str) -> chopin_job_broker::Result<Caller> {
        self.key.engine(credential)
    }

    async fn authenticate_worker(
        &self,
        credential: &str,
    ) -> chopin_job_broker::Result<WorkerCaller> {
        self.key.worker(credential)
    }

    async fn authorize_scope(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        caller: &Caller,
    ) -> chopin_job_broker::Result<()> {
        self.check_scope(&mut tx.into(), caller).await
    }

    async fn authorize(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        caller: &Caller,
        request: &Request,
        action: Action,
    ) -> chopin_job_broker::Result<()> {
        self.check_request(&mut tx.into(), caller, request, action)
            .await
    }

    async fn authorize_worker(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        caller: &WorkerCaller,
        request: &Request,
        action: WorkerAction,
    ) -> chopin_job_broker::Result<()> {
        self.check_worker(&mut tx.into(), caller, request, action)
            .await
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

#[async_trait]
impl TransactionalHostAuthority for JobAuthority {
    async fn authenticate(&self, credential: &str) -> chopin_job_broker::Result<Caller> {
        self.key.engine(credential)
    }

    async fn authenticate_worker(
        &self,
        credential: &str,
    ) -> chopin_job_broker::Result<WorkerCaller> {
        self.key.worker(credential)
    }

    async fn authorize_scope(
        &self,
        tx: &mut StoreTransaction<'_>,
        caller: &Caller,
    ) -> chopin_job_broker::Result<()> {
        self.check_scope(&mut coordination(tx), caller).await
    }

    async fn authorize(
        &self,
        tx: &mut StoreTransaction<'_>,
        caller: &Caller,
        request: &Request,
        action: Action,
    ) -> chopin_job_broker::Result<()> {
        self.check_request(&mut coordination(tx), caller, request, action)
            .await
    }

    async fn authorize_worker(
        &self,
        tx: &mut StoreTransaction<'_>,
        caller: &WorkerCaller,
        request: &Request,
        action: WorkerAction,
    ) -> chopin_job_broker::Result<()> {
        self.check_worker(&mut coordination(tx), caller, request, action)
            .await
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

fn coordination<'a, 'db>(tx: &'a mut StoreTransaction<'db>) -> CoordinationTransaction<'a, 'db> {
    match tx {
        StoreTransaction::Postgres(tx) => tx.into(),
        StoreTransaction::Sqlite(tx) => tx.into(),
    }
}
