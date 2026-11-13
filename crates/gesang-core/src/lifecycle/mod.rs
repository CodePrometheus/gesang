//! Durable instance preparation is distinct from execution-owned goal state.

mod store;

use gesang_api::ApiProblem;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{AgentBinding, AnchorHead, Error, Result, Runtime, RuntimeClaim};

/// Persisted allocation identity for reconciling original work after runtime replacement.
#[derive(Clone, Debug)]
pub struct InstanceRecord {
    /// Immutable owner and physical workspace binding.
    pub binding: AgentBinding,
    /// Exact admitted allocation, including its original generation and locator.
    pub runtime: Runtime,
    /// Immutable purpose and provider definition.
    pub plan: InstancePlan,
    /// Last persisted preparation phase; this is not a live process observation.
    pub phase: InstancePhase,
}

/// A writer's fixed purpose for its entire generation.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstancePurpose {
    /// A protected execution service, initially paused until ready.
    Execution,
    /// A first-store initializer with no existing-data migration or execution authority.
    Initialization,
    /// A protected data migration process with no model or tool authority.
    Migration,
}

/// An immutable host-validated definition persisted before infrastructure effects.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstancePlan {
    /// Fixed writer purpose; migration cannot be promoted to execution in place.
    pub purpose: InstancePurpose,
    /// Stable provider definition namespace.
    pub provider: String,
    /// Provider schema version used to decode the definition after recovery.
    pub schema_version: u32,
    /// Typed provider settings encoded at this boundary; contains immutable references, never secrets.
    pub definition: Value,
}

impl InstancePlan {
    pub(super) fn validate(&self) -> Result<()> {
        if self.provider.trim().is_empty()
            || self.schema_version == 0
            || !self.definition.is_object()
        {
            return Err(Error::InvalidCommand(
                "instance plan requires a versioned provider definition".into(),
            ));
        }
        Ok(())
    }
}

/// Persisted preparation stage; unavailable observations do not imply a terminal stage.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstancePhase {
    /// A stable identity and definition exist, but no writer has been admitted.
    Allocating,
    /// The gated execution allocation is admitted and may start its protected service.
    Starting,
    /// The service is reachable and may restore storage while its scheduler stays paused.
    Unlocking,
    /// The admitted first-store initializer may create only its verified new storage.
    Initializing,
    /// The admitted migration process may write storage but never execute user work.
    Migrating,
    /// Restored execution is eligible for routing and explicit scheduler resume.
    Ready,
    /// No new execution is admitted while existing effects and storage are drained.
    Draining,
    /// Independent physical termination has been verified.
    Stopped,
}

impl InstancePhase {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Allocating => "allocating",
            Self::Starting => "starting",
            Self::Unlocking => "unlocking",
            Self::Initializing => "initializing",
            Self::Migrating => "migrating",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
        }
    }

    pub(crate) fn from_str(value: &str) -> Result<Self> {
        match value {
            "allocating" => Ok(Self::Allocating),
            "starting" => Ok(Self::Starting),
            "unlocking" => Ok(Self::Unlocking),
            "initializing" => Ok(Self::Initializing),
            "migrating" => Ok(Self::Migrating),
            "ready" => Ok(Self::Ready),
            "draining" => Ok(Self::Draining),
            "stopped" => Ok(Self::Stopped),
            _ => Err(Error::InvalidCiphertext),
        }
    }
}

/// One stable provisioning attempt, refreshed with the current controller lease.
#[derive(Clone, Debug)]
pub struct InstanceIntent {
    /// Core-allocated runtime identity, persisted before provisioning.
    pub id: Uuid,
    /// Current controller claim and immutable ownership/resource binding.
    pub claim: RuntimeClaim,
    /// Original provider definition, retained across controller retries.
    pub plan: InstancePlan,
    /// Durable preparation stage.
    pub phase: InstancePhase,
    /// Compare-and-set revision for state changes.
    pub revision: i64,
    /// Exact admitted allocation; absent while allocation is still pending.
    pub runtime: Option<Runtime>,
    /// Confirmed target storage head after a successful stopped migration.
    pub migration_head: Option<AnchorHead>,
    /// Sanitized host diagnosis, without inventing a successful or terminal outcome.
    pub problem: Option<ApiProblem>,
}

/// Historical proof of a successful migration, separate from any current writer claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedMigration {
    /// Original migration writer; this historical identity grants no current authority.
    pub runtime: Runtime,
    /// Exact immutable definition used by the successful migration.
    pub plan: InstancePlan,
    /// Confirmed storage head at completion, not necessarily the current head.
    pub head: AnchorHead,
}

/// Provider observations of a specific admitted preparation attempt.
#[derive(Clone, Debug)]
pub enum PreparationProgress {
    /// The same allocation remains incomplete or its readiness is not yet established.
    Pending,
    /// The exact protected execution process is reachable, with its scheduler still paused.
    ExecutionAvailable,
    /// The exact migration process stopped successfully and verified the committed target head.
    MigrationComplete(AnchorHead),
    /// The exact first-store initializer stopped after publishing custody at this confirmed head.
    InitializationComplete(AnchorHead),
    /// The provider verified physical termination without a successful preparation result.
    Stopped(ApiProblem),
}
