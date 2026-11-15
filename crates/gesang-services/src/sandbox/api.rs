use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gesang_api::CapabilityScope;
use gesang_core::ResourceBinding;
use gesang_plugins::{InterfaceId, ServiceEndpoint};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{Caller, Result};

/// The independent resource service contract registered by business providers.
#[must_use]
pub fn sandbox_interface() -> InterfaceId {
    InterfaceId {
        name: "gesang.sandbox".into(),
        version: 1,
    }
}

/// Permission to create from one exact installed profile; configuration changes require new scope.
pub fn create_scope(endpoint: &ServiceEndpoint, profile: &str) -> Result<CapabilityScope> {
    Ok(CapabilityScope {
        capability: "sandbox.create".into(),
        resource: serde_json::to_string(&(endpoint, profile))?,
    })
}

/// Permission to observe the authenticated principal's own sandbox collection.
#[must_use]
pub fn observe_scope() -> CapabilityScope {
    CapabilityScope {
        capability: "sandbox.observe".into(),
        resource: "owned-sandboxes".into(),
    }
}

/// Permission to stop the authenticated principal's own sandboxes, including old provider versions.
#[must_use]
pub fn stop_scope() -> CapabilityScope {
    CapabilityScope {
        capability: "sandbox.stop".into(),
        resource: "owned-sandboxes".into(),
    }
}

/// Creation arguments; callers choose an approved profile, never raw infrastructure settings.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateSandbox {
    /// Stable request identity reused after uncertain replies.
    pub request_id: Uuid,
    /// Profile name within the selected provider configuration.
    pub profile: String,
}

impl CreateSandbox {
    /// Fingerprints immutable arguments independently of the mutable default provider selection.
    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"gesang:sandbox:create:v1\0");
        hash.update(self.profile.as_bytes());
        hash.finalize().into()
    }
}

/// Durable outcome of one resource operation; acceptance is not execution success.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    /// The request is durably recorded and still needs work.
    Accepted,
    /// The provider's corresponding successful outcome has been verified.
    Succeeded,
    /// Work cannot proceed; any allocated resource remains addressable for observation and stop.
    Failed,
    /// The original external outcome requires reconciliation.
    Unknown,
}

impl OperationStatus {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }
}

/// An operation receipt that can be queried with another authenticated connection.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SandboxOperation {
    /// Original logical resource identity, independent of any agent or goal.
    pub resource_id: Uuid,
    /// Original caller-selected request identity.
    pub request_id: Uuid,
    /// Current durable outcome of this operation.
    pub status: OperationStatus,
    /// Stable diagnostic category, without credentials or infrastructure payloads.
    pub problem: Option<String>,
}

/// Current resource evidence, distinct from operation admission.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SandboxState {
    /// No external allocation has been issued.
    Pending,
    /// The original instance is allocated but not observed running.
    Allocated,
    /// The original instance's main process is observed running.
    Running,
    /// Its main process exited; this is not independent destruction evidence.
    Exited,
    /// Positive evidence confirms termination of the original compute instance.
    Stopped,
    /// Current state cannot be established.
    Unknown,
}

/// A current observation of one owned resource.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SandboxView {
    /// Stable logical resource identity.
    pub id: Uuid,
    /// Original configured profile.
    pub profile: String,
    /// Current observed state, not a cached execution result.
    pub state: SandboxState,
    /// Whether a durable stop request is outstanding.
    pub stopping: bool,
    /// Time of this observation attempt.
    pub observed_at: DateTime<Utc>,
}

/// Independent resource operations shared by external clients and trusted host adapters.
/// Implementations must recheck current authorization and persist intent before external effects.
#[async_trait]
pub trait Sandbox: Send + Sync + std::fmt::Debug {
    /// Accepts one original creation request after validating its reserved resource route.
    async fn create(
        &self,
        caller: &Caller,
        binding: &ResourceBinding,
        request: &CreateSandbox,
    ) -> Result<SandboxOperation>;
    /// Accepts a stop for the original resource, without changing its provider or recreating it.
    async fn stop(
        &self,
        caller: &Caller,
        binding: &ResourceBinding,
        request_id: Uuid,
    ) -> Result<SandboxOperation>;
    /// Reads current evidence; observation failure must remain unknown.
    async fn observe(&self, caller: &Caller, binding: &ResourceBinding) -> Result<SandboxView>;
    /// Queries the original operation after a lost reply or client restart.
    async fn operation(
        &self,
        caller: &Caller,
        binding: &ResourceBinding,
        request_id: Uuid,
    ) -> Result<SandboxOperation>;
}
