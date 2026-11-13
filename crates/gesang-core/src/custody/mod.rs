//! Explicit owner-authorized custody and durable retirement of unlocked instances.

mod key;
mod store;

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{AgentBinding, AnchorHead, Result, Runtime, Store};

pub use key::{CustodyKey, ServiceKey};

/// Explicit first-store enrollment, persisted before creating resources or releasing keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitializationRequest {
    /// Stable operation identity, reused for reconciliation after a lost response.
    pub operation_id: Uuid,
    /// Digest of the host's immutable nonsecret provisioning plan.
    pub plan_digest: [u8; 32],
}

/// Owner-scoped initialization metadata; no key material is exposed by observation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Initialization {
    /// Logical agent retaining its original owner identity.
    pub agent_id: Uuid,
    /// Original first-store operation.
    pub operation_id: Uuid,
    /// Exact nonsecret provisioning plan digest.
    pub plan_digest: [u8; 32],
    /// Durable initialization state, independent of current custody availability.
    pub state: InitializationState,
    /// Database-confirmed creation time.
    pub created_at: DateTime<Utc>,
}

/// First-store creation does not grant execution or prove physical termination.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum InitializationState {
    /// The original operation may provision and initialize its verified new resources.
    Pending,
    /// The exact initializer completed storage and published the original custody receipt.
    Completed {
        /// Original admitted initializer; this record grants no current writer authority.
        runtime: Runtime,
        /// Confirmed storage head after the initializer closed its store.
        head: AnchorHead,
        /// Immutable enrollment result; current revocation status is read separately.
        receipt: CustodyReceipt,
    },
    /// Owner revocation permanently prevents this operation from initializing storage.
    Revoked,
}

/// Protected key handoff to the admitted initializer for its original operation.
#[derive(Debug)]
pub struct InitializationKeyLease {
    /// Original operation, checked before the receiver accesses its destination.
    pub operation_id: Uuid,
    /// Independently generated service key, retained only for initialization.
    pub key: ServiceKey,
}

/// Trusted validation against the owner's existing vault before accepting custody.
#[async_trait]
pub trait ServiceKeyVerifier: Send + Sync {
    /// Verifies the bound vault without creating, replacing, or resetting data on failure.
    async fn verify(&self, binding: &AgentBinding, key: &ServiceKey) -> Result<()>;
}

/// An explicit enrollment operation, including a stable identifier and observed revision.
#[derive(Debug)]
pub struct CustodyEnrollment {
    /// Stable identity reused unchanged after an uncertain response.
    pub operation_id: Uuid,
    /// Observed custody revision, or zero when no custody record exists.
    pub expected_revision: i64,
    /// Derived key already available to the owner's unlocked client.
    pub service_key: ServiceKey,
}

/// An immutable operation result; current status must be read separately.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CustodyReceipt {
    /// Exact operation identity supplied by the owner.
    pub operation_id: Uuid,
    /// Monotonic custody revision created by this operation.
    pub revision: i64,
    /// Whether this operation enrolled or revoked custody.
    pub action: CustodyAction,
    /// Database-confirmed operation time.
    pub created_at: DateTime<Utc>,
}

/// Accepted custody operation types.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CustodyAction {
    /// A verified derived key became available for unattended unlock.
    Enrolled,
    /// Future unlock and instance admissions were revoked.
    Revoked,
}

/// Current custody availability, separate from physical instance termination.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CustodyStatus {
    /// Latest committed revision, including revocations.
    pub revision: i64,
    /// Enrolled key version currently available for release.
    pub active_version: Option<i64>,
    /// Whether exact physical termination remains to be established.
    pub retirement_pending: bool,
}

/// A current-generation unlock release for a trusted host connector.
#[derive(Debug)]
pub struct ServiceKeyLease {
    /// Exact custody version used for this unlock.
    pub revision: i64,
    /// Secret material retained only for the protected unlock operation.
    pub key: ServiceKey,
}

/// Persisted intent to terminate every admitted or unrecorded launch through a generation.
#[derive(Clone, Debug)]
pub struct RuntimeRetirement {
    /// Stable retirement operation identity.
    pub id: Uuid,
    /// Immutable owner, vault, and physical resource binding.
    pub binding: AgentBinding,
    /// All launches up to and including this generation must be independently stopped.
    pub generation: i64,
    /// Last admitted writer, if a launch reached admission.
    pub previous: Option<Runtime>,
    /// Exact controller attempt claiming this durable intent.
    pub claimant: Uuid,
    /// Database-observed claim expiration.
    pub expires_at: DateTime<Utc>,
}

/// Shared custody authority used by standalone and embedded hosts.
#[derive(Clone, Debug)]
pub struct Custody {
    store: Store,
    key: Arc<CustodyKey>,
}

impl Custody {
    /// Uses a separately supplied host wrapping key; no process-local custody fallback exists.
    pub fn new(store: Store, key: CustodyKey) -> Self {
        Self {
            store,
            key: Arc::new(key),
        }
    }
}
