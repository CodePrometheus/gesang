//! Typed configuration revisions, protected secrets, and explicit publication boundaries.

mod key;
mod store;

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{Error, Owner, Result, VerifiedIdentity};

pub use key::ConfigurationKey;
pub use store::ConfigurationCenter;

/// Configuration ownership is independent of component names and runtime instances.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigurationScope {
    /// Settings available only to a trusted deployment administrator.
    Deployment,
    /// Settings exclusively owned by one persisted platform identity.
    Owner(Uuid),
}

impl ConfigurationScope {
    pub(super) fn key(&self) -> String {
        match self {
            Self::Deployment => "deployment".into(),
            Self::Owner(id) => format!("owner/{id}"),
        }
    }

    pub(super) fn owner(&self) -> Option<Uuid> {
        match self {
            Self::Deployment => None,
            Self::Owner(id) => Some(*id),
        }
    }
}

/// Authority verified by the embedding host, never deserialized from an HTTP body.
#[derive(Clone, Debug)]
pub enum ConfigurationAccess {
    /// The host independently verified the administrator role for this identity.
    Administrator(VerifiedIdentity),
    /// An authenticated account may manage only its own namespace.
    Owner(Owner),
}

impl ConfigurationAccess {
    pub(super) fn check(&self, scope: &ConfigurationScope) -> Result<()> {
        match self {
            Self::Administrator(identity)
                if !identity.issuer.is_empty() && !identity.subject.is_empty() =>
            {
                Ok(())
            }
            Self::Owner(owner) if scope == &ConfigurationScope::Owner(owner.id) => Ok(()),
            _ => Err(Error::Forbidden),
        }
    }

    pub(super) fn audit(&self) -> serde_json::Value {
        match self {
            Self::Administrator(identity) => {
                serde_json::json!({"kind":"administrator","issuer":identity.issuer,"subject":identity.subject})
            }
            Self::Owner(owner) => serde_json::json!({"kind":"owner","ownerId":owner.id}),
        }
    }
}

/// The semantic boundary at which a consumer may adopt a configuration revision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdoptionBoundary {
    /// Every new admission must check the current revision; stale pins are rejected.
    Live,
    /// Existing durable work may retain its pinned definition until a safe checkpoint.
    Checkpoint,
    /// The host applies the revision when constructing a new resource instance.
    Instance,
}

impl AdoptionBoundary {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Checkpoint => "checkpoint",
            Self::Instance => "instance",
        }
    }
}

/// A component's authoritative configuration schema and cross-field invariants.
pub trait Configuration: Clone + Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Stable component namespace, also used for plugin-specific configuration.
    const COMPONENT: &'static str;
    /// Explicit schema version; incompatible historical values are not silently coerced.
    const SCHEMA_VERSION: u32;
    /// Required application boundary for this component's settings.
    const ADOPTION_BOUNDARY: AdoptionBoundary;
    /// Validates scope, typed values, and the exact available secret names before publication.
    fn validate(&self, scope: &ConfigurationScope, secrets: &BTreeSet<String>) -> Result<()>;
}

/// An immutable version of a secret within one component and owner scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretReference {
    /// Opaque version identity; it does not reveal the secret's value.
    pub id: Uuid,
}

/// Secret input is deliberately excluded from serializable public configuration DTOs.
pub enum SecretInput {
    /// Reuses a non-revoked version of the same secret name and configuration stream.
    Keep(SecretReference),
    /// Stages a new encrypted value atomically with the prepared revision.
    Set(Zeroizing<Vec<u8>>),
}

impl std::fmt::Debug for SecretInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Keep(reference) => f.debug_tuple("Keep").field(reference).finish(),
            Self::Set(_) => f.write_str("Set([REDACTED])"),
        }
    }
}

/// One immutable publication attempt, reused after an uncertain response.
#[derive(Debug)]
pub struct ConfigurationChange<T> {
    /// Stable idempotency identity within this component and ownership scope.
    pub operation_id: Uuid,
    /// Required current published revision, or no revision for first publication.
    pub expected_revision: Option<i64>,
    /// Typed values validated by the component.
    pub values: T,
    /// Complete desired secret mapping; omitted names are not inherited implicitly.
    pub secrets: BTreeMap<String, SecretInput>,
}

/// Queryable preparation or publication receipt; secret values are never included.
#[derive(Clone, Debug)]
pub struct ConfigurationRevision<T> {
    /// Monotonic revision, distinct from the schema version.
    pub revision: i64,
    /// Original idempotent publication attempt.
    pub operation_id: Uuid,
    /// Published revision observed by the request's author.
    pub expected_revision: Option<i64>,
    /// Immutable validated values.
    pub values: T,
    /// Protected secret versions referenced by these values.
    pub secrets: BTreeMap<String, SecretReference>,
    /// Source revision copied by an explicit rollback publication.
    pub rollback_of: Option<i64>,
    /// Database-observed time of durable preparation.
    pub prepared_at: DateTime<Utc>,
    /// Present only after atomic publication, not after staging alone.
    pub published_at: Option<DateTime<Utc>>,
}
