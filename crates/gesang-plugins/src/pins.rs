//! Persistent version references, independent from running service activations.

use crate::BindingScope;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The durable owner of a package reference, independent of a live service activation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
#[non_exhaustive]
pub enum PinSubject {
    /// Work or configuration belonging to a real owner.
    Owner {
        /// Stable owner identity.
        owner_id: Uuid,
    },
    /// An installed service binding, which may exist before any user.
    Installation {
        /// Deployment or business integration retaining the code.
        binding: BindingScope,
    },
    /// A retained external resource with an independent lifecycle.
    Resource {
        /// Original provider's binding.
        binding: BindingScope,
        /// Stable resource identity, not a user or goal identity.
        resource_id: Uuid,
    },
}

/// An immutable package identity, including the digest verified by the trusted installer.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PackageVersion {
    /// Package identifier from the trusted installation manifest.
    pub package: String,
    /// Version identifier from the same manifest.
    pub version: String,
    /// Content digest whose format is defined by the installer.
    pub digest: String,
}

/// A persistent reference keeping one package version available to its durable subject.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VersionPin {
    /// Durable installation, owner or resource retaining this reference.
    pub subject: PinSubject,
    /// Durable consumer identity, such as a goal or an installed configuration.
    pub reference_id: Uuid,
    /// Installed package identifier.
    pub package: String,
    /// Immutable selected version.
    pub version: String,
    /// Digest of the selected package content.
    pub digest: String,
}

/// A persistent reference conflicts with a requested version, or could not be stored.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PinError {
    /// The same subject, reference and package already select different content.
    #[error("a durable reference already pins another package version")]
    Conflict {
        /// Immutable reference already present in the platform store.
        existing: Box<VersionPin>,
        /// Incompatible reference requested by the caller.
        requested: Box<VersionPin>,
    },
    /// The platform store could not complete the operation.
    #[error("version pin store failed: {0}")]
    Storage(String),
}

/// Platform persistence for immutable references; implementations must provide atomic pinning.
#[async_trait]
pub trait PinStore: Send + Sync + std::fmt::Debug {
    /// Inserts a pin atomically, returns the identical existing pin, or reports a conflict.
    async fn pin(&self, pin: VersionPin) -> Result<VersionPin, PinError>;

    /// Reads a reference without changing its selected version.
    async fn get(
        &self,
        subject: &PinSubject,
        reference_id: Uuid,
        package: &str,
    ) -> Result<Option<VersionPin>, PinError>;

    /// Lists all package references held by this durable consumer.
    async fn list(
        &self,
        subject: &PinSubject,
        reference_id: Uuid,
    ) -> Result<Vec<VersionPin>, PinError>;

    /// Explicitly releases a durable reference; resource shutdown must not call this method.
    async fn unpin(
        &self,
        subject: &PinSubject,
        reference_id: Uuid,
        package: &str,
    ) -> Result<(), PinError>;
}
