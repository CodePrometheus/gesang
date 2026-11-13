//! Embeddable application kernel for durable personal execution hosts.

mod anchor;
mod configuration;
mod crypto;
mod custody;
mod error;
mod host;
mod kernel;
mod lifecycle;
mod pins;
mod store;

pub use anchor::{Anchor, AnchorHead, PendingAnchor};
pub use configuration::{
    AdoptionBoundary, Configuration, ConfigurationAccess, ConfigurationCenter, ConfigurationChange,
    ConfigurationKey, ConfigurationRevision, ConfigurationScope, SecretInput, SecretReference,
};
pub use crypto::PayloadKey;
pub use custody::{
    Custody, CustodyAction, CustodyEnrollment, CustodyKey, CustodyReceipt, CustodyStatus,
    Initialization, InitializationKeyLease, InitializationRequest, InitializationState,
    RuntimeRetirement, ServiceKey, ServiceKeyLease, ServiceKeyVerifier,
};
pub use error::{Error, Result};
pub use host::*;
pub use kernel::{Kernel, RuntimePolicy};
pub use lifecycle::{
    CompletedMigration, InstanceIntent, InstancePhase, InstancePlan, InstancePurpose,
    InstanceRecord, PreparationProgress,
};
pub use store::{GrantRequirement, PendingCommand, Store, WakeClaim};
