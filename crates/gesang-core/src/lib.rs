//! Embeddable application kernel for durable personal execution hosts.

mod anchor;
mod catalog;
mod configuration;
mod coordination;
mod crypto;
mod custody;
mod error;
mod host;
mod kernel;
mod lifecycle;
mod pins;
mod postgres_timestamp;
mod resources;
mod store;

pub use anchor::{Anchor, AnchorHead, PendingAnchor};
pub use catalog::{CatalogError, CatalogResult, InstalledPlugin, PluginCatalog, ServiceSelection};
pub use configuration::{
    AdoptionBoundary, Configuration, ConfigurationAccess, ConfigurationCenter, ConfigurationChange,
    ConfigurationKey, ConfigurationRevision, ConfigurationScope, SecretInput, SecretReference,
};
pub use coordination::CoordinationStore;
pub use crypto::PayloadKey;
pub use custody::{
    Custody, CustodyAction, CustodyEnrollment, CustodyKey, CustodyReceipt, CustodyStatus,
    Initialization, InitializationKeyLease, InitializationRequest, InitializationState,
    RuntimeRetirement, ServiceKey, ServiceKeyLease, ServiceKeyVerifier,
};
pub use error::{Error, Result};
pub use host::*;
pub use kernel::{Kernel, RecoveryLane, RuntimePolicy};
pub use lifecycle::{
    CompletedMigration, InstanceIntent, InstancePhase, InstancePlan, InstancePurpose,
    InstanceRecord, PreparationProgress,
};
pub use postgres_timestamp::PostgresTimestamp;
pub use resources::ResourceBinding;
pub use store::{CoordinationTransaction, GrantRequirement, PendingCommand, Store, WakeClaim};
