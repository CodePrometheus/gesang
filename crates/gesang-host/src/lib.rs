//! Shared transport adapters for standalone and embedded application hosts.

mod control;
mod credentials;
mod engine;
mod jobs;
mod personal_model;
mod sandbox;
mod subscription;

pub use control::{ControlPlane, ControlPolicy, ExecutionAction};
pub use credentials::{ControlKey, ControlToken};
pub use engine::{
    ArtifactDownload, ArtifactRead, EngineAdapter, EngineConnectionPolicy, EngineConnector,
    EngineObservation, SubscriptionChange,
};
pub use jobs::{JobAuthority, JobControlKey, JobPolicy, TransactionalJobPolicy};
pub use personal_model::{PersonalCredentialChange, PersonalCredentialClaims};
pub use sandbox::SandboxGateway;
pub use subscription::SubscriptionChallenge;
