//! Shared transport adapters for standalone and embedded application hosts.

mod control;
mod credentials;
mod engine;
mod jobs;
mod sandbox;

pub use control::{ControlPlane, ControlPolicy, ExecutionAction};
pub use credentials::{ControlKey, ControlToken};
pub use engine::{
    ArtifactDownload, ArtifactRead, EngineAdapter, EngineConnectionPolicy, EngineConnector,
    EngineObservation,
};
pub use jobs::{JobAuthority, JobControlKey, JobPolicy, TransactionalJobPolicy};
pub use sandbox::SandboxGateway;
