//! Shared transport adapters for standalone and embedded application hosts.

mod commands;
mod control;
mod credentials;
mod engine;
mod goals;
mod jobs;
mod personal_model;
mod routes;
mod sandbox;
mod subscription;

pub use commands::{
    CommandGateway, CommandGatewayPolicy, observe_command_scope, submit_command_scope,
};
pub use control::{ControlPlane, ControlPolicy, ExecutionAction};
pub use credentials::{ControlKey, ControlToken};
pub use engine::{
    ArtifactDownload, ArtifactRead, EngineAdapter, EngineConnectionPolicy, EngineConnector,
    EngineObservation, SubscriptionChange,
};
pub use goals::{ArtifactDownloadPolicy, GoalGateway, observe_goal_scope};
pub use jobs::{JobAuthority, JobControlKey, JobPolicy, TransactionalJobPolicy};
pub use personal_model::{PersonalCredentialChange, PersonalCredentialClaims};
pub use sandbox::SandboxGateway;
pub use subscription::SubscriptionChallenge;
