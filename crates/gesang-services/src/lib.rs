//! Host resource implementations; operation state belongs here, not in agent execution metadata.

mod access;
mod error;
mod sandbox;

pub use access::{
    Authenticate, AuthorizationRef, Caller, Credential, CredentialGrant, ServiceAccess,
    authentication_interface,
};
pub use error::{Error, Result};
pub use sandbox::{
    ContainerConnection, ContainerId, ContainerProfile, ContainerRuntime, ContainerState,
    PhysicalContainer, Termination,
};
pub use sandbox::{ContainerSandbox, ContainerSettings};
pub use sandbox::{
    CreateSandbox, OperationStatus, Sandbox, SandboxOperation, SandboxState, SandboxView,
    create_scope, observe_scope, sandbox_interface, stop_scope,
};
