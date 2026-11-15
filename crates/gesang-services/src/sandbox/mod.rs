//! Independent sandbox resources; persistent workspaces and agent goals have separate identities.

mod api;
mod container;
mod service;

pub use api::{
    CreateSandbox, OperationStatus, Sandbox, SandboxOperation, SandboxState, SandboxView,
    create_scope, observe_scope, sandbox_interface, stop_scope,
};
pub use container::{
    ContainerConnection, ContainerId, ContainerProfile, ContainerRuntime, ContainerState,
    PhysicalContainer, Termination,
};
pub use service::{ContainerSandbox, ContainerSettings};
