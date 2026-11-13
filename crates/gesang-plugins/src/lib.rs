//! Trusted, typed service composition with scoped authority and explicit resource lifetimes.
//!
//! This graph coordinates host services, not agent execution. Untrusted plugin code belongs in
//! a separate protection domain. Persistent package references belong to the platform store;
//! releasing an activation never releases those references.

mod graph;
mod pins;
mod scope;

pub use graph::{
    CallFuture, ChangeReport, Dependency, Factory, FailureStage, Graph, GraphError, ServiceContext,
    ServiceFailure, ServiceHandle, ServiceId, ServiceKey, ServiceSnapshot, ServiceSpec,
    ServiceState, Shutdown, Started,
};
pub use pins::{PackageVersion, PinError, PinStore, VersionPin};
pub use scope::{Authority, AuthorityError, ResourceScope};
