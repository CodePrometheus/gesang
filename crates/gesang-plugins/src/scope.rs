//! Resource ownership and non-expanding delegation for trusted services.

use std::collections::BTreeSet;

use gesang_api::CapabilityScope;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// An explicit deployment or business integration; neither grants user authority.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
#[non_exhaustive]
pub enum BindingScope {
    /// Services installed for one deployment.
    Deployment {
        /// Stable deployment identity supplied by the host.
        deployment_id: Uuid,
    },
    /// Services installed for one business connection within a deployment.
    Integration {
        /// Deployment containing this connection.
        deployment_id: Uuid,
        /// Stable identity of this business connection.
        integration_id: Uuid,
    },
}

impl BindingScope {
    /// Whether the other binding belongs to this exact deployment or integration.
    #[must_use]
    pub fn contains(self, other: Self) -> bool {
        match (self, other) {
            (
                Self::Deployment {
                    deployment_id: left,
                },
                Self::Deployment {
                    deployment_id: right,
                }
                | Self::Integration {
                    deployment_id: right,
                    ..
                },
            ) => left == right,
            (Self::Integration { .. }, Self::Integration { .. }) => self == other,
            _ => false,
        }
    }
}

/// The lifetime and ownership boundary containing a service's resources.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceScope {
    /// Resources shared by one owner's instances.
    Owner {
        /// Owner whose resources are shared.
        owner_id: Uuid,
    },
    /// Resources belonging to one execution instance.
    Instance {
        /// Owner of the execution instance.
        owner_id: Uuid,
        /// Stable identity of the execution instance.
        instance_id: Uuid,
    },
    /// Resources belonging to one activation of an instance.
    Activation {
        /// Owner of the execution instance.
        owner_id: Uuid,
        /// Stable identity of the execution instance.
        instance_id: Uuid,
        /// Identity of this resource activation.
        activation_id: Uuid,
    },
}

impl ResourceScope {
    /// Returns the owner whose resources this scope contains.
    #[must_use]
    pub fn owner_id(&self) -> Uuid {
        match self {
            Self::Owner { owner_id }
            | Self::Instance { owner_id, .. }
            | Self::Activation { owner_id, .. } => *owner_id,
        }
    }

    /// Whether `other` has the same owner and an equal or shorter lifetime.
    #[must_use]
    pub fn contains(&self, other: &Self) -> bool {
        if self.owner_id() != other.owner_id() {
            return false;
        }
        match (self, other) {
            (Self::Owner { .. }, _) => true,
            (
                Self::Instance {
                    instance_id: left, ..
                },
                Self::Instance {
                    instance_id: right, ..
                }
                | Self::Activation {
                    instance_id: right, ..
                },
            ) => left == right,
            (Self::Activation { .. }, Self::Activation { .. }) => self == other,
            _ => false,
        }
    }
}

/// Service resource lifetime, independent of the identity of any caller.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum ServiceScope {
    /// A deployment or integration service with no implicit user ownership.
    Shared {
        /// Binding retaining these shared resources.
        binding: BindingScope,
    },
    /// Private resources within one binding and one owner's lifecycle.
    Owned {
        /// Binding retaining this service.
        binding: BindingScope,
        /// Explicit private resource ownership.
        owner: ResourceScope,
    },
}

impl ServiceScope {
    /// Declares a shared service that can exist before any user or agent exists.
    #[must_use]
    pub fn shared(binding: BindingScope) -> Self {
        Self::Shared { binding }
    }

    /// Declares a service whose resources additionally belong to one owner's scope.
    #[must_use]
    pub fn owned(binding: BindingScope, owner: ResourceScope) -> Self {
        Self::Owned { binding, owner }
    }

    /// Returns the service's deployment or integration boundary.
    #[must_use]
    pub fn binding(&self) -> BindingScope {
        match self {
            Self::Shared { binding } | Self::Owned { binding, .. } => *binding,
        }
    }

    /// Returns private ownership when present; shared services have no placeholder owner.
    #[must_use]
    pub fn owner_scope(&self) -> Option<&ResourceScope> {
        match self {
            Self::Shared { .. } => None,
            Self::Owned { owner, .. } => Some(owner),
        }
    }

    /// Whether this provider outlives the consumer in the same binding and ownership domain.
    #[must_use]
    pub fn contains(&self, other: &Self) -> bool {
        self.binding().contains(other.binding())
            && match (self.owner_scope(), other.owner_scope()) {
                (None, _) => true,
                (Some(parent), Some(child)) => parent.contains(child),
                (Some(_), None) => false,
            }
    }

    /// Checks static call boundaries, without replacing current policy at the service boundary.
    #[must_use]
    pub fn accepts(&self, authority: &Authority) -> bool {
        self.binding().contains(authority.binding)
            && match self.owner_scope() {
                None => true,
                Some(owner) => authority
                    .owner_scope()
                    .is_some_and(|caller| owner.contains(caller)),
            }
    }
}

/// A host-verified caller; service accounts do not masquerade as user accounts.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
#[non_exhaustive]
pub enum CallSubject {
    /// A verified owner acting within this resource scope.
    Owner {
        /// Maximum private resource scope of this call.
        scope: ResourceScope,
    },
    /// A verified service identity in the call's binding.
    Service {
        /// Stable service-account identity assigned by the trusted host.
        service_id: Uuid,
    },
}

/// A durable principal identity, not proof that a request is authenticated or authorized.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
#[non_exhaustive]
pub enum PrincipalId {
    /// One persisted owner, independent of the current execution instance.
    Owner {
        /// Stable platform owner identity.
        owner_id: Uuid,
    },
    /// One service account; it does not require a placeholder owner.
    Service {
        /// Stable account assigned by the trusted host.
        service_id: Uuid,
    },
}

/// Authority issued by a trusted host; services may delegate only a subset of it.
/// It is deliberately not deserializable from an unverified request.
///
/// ```compile_fail
/// fn accepts_request<T: serde::de::DeserializeOwned>() {}
/// accepts_request::<gesang_plugins::Authority>();
/// ```
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Authority {
    binding: BindingScope,
    subject: CallSubject,
    capabilities: BTreeSet<CapabilityScope>,
}

/// A delegation attempted to enlarge its resource boundary or capabilities.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum AuthorityError {
    /// The requested lifetime or owner is outside the parent's boundary.
    #[error("delegation cannot enlarge its resource scope")]
    ScopeExpansion,
    /// At least one requested capability is absent from the parent.
    #[error("delegation cannot add capabilities")]
    CapabilityExpansion,
}

impl Authority {
    /// Issues authority at a trusted host boundary, without wildcard interpretation.
    #[must_use]
    pub fn owner(
        binding: BindingScope,
        scope: ResourceScope,
        capabilities: impl IntoIterator<Item = CapabilityScope>,
    ) -> Self {
        Self {
            binding,
            subject: CallSubject::Owner { scope },
            capabilities: capabilities.into_iter().collect(),
        }
    }

    /// Issues service-call authority after host authentication and current policy checks.
    #[must_use]
    pub fn service(
        binding: BindingScope,
        service_id: Uuid,
        capabilities: impl IntoIterator<Item = CapabilityScope>,
    ) -> Self {
        Self {
            binding,
            subject: CallSubject::Service { service_id },
            capabilities: capabilities.into_iter().collect(),
        }
    }

    /// Returns the binding verified for this call.
    #[must_use]
    pub fn binding(&self) -> BindingScope {
        self.binding
    }

    /// Returns the actual caller of this invocation.
    #[must_use]
    pub fn subject(&self) -> &CallSubject {
        &self.subject
    }

    /// Returns durable caller identity without widening this call's authority.
    #[must_use]
    pub fn principal(&self) -> PrincipalId {
        match &self.subject {
            CallSubject::Owner { scope } => PrincipalId::Owner {
                owner_id: scope.owner_id(),
            },
            CallSubject::Service { service_id } => PrincipalId::Service {
                service_id: *service_id,
            },
        }
    }

    /// Returns the owner's resource boundary, absent for service accounts.
    #[must_use]
    pub fn owner_scope(&self) -> Option<&ResourceScope> {
        match &self.subject {
            CallSubject::Owner { scope } => Some(scope),
            CallSubject::Service { .. } => None,
        }
    }

    /// Returns the exact capability and resource pairs granted by the host.
    #[must_use]
    pub fn capabilities(&self) -> &BTreeSet<CapabilityScope> {
        &self.capabilities
    }

    /// Delegates authority within the current boundary without adding capabilities.
    pub fn narrow(
        &self,
        scope: ResourceScope,
        capabilities: impl IntoIterator<Item = CapabilityScope>,
    ) -> Result<Self, AuthorityError> {
        if !self
            .owner_scope()
            .is_some_and(|parent| parent.contains(&scope))
        {
            return Err(AuthorityError::ScopeExpansion);
        }
        let capabilities = capabilities.into_iter().collect::<BTreeSet<_>>();
        if !capabilities.is_subset(&self.capabilities) {
            return Err(AuthorityError::CapabilityExpansion);
        }
        Ok(Self {
            binding: self.binding,
            subject: CallSubject::Owner { scope },
            capabilities,
        })
    }

    /// Removes capabilities while retaining the original subject and binding.
    pub fn restrict(
        &self,
        capabilities: impl IntoIterator<Item = CapabilityScope>,
    ) -> Result<Self, AuthorityError> {
        let capabilities = capabilities.into_iter().collect::<BTreeSet<_>>();
        if !capabilities.is_subset(&self.capabilities) {
            return Err(AuthorityError::CapabilityExpansion);
        }
        Ok(Self {
            binding: self.binding,
            subject: self.subject.clone(),
            capabilities,
        })
    }
}
