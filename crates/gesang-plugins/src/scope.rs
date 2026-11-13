//! Resource ownership and non-expanding delegation for trusted services.

use std::collections::BTreeSet;

use gesang_api::CapabilityScope;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

/// Authority issued by a trusted host; services may delegate only a subset of it.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Authority {
    scope: ResourceScope,
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
    pub fn issue(
        scope: ResourceScope,
        capabilities: impl IntoIterator<Item = CapabilityScope>,
    ) -> Self {
        Self {
            scope,
            capabilities: capabilities.into_iter().collect(),
        }
    }

    /// Returns the resource boundary, including its owner.
    #[must_use]
    pub fn scope(&self) -> &ResourceScope {
        &self.scope
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
        if !self.scope.contains(&scope) {
            return Err(AuthorityError::ScopeExpansion);
        }
        let capabilities = capabilities.into_iter().collect::<BTreeSet<_>>();
        if !capabilities.is_subset(&self.capabilities) {
            return Err(AuthorityError::CapabilityExpansion);
        }
        Ok(Self {
            scope,
            capabilities,
        })
    }
}
