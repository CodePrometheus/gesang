//! Declarative contracts and immutable bindings for trusted business integrations.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{BindingScope, PackageVersion, ServiceKey, ServiceScope};

struct LocalSchema;

impl jsonschema::Retrieve for LocalSchema {
    fn retrieve(
        &self,
        _: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("external configuration schema references are disabled".into())
    }
}

/// A service contract; incompatible contracts have different explicit versions.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InterfaceId {
    /// Stable interface name, independent of the implementation package.
    pub name: String,
    /// Positive contract version; compatibility is exact, not inferred from package versions.
    pub version: u32,
}

impl InterfaceId {
    /// Rejects empty names and unspecified contract versions.
    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.name.is_empty() || self.version == 0 {
            return Err(ManifestError::Identity);
        }
        Ok(())
    }
}

/// One explicit service import of a plugin package.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceRequirement {
    /// Exact interface supplied by the selected provider.
    pub interface: InterfaceId,
    /// Whether the factory supports absence; a configured import must still resolve correctly.
    pub optional: bool,
}

/// An installation within a deployment or business integration, independent of any user.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstallationId {
    /// Host-controlled installation boundary.
    pub binding: BindingScope,
    /// Stable identity permitting multiple independently configured instances of one package.
    pub id: Uuid,
}

impl InstallationId {
    /// Namespaces a configuration request's package reference within this installation.
    #[must_use]
    pub fn pin_id(self, operation_id: Uuid) -> Uuid {
        Uuid::new_v5(&self.id, operation_id.as_bytes())
    }
}

/// One immutable published configuration of an installed package.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstalledVersion {
    /// Exact original installation, including its business binding.
    pub installation: InstallationId,
    /// Positive published revision, never an alias for the current revision.
    pub revision: i64,
}

/// A specific export of an immutable installed version.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceEndpoint {
    /// Published provider and configuration retained by this endpoint.
    pub provider: InstalledVersion,
    /// Export name declared by that provider's manifest.
    pub service: String,
}

impl ServiceEndpoint {
    /// Validates reference shape; the catalog separately verifies publication and the interface.
    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.provider.revision <= 0 || self.service.is_empty() {
            return Err(ManifestError::Identity);
        }
        Ok(())
    }

    /// Uses immutable provider identity for graph lookup; this does not authorize invocation.
    #[must_use]
    pub fn key<T: Send + Sync + 'static>(&self) -> ServiceKey<T> {
        ServiceKey::new(
            format!(
                "{}/{}/{}",
                self.provider.installation.id, self.provider.revision, self.service
            ),
            ServiceScope::shared(self.provider.installation.binding),
        )
    }
}

/// A selected provider for new calls; retained resources keep their original endpoint.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceChoice {
    /// Exact interface exposed by the catalog.
    pub interface: InterfaceId,
    /// Explicit provider; installation order never selects one implicitly.
    pub endpoint: ServiceEndpoint,
}

/// Metadata verified against package content by the trusted installer, without executing it.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginManifest {
    /// Manifest protocol version supported by this host.
    pub format: u32,
    /// Immutable package identity, including its installer-verified content digest.
    pub package: PackageVersion,
    /// Export names and the exact interfaces they implement.
    pub provides: BTreeMap<String, InterfaceId>,
    /// Import names shared by the package's services.
    pub requires: BTreeMap<String, ServiceRequirement>,
    /// Self-contained configuration schema using draft 2020-12.
    pub configuration: Value,
    /// Complete set of required secret names; values remain in protected custody.
    pub secrets: BTreeSet<String>,
}

/// One installation's nonsecret configuration and immutable dependency selections.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginSettings {
    /// Verified declaration for this exact package version.
    pub manifest: PluginManifest,
    /// Values checked against the package's configuration schema.
    pub values: Value,
    /// Exact imported providers; required imports cannot be omitted.
    pub dependencies: BTreeMap<String, ServiceEndpoint>,
}

/// A declaration or configuration cannot establish a valid service binding.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ManifestError {
    /// An identity, version, or export name is missing or invalid.
    #[error("invalid plugin or interface identity")]
    Identity,
    /// This host does not implement the declared manifest protocol.
    #[error("unsupported plugin manifest format")]
    Format,
    /// The configuration schema is invalid or requires unavailable external content.
    #[error("configuration schema must be a valid self-contained draft 2020-12 schema")]
    Schema,
    /// Configuration does not satisfy the schema; values are deliberately not included.
    #[error("configuration violates the schema at {path}")]
    Configuration {
        /// JSON pointer to the failing input field, excluding its value.
        path: String,
    },
    /// The protected secret names differ from the package's declared names.
    #[error("configuration secret names do not match the plugin manifest")]
    Secrets,
    /// A dependency is undeclared, missing, or outside the consumer binding.
    #[error("invalid plugin dependency: {0}")]
    Dependency(String),
}

impl PluginSettings {
    /// Checks declarations and local configuration without loading code or accessing resources.
    pub fn validate(
        &self,
        binding: BindingScope,
        secrets: &BTreeSet<String>,
    ) -> Result<(), ManifestError> {
        let manifest = &self.manifest;
        if manifest.format != 1 {
            return Err(ManifestError::Format);
        }
        if manifest.package.package.is_empty()
            || manifest.package.version.is_empty()
            || manifest.package.digest.is_empty()
            || manifest.provides.is_empty()
            || manifest.provides.keys().any(String::is_empty)
            || manifest.requires.keys().any(String::is_empty)
            || manifest.secrets.iter().any(String::is_empty)
        {
            return Err(ManifestError::Identity);
        }
        for interface in manifest.provides.values() {
            interface.validate()?;
        }
        for (name, requirement) in &manifest.requires {
            requirement.interface.validate()?;
            if !requirement.optional && !self.dependencies.contains_key(name) {
                return Err(ManifestError::Dependency(name.clone()));
            }
        }
        for (name, dependency) in &self.dependencies {
            dependency.validate()?;
            if !manifest.requires.contains_key(name)
                || !dependency.provider.installation.binding.contains(binding)
            {
                return Err(ManifestError::Dependency(name.clone()));
            }
        }
        if &manifest.secrets != secrets {
            return Err(ManifestError::Secrets);
        }
        if manifest.configuration.get("$schema").is_some_and(|draft| {
            draft.as_str() != Some("https://json-schema.org/draft/2020-12/schema")
        }) {
            return Err(ManifestError::Schema);
        }
        let validator = jsonschema::draft202012::options()
            .with_retriever(LocalSchema)
            .should_validate_formats(true)
            .should_ignore_unknown_formats(false)
            .build(&manifest.configuration)
            .map_err(|_| ManifestError::Schema)?;
        validator
            .validate(&self.values)
            .map_err(|error| ManifestError::Configuration {
                path: error.instance_path().to_string(),
            })
    }
}
