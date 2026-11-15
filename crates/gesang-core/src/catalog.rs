//! Explicit provider selection and immutable installed configuration for trusted hosts.

use std::collections::{BTreeMap, BTreeSet};

use gesang_plugins::{
    Authority, BindingScope, Dependency, Factory, Graph, GraphError, InstallationId,
    InstalledVersion, InterfaceId, ManifestError, PinError, PinStore, PinSubject, PluginSettings,
    ServiceChoice, ServiceEndpoint, ServiceHandle, ServiceKey, ServiceSpec, VersionPin,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{
    AdoptionBoundary, Configuration, ConfigurationAccess, ConfigurationCenter, ConfigurationChange,
    ConfigurationKey, ConfigurationRevision, ConfigurationScope, Error, ResourceBinding, Store,
};

/// A catalog error never converts unresolved configuration or storage into an available service.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CatalogError {
    /// A declaration or configuration was rejected before publication.
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    /// The underlying protected configuration operation failed.
    #[error(transparent)]
    Configuration(#[from] Error),
    /// The immutable package reference could not be retained.
    #[error(transparent)]
    Pin(#[from] PinError),
    /// The selected service is not currently available in the host graph.
    #[error(transparent)]
    Graph(#[from] Box<GraphError>),
    /// A binding selected an undeclared or incompatible interface.
    #[error("selected provider does not implement the requested interface")]
    Interface,
    /// Native registration differs from the published dependency mapping.
    #[error("registered service dependencies do not match the installed configuration")]
    Dependencies,
}

/// A result preserving configuration, declaration, and service lifecycle failures.
pub type CatalogResult<T> = std::result::Result<T, CatalogError>;

impl From<GraphError> for CatalogError {
    fn from(error: GraphError) -> Self {
        Self::Graph(Box::new(error))
    }
}

/// The complete explicit provider choices for new calls within one business boundary.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceSelection {
    /// At most one provider per exact interface; an empty catalog exposes no default services.
    pub services: Vec<ServiceChoice>,
}

impl Configuration for ServiceSelection {
    const COMPONENT: &'static str = "services.catalog";
    const SCHEMA_VERSION: u32 = 1;
    const ADOPTION_BOUNDARY: AdoptionBoundary = AdoptionBoundary::Live;

    fn validate(
        &self,
        scope: &ConfigurationScope,
        secrets: &BTreeSet<String>,
    ) -> crate::Result<()> {
        let ConfigurationScope::Services(binding) = scope else {
            return Err(Error::Forbidden);
        };
        if !secrets.is_empty() {
            return Err(Error::InvalidCommand(
                "catalog selections cannot hold secrets".into(),
            ));
        }
        let mut interfaces = BTreeSet::new();
        for choice in &self.services {
            choice.interface.validate().map_err(invalid)?;
            choice.endpoint.validate().map_err(invalid)?;
            if !choice
                .endpoint
                .provider
                .installation
                .binding
                .contains(*binding)
                || !interfaces.insert(&choice.interface)
            {
                return Err(Error::InvalidCommand(
                    "conflicting service selection or binding".into(),
                ));
            }
        }
        Ok(())
    }
}

impl Configuration for PluginSettings {
    const COMPONENT: &'static str = "services.installation";
    const SCHEMA_VERSION: u32 = 1;
    const ADOPTION_BOUNDARY: AdoptionBoundary = AdoptionBoundary::Instance;

    fn validate(
        &self,
        scope: &ConfigurationScope,
        secrets: &BTreeSet<String>,
    ) -> crate::Result<()> {
        let ConfigurationScope::Installation(installation) = scope else {
            return Err(Error::Forbidden);
        };
        self.validate(installation.binding, secrets)
            .map_err(invalid)
    }
}

fn invalid(error: ManifestError) -> Error {
    Error::InvalidCommand(error.to_string())
}

/// One published installation loaded without substituting its current configuration.
#[derive(Clone, Debug)]
pub struct InstalledPlugin {
    reference: InstalledVersion,
    configuration: ConfigurationRevision<PluginSettings>,
}

impl InstalledPlugin {
    /// The immutable installation and configuration identity.
    #[must_use]
    pub fn reference(&self) -> InstalledVersion {
        self.reference
    }

    /// Nonsecret settings and declared providers for this exact version.
    #[must_use]
    pub fn settings(&self) -> &PluginSettings {
        &self.configuration.values
    }

    /// Resolves a declared export against an exact interface contract.
    pub fn endpoint(
        &self,
        service: &str,
        interface: &InterfaceId,
    ) -> CatalogResult<ServiceEndpoint> {
        if self.settings().manifest.provides.get(service) != Some(interface) {
            return Err(CatalogError::Interface);
        }
        Ok(ServiceEndpoint {
            provider: self.reference,
            service: service.into(),
        })
    }

    /// Registers a verified native implementation with its published package and dependencies.
    /// Factories remain trusted code; registration does not authenticate calls or isolate code.
    pub async fn register<T: Send + Sync + 'static>(
        &self,
        graph: &Graph,
        service: &str,
        interface: &InterfaceId,
        dependencies: Vec<Dependency>,
        factory: impl Factory<T>,
    ) -> CatalogResult<ServiceKey<T>> {
        let endpoint = self.endpoint(service, interface)?;
        let expected = self
            .settings()
            .dependencies
            .values()
            .map(|dependency| dependency.key::<()>().id().clone())
            .collect::<BTreeSet<_>>();
        let actual = dependencies
            .iter()
            .map(|dependency| dependency.id().clone())
            .collect::<BTreeSet<_>>();
        if expected != actual || actual.len() != dependencies.len() {
            return Err(CatalogError::Dependencies);
        }
        let key = endpoint.key();
        graph
            .register(
                key.clone(),
                ServiceSpec {
                    package: self.settings().manifest.package.clone(),
                    pin_reference: self
                        .reference
                        .installation
                        .pin_id(self.configuration.operation_id),
                    dependencies,
                },
                factory,
            )
            .await?;
        Ok(key)
    }
}

/// Durable catalog operations for a trusted host, separate from business call authorization.
#[derive(Clone, Debug)]
pub struct PluginCatalog {
    store: Store,
    configuration: ConfigurationCenter,
}

impl PluginCatalog {
    /// Shares the platform's configuration custody and transactional package-reference store.
    pub fn new(store: Store, key: ConfigurationKey) -> Self {
        Self {
            configuration: ConfigurationCenter::new(store.clone(), key),
            store,
        }
    }

    /// Reserves the original resource route and retains its provider code atomically.
    ///
    /// `request_id` is scoped to the caller and interface. Reusing it with the same
    /// digest returns the original binding, even after the default provider changes.
    /// The host must derive the digest from all immutable creation arguments.
    ///
    /// This reservation does not provision a resource or authorize an operation.
    /// The selected service owns those decisions and their recovery records.
    ///
    /// # Errors
    ///
    /// A changed digest returns [`Error::RequestConflict`]. Missing or incompatible
    /// providers, inaccessible configuration, and persistence failures are returned
    /// without substituting another provider. A package-reference conflict rolls
    /// the new resource binding back in the same transaction.
    ///
    /// # Cancel safety
    ///
    /// Cancellation during commit may leave a durable reservation. Reuse the
    /// original request identity and digest to recover it; do not allocate another
    /// request identity merely because the response was lost.
    pub async fn reserve(
        &self,
        access: &ConfigurationAccess,
        caller: &Authority,
        interface: &InterfaceId,
        request_id: Uuid,
        request_digest: [u8; 32],
    ) -> CatalogResult<ResourceBinding> {
        interface.validate()?;
        access.check(&ConfigurationScope::Services(caller.binding()))?;
        if let Some(existing) = self
            .store
            .resource_request(caller, interface, request_id)
            .await?
        {
            if existing.request_digest != request_digest {
                return Err(Error::RequestConflict.into());
            }
            return Ok(existing);
        }
        let endpoint = self.selected(access, caller.binding(), interface).await?;
        let installed = self.load(access, endpoint.provider).await?;
        Ok(self
            .store
            .reserve_resource(crate::resources::ResourceReservation {
                caller,
                interface,
                request_id,
                request_digest,
                endpoint: &endpoint,
                package: &installed.settings().manifest.package,
            })
            .await?)
    }

    /// Publishes a validated installation after retaining its package and checking exact imports.
    /// Package retention precedes publication; uncertain or refused publication keeps that reference.
    pub async fn install(
        &self,
        access: &ConfigurationAccess,
        installation: InstallationId,
        change: &ConfigurationChange<PluginSettings>,
    ) -> CatalogResult<InstalledPlugin> {
        let scope = ConfigurationScope::Installation(installation);
        access.check(&scope)?;
        Configuration::validate(
            &change.values,
            &scope,
            &change.secrets.keys().cloned().collect(),
        )?;
        for (name, endpoint) in &change.values.dependencies {
            let dependency = self.load(access, endpoint.provider).await?;
            let requirement = change
                .values
                .manifest
                .requires
                .get(name)
                .ok_or(CatalogError::Dependencies)?;
            dependency.endpoint(&endpoint.service, &requirement.interface)?;
        }
        let prepared = self.configuration.prepare(access, &scope, change).await?;
        self.store
            .pin(VersionPin {
                subject: PinSubject::Installation {
                    binding: installation.binding,
                },
                reference_id: installation.pin_id(prepared.operation_id),
                package: prepared.values.manifest.package.package.clone(),
                version: prepared.values.manifest.package.version.clone(),
                digest: prepared.values.manifest.package.digest.clone(),
            })
            .await?;
        let configuration = self
            .configuration
            .publish(access, &scope, change.operation_id)
            .await?;
        Ok(InstalledPlugin {
            reference: InstalledVersion {
                installation,
                revision: configuration.revision,
            },
            configuration,
        })
    }

    /// Loads an exact published version; missing versions never fall back to a current provider.
    pub async fn load(
        &self,
        access: &ConfigurationAccess,
        reference: InstalledVersion,
    ) -> CatalogResult<InstalledPlugin> {
        if reference.revision <= 0 {
            return Err(ManifestError::Identity.into());
        }
        let configuration = self
            .configuration
            .history(
                access,
                &ConfigurationScope::Installation(reference.installation),
                reference.revision,
            )
            .await?;
        Ok(InstalledPlugin {
            reference,
            configuration,
        })
    }

    /// Atomically publishes explicit defaults after checking every exact provider and interface.
    pub async fn publish(
        &self,
        access: &ConfigurationAccess,
        binding: BindingScope,
        change: &ConfigurationChange<ServiceSelection>,
    ) -> CatalogResult<ConfigurationRevision<ServiceSelection>> {
        let scope = ConfigurationScope::Services(binding);
        access.check(&scope)?;
        change
            .values
            .validate(&scope, &change.secrets.keys().cloned().collect())?;
        for choice in &change.values.services {
            self.load(access, choice.endpoint.provider)
                .await?
                .endpoint(&choice.endpoint.service, &choice.interface)?;
        }
        self.configuration.prepare(access, &scope, change).await?;
        Ok(self
            .configuration
            .publish(access, &scope, change.operation_id)
            .await?)
    }

    /// Resolves the selected immutable endpoint for new work without choosing an implicit default.
    pub async fn selected(
        &self,
        access: &ConfigurationAccess,
        binding: BindingScope,
        interface: &InterfaceId,
    ) -> CatalogResult<ServiceEndpoint> {
        interface.validate()?;
        let current = self
            .configuration
            .current::<ServiceSelection>(access, &ConfigurationScope::Services(binding))
            .await?;
        let choice = current
            .values
            .services
            .into_iter()
            .find(|choice| &choice.interface == interface)
            .ok_or(Error::NotFound)?;
        self.load(access, choice.endpoint.provider)
            .await?
            .endpoint(&choice.endpoint.service, interface)
    }

    /// Resolves a retained endpoint into its existing native service generation.
    /// Callers must still supply current verified authority when invoking the handle.
    pub async fn service<T: Send + Sync + 'static>(
        &self,
        access: &ConfigurationAccess,
        graph: &Graph,
        endpoint: &ServiceEndpoint,
        interface: &InterfaceId,
    ) -> CatalogResult<ServiceHandle<T>> {
        self.load(access, endpoint.provider)
            .await?
            .endpoint(&endpoint.service, interface)?;
        Ok(graph.service(&endpoint.key())?)
    }

    /// Resolves only the named secret of this exact published provider under current revocation.
    pub async fn secret(
        &self,
        access: &ConfigurationAccess,
        provider: InstalledVersion,
        name: &str,
    ) -> CatalogResult<Zeroizing<Vec<u8>>> {
        Ok(self
            .configuration
            .resolve_secret::<PluginSettings>(
                access,
                &ConfigurationScope::Installation(provider.installation),
                provider.revision,
                name,
            )
            .await?)
    }

    /// Retains a provider's code for an existing resource or owner operation before external work.
    pub async fn retain(
        &self,
        access: &ConfigurationAccess,
        endpoint: &ServiceEndpoint,
        subject: PinSubject,
        reference_id: Uuid,
    ) -> CatalogResult<VersionPin> {
        let installed = self.load(access, endpoint.provider).await?;
        if !installed
            .settings()
            .manifest
            .provides
            .contains_key(&endpoint.service)
        {
            return Err(CatalogError::Interface);
        }
        match &subject {
            PinSubject::Installation { binding } | PinSubject::Resource { binding, .. }
                if !endpoint.provider.installation.binding.contains(*binding) =>
            {
                return Err(Error::Forbidden.into());
            }
            PinSubject::Owner { .. }
            | PinSubject::Installation { .. }
            | PinSubject::Resource { .. } => {}
            _ => return Err(Error::Forbidden.into()),
        }
        let package = &installed.settings().manifest.package;
        Ok(self
            .store
            .pin(VersionPin {
                subject,
                reference_id,
                package: package.package.clone(),
                version: package.version.clone(),
                digest: package.digest.clone(),
            })
            .await?)
    }

    /// Returns the currently declared selection without exposing installation secret values.
    pub async fn choices(
        &self,
        access: &ConfigurationAccess,
        binding: BindingScope,
    ) -> CatalogResult<BTreeMap<InterfaceId, ServiceEndpoint>> {
        Ok(self
            .configuration
            .current::<ServiceSelection>(access, &ConfigurationScope::Services(binding))
            .await?
            .values
            .services
            .into_iter()
            .map(|choice| (choice.interface, choice.endpoint))
            .collect())
    }
}
