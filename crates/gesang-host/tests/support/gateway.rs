//! Publish a real database-backed authenticator in the host service catalog.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use gesang_core::{
    ConfigurationAccess, ConfigurationChange, ConfigurationKey, PluginCatalog, ServiceSelection,
    Store, VerifiedIdentity,
};
use gesang_plugins::{
    Authority, BindingScope, Graph, InstallationId, PackageVersion, PluginManifest, PluginSettings,
    ServiceChoice, Started,
};
use gesang_services::{Authenticate, ServiceAccess, authentication_interface};
use serde_json::json;
use uuid::Uuid;

pub(super) struct GatewayFixture {
    pub catalog: PluginCatalog,
    pub graph: Graph,
    pub administrator: ConfigurationAccess,
    pub host: Authority,
    pub binding: BindingScope,
    pub credentials: ServiceAccess,
}

pub(super) async fn configure(store: &Store) -> GatewayFixture {
    let credentials = ServiceAccess::new(store.clone());
    credentials.migrate().await.unwrap();
    let binding = BindingScope::Integration {
        deployment_id: Uuid::new_v4(),
        integration_id: Uuid::new_v4(),
    };
    let catalog = PluginCatalog::new(store.clone(), ConfigurationKey::new([7; 32]).unwrap());
    let graph = Graph::new(Arc::new(store.clone()));
    let administrator = ConfigurationAccess::Administrator(VerifiedIdentity {
        issuer: "gateway-test".into(),
        subject: "administrator".into(),
    });
    let installation = catalog
        .install(
            &administrator,
            InstallationId {
                binding,
                id: Uuid::new_v4(),
            },
            &ConfigurationChange {
                operation_id: Uuid::new_v4(),
                expected_revision: None,
                values: PluginSettings {
                    manifest: PluginManifest {
                        format: 1,
                        package: PackageVersion {
                            package: "test.authentication".into(),
                            version: "1".into(),
                            digest: "test-authentication".into(),
                        },
                        provides: BTreeMap::from([(
                            "authentication".into(),
                            authentication_interface(),
                        )]),
                        requires: BTreeMap::new(),
                        configuration: json!({"type":"object","additionalProperties":false}),
                        secrets: BTreeSet::new(),
                    },
                    values: json!({}),
                    dependencies: BTreeMap::new(),
                },
                secrets: BTreeMap::new(),
            },
        )
        .await
        .unwrap();
    let authenticator: Arc<dyn Authenticate> = Arc::new(credentials.clone());
    installation
        .register(
            &graph,
            "authentication",
            &authentication_interface(),
            vec![],
            move |_| {
                let authenticator = authenticator.clone();
                async move { Ok(Started::new(authenticator, || async { Ok(()) })) }
            },
        )
        .await
        .unwrap();
    assert!(graph.activate().await.unwrap().is_success());
    catalog
        .publish(
            &administrator,
            binding,
            &ConfigurationChange {
                operation_id: Uuid::new_v4(),
                expected_revision: None,
                values: ServiceSelection {
                    services: vec![ServiceChoice {
                        interface: authentication_interface(),
                        endpoint: installation
                            .endpoint("authentication", &authentication_interface())
                            .unwrap(),
                    }],
                },
                secrets: BTreeMap::new(),
            },
        )
        .await
        .unwrap();
    GatewayFixture {
        catalog,
        graph,
        administrator,
        host: Authority::service(binding, Uuid::new_v4(), []),
        binding,
        credentials,
    }
}
