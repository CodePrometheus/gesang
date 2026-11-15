//! Real catalog publication, immutable routing, package retention, and configuration custody.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use gesang_core::{
    AdoptionBoundary, CatalogError, Configuration, ConfigurationAccess, ConfigurationCenter,
    ConfigurationChange, ConfigurationKey, ConfigurationScope, Error, InstalledPlugin, PayloadKey,
    PluginCatalog, SecretInput, ServiceSelection, Store, VerifiedIdentity,
};
use gesang_plugins::{
    Authority, BindingScope, Graph, InstallationId, InstalledVersion, InterfaceId, PackageVersion,
    PinError, PinStore, PinSubject, PluginManifest, PluginSettings, ServiceChoice, ServiceContext,
    ServiceEndpoint, ServiceHandle, ServiceRequirement, ServiceScope, Started, VersionPin,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;
use zeroize::Zeroizing;

struct Fixture {
    store: Store,
    catalog: PluginCatalog,
    access: ConfigurationAccess,
}

impl Fixture {
    async fn new() -> Self {
        let store = database().await;
        store.migrate().await.unwrap();
        let catalog = PluginCatalog::new(store.clone(), ConfigurationKey::new([7; 32]).unwrap());
        Self {
            store,
            catalog,
            access: ConfigurationAccess::Administrator(VerifiedIdentity {
                issuer: "catalog-test".into(),
                subject: "administrator".into(),
            }),
        }
    }

    fn center(&self) -> ConfigurationCenter {
        ConfigurationCenter::new(self.store.clone(), ConfigurationKey::new([7; 32]).unwrap())
    }
}

async fn database() -> Store {
    let url =
        std::env::var("GESANG_TEST_DATABASE_URL").expect("dedicated integration database required");
    let schema = format!("test_{}", Uuid::new_v4().simple());
    let bootstrap = PgPoolOptions::new().connect(&url).await.unwrap();
    bootstrap
        .execute(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .await
        .unwrap();
    let pool = PgPoolOptions::new()
        .connect_with(
            PgConnectOptions::from_str(&url)
                .unwrap()
                .options([("search_path", schema.as_str())]),
        )
        .await
        .unwrap();
    bootstrap.close().await;
    Store::new(pool, PayloadKey::new([42; 32]))
}

fn binding() -> BindingScope {
    BindingScope::Integration {
        deployment_id: Uuid::new_v4(),
        integration_id: Uuid::new_v4(),
    }
}

fn installation(binding: BindingScope) -> InstallationId {
    InstallationId {
        binding,
        id: Uuid::new_v4(),
    }
}

fn interface(name: &str) -> InterfaceId {
    InterfaceId {
        name: name.into(),
        version: 1,
    }
}

fn file(content: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!("gesang-catalog-{}", Uuid::new_v4()));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("record.txt");
    fs::write(&path, content).unwrap();
    path
}

fn settings(path: &std::path::Path) -> PluginSettings {
    PluginSettings {
        manifest: PluginManifest {
            format: 1,
            package: PackageVersion {
                package: "test.files".into(),
                version: "1".into(),
                digest: "content-a".into(),
            },
            provides: BTreeMap::from([
                ("files".into(), interface("test.files")),
                ("metadata".into(), interface("test.metadata")),
            ]),
            requires: BTreeMap::new(),
            configuration: json!({"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":false}),
            secrets: BTreeSet::new(),
        },
        values: json!({"path":path}),
        dependencies: BTreeMap::new(),
    }
}

fn change<T>(values: T, expected_revision: Option<i64>) -> ConfigurationChange<T> {
    ConfigurationChange {
        operation_id: Uuid::new_v4(),
        expected_revision,
        values,
        secrets: BTreeMap::new(),
    }
}

fn choice(installed: &InstalledPlugin, name: &str) -> ServiceChoice {
    let interface = interface(&format!("test.{name}"));
    ServiceChoice {
        endpoint: installed.endpoint(name, &interface).unwrap(),
        interface,
    }
}

async fn register(installed: &InstalledPlugin, graph: &Graph) {
    for name in ["files", "metadata"] {
        let path = PathBuf::from(installed.settings().values["path"].as_str().unwrap());
        installed
            .register(
                graph,
                name,
                &interface(&format!("test.{name}")),
                vec![],
                move |_| {
                    let path = path.clone();
                    async move {
                        fs::metadata(&path).map_err(|error| error.to_string())?;
                        Ok(Started::new(path, || async { Ok(()) }))
                    }
                },
            )
            .await
            .unwrap();
    }
    assert!(graph.activate().await.unwrap().is_success());
}

async fn read(fixture: &Fixture, graph: &Graph, endpoint: &ServiceEndpoint) -> String {
    let handle = fixture
        .catalog
        .service::<PathBuf>(&fixture.access, graph, endpoint, &interface("test.files"))
        .await
        .unwrap();
    let authority = Authority::service(endpoint.provider.installation.binding, Uuid::new_v4(), []);
    handle
        .call(&authority, |path, _| {
            Box::pin(async move { fs::read_to_string(path) })
        })
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn version_routes() {
    let mut fixture = Fixture::new().await;
    let binding = binding();
    let installation = installation(binding);
    let original = change(settings(&file("original store")), None);
    let first = fixture
        .catalog
        .install(&fixture.access, installation, &original)
        .await
        .unwrap();
    let graph = Graph::new(Arc::new(fixture.store.clone()));
    register(&first, &graph).await;
    let old = choice(&first, "files").endpoint;
    let selection = change(
        ServiceSelection {
            services: vec![choice(&first, "files"), choice(&first, "metadata")],
        },
        None,
    );
    fixture
        .catalog
        .publish(&fixture.access, binding, &selection)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .catalog
            .choices(&fixture.access, binding)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(read(&fixture, &graph, &old).await, "original store");
    let resource = PinSubject::Resource {
        binding,
        resource_id: Uuid::new_v4(),
    };
    let reference = Uuid::new_v4();
    let retained = fixture
        .catalog
        .retain(&fixture.access, &old, resource.clone(), reference)
        .await
        .unwrap();
    let second = fixture
        .catalog
        .install(
            &fixture.access,
            installation,
            &change(
                settings(&file("replacement store")),
                Some(first.reference().revision),
            ),
        )
        .await
        .unwrap();
    register(&second, &graph).await;
    fixture
        .catalog
        .publish(
            &fixture.access,
            binding,
            &change(
                ServiceSelection {
                    services: vec![choice(&second, "files"), choice(&second, "metadata")],
                },
                Some(1),
            ),
        )
        .await
        .unwrap();
    let selected = fixture
        .catalog
        .selected(&fixture.access, binding, &interface("test.files"))
        .await
        .unwrap();
    assert_ne!(selected, old);
    assert_eq!(read(&fixture, &graph, &selected).await, "replacement store");
    assert_eq!(read(&fixture, &graph, &old).await, "original store");
    let receipt = fixture
        .catalog
        .install(&fixture.access, installation, &original)
        .await
        .unwrap();
    assert_eq!(receipt.reference(), first.reference());
    assert_eq!(
        fixture
            .catalog
            .load(&fixture.access, old.provider)
            .await
            .unwrap()
            .settings(),
        first.settings()
    );
    assert!(matches!(
        fixture
            .catalog
            .service::<String>(&fixture.access, &graph, &old, &interface("test.files"))
            .await,
        Err(CatalogError::Graph(_))
    ));
    let options = fixture.store.pool().connect_options().as_ref().clone();
    fixture.store.pool().close().await;
    fixture.store = Store::new(
        PgPoolOptions::new().connect_with(options).await.unwrap(),
        PayloadKey::new([42; 32]),
    );
    fixture.catalog = PluginCatalog::new(
        fixture.store.clone(),
        ConfigurationKey::new([7; 32]).unwrap(),
    );
    assert_eq!(
        fixture
            .catalog
            .load(&fixture.access, old.provider)
            .await
            .unwrap()
            .settings(),
        first.settings()
    );
    assert_eq!(read(&fixture, &graph, &old).await, "original store");
    assert_eq!(
        fixture
            .store
            .get(&resource, reference, "test.files")
            .await
            .unwrap(),
        Some(retained)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gesang_owners")
            .fetch_one(fixture.store.pool())
            .await
            .unwrap(),
        0
    );
    assert!(
        graph
            .release(ServiceScope::shared(binding))
            .await
            .unwrap()
            .is_success()
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn dependency_routes() {
    let fixture = Fixture::new().await;
    let binding = binding();
    let BindingScope::Integration { deployment_id, .. } = binding else {
        unreachable!()
    };
    let root = BindingScope::Deployment { deployment_id };
    let provider = fixture
        .catalog
        .install(
            &fixture.access,
            installation(root),
            &change(settings(&file("shared record")), None),
        )
        .await
        .unwrap();
    let imported = choice(&provider, "files").endpoint;
    let mut consumer = settings(&file("consumer"));
    consumer.manifest.package.package = "test.consumer".into();
    consumer.manifest.requires.insert(
        "records".into(),
        ServiceRequirement {
            interface: interface("test.files"),
            optional: false,
        },
    );
    consumer
        .dependencies
        .insert("records".into(), imported.clone());
    let installed = fixture
        .catalog
        .install(
            &fixture.access,
            installation(binding),
            &change(consumer.clone(), None),
        )
        .await
        .unwrap();
    let graph = Graph::new(Arc::new(fixture.store.clone()));
    register(&provider, &graph).await;
    let key = imported.key::<PathBuf>();
    let provider_key = key.clone();
    let endpoint = installed
        .endpoint("files", &interface("test.files"))
        .unwrap();
    installed
        .register(
            &graph,
            "files",
            &interface("test.files"),
            vec![key.dependency()],
            move |context: ServiceContext| {
                let key = provider_key.clone();
                async move {
                    Ok(Started::new(
                        context.service(&key).map_err(|error| error.to_string())?,
                        || async { Ok(()) },
                    ))
                }
            },
        )
        .await
        .unwrap();
    assert!(graph.activate().await.unwrap().is_success());
    let handle = fixture
        .catalog
        .service::<ServiceHandle<PathBuf>>(
            &fixture.access,
            &graph,
            &endpoint,
            &interface("test.files"),
        )
        .await
        .unwrap();
    let authority = Authority::service(binding, Uuid::new_v4(), []);
    let value = handle
        .call(&authority, |provider, actual| {
            Box::pin(async move {
                provider
                    .call(actual, |path, _| {
                        Box::pin(async move { fs::read_to_string(path) })
                    })
                    .await
            })
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(value, "shared record");
    assert!(matches!(
        installed
            .register(
                &graph,
                "metadata",
                &interface("test.metadata"),
                vec![],
                |_| async { Ok(Started::new((), || async { Ok(()) })) }
            )
            .await,
        Err(CatalogError::Dependencies)
    ));
    consumer
        .manifest
        .requires
        .get_mut("records")
        .unwrap()
        .interface
        .version = 2;
    assert!(matches!(
        fixture
            .catalog
            .install(
                &fixture.access,
                installation(binding),
                &change(consumer.clone(), None)
            )
            .await,
        Err(CatalogError::Interface)
    ));
    consumer
        .manifest
        .requires
        .get_mut("records")
        .unwrap()
        .interface
        .version = 1;
    consumer
        .dependencies
        .get_mut("records")
        .unwrap()
        .provider
        .revision += 1;
    assert!(matches!(
        fixture
            .catalog
            .install(
                &fixture.access,
                installation(binding),
                &change(consumer, None)
            )
            .await,
        Err(CatalogError::Configuration(Error::NotFound))
    ));
    assert!(
        graph
            .release(ServiceScope::shared(root))
            .await
            .unwrap()
            .is_success()
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn catalog_conflicts() {
    let fixture = Fixture::new().await;
    let binding = binding();
    let installed = fixture
        .catalog
        .install(
            &fixture.access,
            installation(binding),
            &change(settings(&file("record")), None),
        )
        .await
        .unwrap();
    let selected = choice(&installed, "files");
    fixture
        .catalog
        .publish(
            &fixture.access,
            binding,
            &change(
                ServiceSelection {
                    services: vec![selected.clone()],
                },
                None,
            ),
        )
        .await
        .unwrap();
    let duplicate = change(
        ServiceSelection {
            services: vec![selected.clone(), selected.clone()],
        },
        Some(1),
    );
    assert!(matches!(
        fixture
            .catalog
            .publish(&fixture.access, binding, &duplicate)
            .await,
        Err(CatalogError::Configuration(Error::InvalidCommand(_)))
    ));
    let left = change(ServiceSelection { services: vec![] }, Some(1));
    let right = change(
        ServiceSelection {
            services: vec![choice(&installed, "metadata")],
        },
        Some(1),
    );
    let results = tokio::join!(
        fixture.catalog.publish(&fixture.access, binding, &left),
        fixture.catalog.publish(&fixture.access, binding, &right)
    );
    assert!(matches!(
        results,
        (
            Ok(_),
            Err(CatalogError::Configuration(Error::VersionConflict))
        ) | (
            Err(CatalogError::Configuration(Error::VersionConflict)),
            Ok(_)
        )
    ));
    let other = self::binding();
    assert!(matches!(
        fixture
            .catalog
            .publish(
                &fixture.access,
                other,
                &change(
                    ServiceSelection {
                        services: vec![selected.clone()]
                    },
                    None
                )
            )
            .await,
        Err(CatalogError::Configuration(Error::InvalidCommand(_)))
    ));
    let owner = fixture
        .store
        .bind_owner(&VerifiedIdentity {
            issuer: "test".into(),
            subject: "owner".into(),
        })
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .catalog
            .load(&ConfigurationAccess::Owner(owner), installed.reference())
            .await,
        Err(CatalogError::Configuration(Error::Forbidden))
    ));
    let other_installation = InstallationId {
        binding: other,
        id: installed.reference().installation.id,
    };
    assert!(matches!(
        fixture
            .catalog
            .load(
                &fixture.access,
                InstalledVersion {
                    installation: other_installation,
                    revision: 1
                }
            )
            .await,
        Err(CatalogError::Configuration(Error::NotFound))
    ));
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn retained_secrets() {
    let fixture = Fixture::new().await;
    let installation = installation(binding());
    let mut first = change(settings(&file("record")), None);
    first.values.manifest.secrets.insert("access".into());
    first.secrets.insert(
        "access".into(),
        SecretInput::Set(Zeroizing::new(b"original-secret".to_vec())),
    );
    let original = fixture
        .catalog
        .install(&fixture.access, installation, &first)
        .await
        .unwrap();
    let scope = ConfigurationScope::Installation(installation);
    let before = fixture
        .center()
        .history::<PluginSettings>(&fixture.access, &scope, original.reference().revision)
        .await
        .unwrap();
    let mut second = change(
        original.settings().clone(),
        Some(original.reference().revision),
    );
    second.secrets.insert(
        "access".into(),
        SecretInput::Set(Zeroizing::new(b"replacement-secret".to_vec())),
    );
    let replacement = fixture
        .catalog
        .install(&fixture.access, installation, &second)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .catalog
            .secret(&fixture.access, original.reference(), "access")
            .await
            .unwrap()
            .as_slice(),
        b"original-secret"
    );
    fixture
        .center()
        .revoke_secret::<PluginSettings>(
            &fixture.access,
            &scope,
            "access",
            before.secrets["access"],
        )
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .catalog
            .secret(&fixture.access, original.reference(), "access")
            .await,
        Err(CatalogError::Configuration(Error::Forbidden))
    ));
    assert_eq!(
        fixture
            .catalog
            .secret(&fixture.access, replacement.reference(), "access")
            .await
            .unwrap()
            .as_slice(),
        b"replacement-secret"
    );
    assert!(matches!(
        fixture
            .catalog
            .secret(&fixture.access, replacement.reference(), "undeclared")
            .await,
        Err(CatalogError::Configuration(Error::NotFound))
    ));
    let bytes = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT sealed_value FROM gesang_configuration_secrets WHERE id=$1",
    )
    .bind(before.secrets["access"].id)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert!(
        !bytes
            .windows(b"original-secret".len())
            .any(|value| value == b"original-secret")
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn pin_before_publish() {
    let fixture = Fixture::new().await;
    let installation = installation(binding());
    let request = change(settings(&file("record")), None);
    let pinned = VersionPin {
        subject: PinSubject::Installation {
            binding: installation.binding,
        },
        reference_id: installation.pin_id(request.operation_id),
        package: "test.files".into(),
        version: "1".into(),
        digest: "other-content".into(),
    };
    fixture.store.pin(pinned.clone()).await.unwrap();
    assert!(matches!(
        fixture
            .catalog
            .install(&fixture.access, installation, &request)
            .await,
        Err(CatalogError::Pin(PinError::Conflict { .. }))
    ));
    assert!(matches!(
        fixture
            .center()
            .current::<PluginSettings>(
                &fixture.access,
                &ConfigurationScope::Installation(installation)
            )
            .await,
        Err(Error::NotFound)
    ));
    assert_eq!(
        fixture
            .store
            .get(&pinned.subject, pinned.reference_id, &pinned.package)
            .await
            .unwrap(),
        Some(pinned)
    );
    let valid = change(request.values.clone(), None);
    let installed = fixture
        .catalog
        .install(&fixture.access, installation, &valid)
        .await
        .unwrap();
    let mut conflict = change(request.values, None);
    conflict.operation_id = valid.operation_id;
    conflict.values.values = json!({"path":"different"});
    assert!(matches!(
        fixture
            .catalog
            .install(&fixture.access, installation, &conflict)
            .await,
        Err(CatalogError::Configuration(Error::RequestConflict))
    ));
    let mut absent = installed.settings().clone();
    absent.manifest.requires.insert(
        "future".into(),
        ServiceRequirement {
            interface: interface("test.files"),
            optional: false,
        },
    );
    absent.dependencies.insert(
        "future".into(),
        ServiceEndpoint {
            provider: InstalledVersion {
                installation,
                revision: installed.reference().revision + 1,
            },
            service: "files".into(),
        },
    );
    assert!(matches!(
        fixture
            .catalog
            .install(
                &fixture.access,
                installation,
                &change(absent, Some(installed.reference().revision))
            )
            .await,
        Err(CatalogError::Configuration(Error::NotFound))
    ));
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn installation_references() {
    let fixture = Fixture::new().await;
    let binding = binding();
    let left = installation(binding);
    let right = installation(binding);
    let request = change(settings(&file("record")), None);
    let first = fixture
        .catalog
        .install(&fixture.access, left, &request)
        .await
        .unwrap();
    let mut changed = change(request.values.clone(), None);
    changed.operation_id = request.operation_id;
    changed.values.manifest.package.version = "2".into();
    changed.values.manifest.package.digest = "content-b".into();
    let second = fixture
        .catalog
        .install(&fixture.access, right, &changed)
        .await
        .unwrap();
    assert_ne!(
        left.pin_id(request.operation_id),
        right.pin_id(request.operation_id)
    );
    let subject = PinSubject::Installation { binding };
    let first_pin = fixture
        .store
        .get(&subject, left.pin_id(request.operation_id), "test.files")
        .await
        .unwrap()
        .unwrap();
    let second_pin = fixture
        .store
        .get(&subject, right.pin_id(request.operation_id), "test.files")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_pin.digest, "content-a");
    assert_eq!(second_pin.digest, "content-b");
    let graph = Graph::new(Arc::new(fixture.store.clone()));
    register(&first, &graph).await;
    register(&second, &graph).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gesang_package_pins")
            .fetch_one(fixture.store.pool())
            .await
            .unwrap(),
        2
    );
    assert!(
        graph
            .release(ServiceScope::shared(binding))
            .await
            .unwrap()
            .is_success()
    );
    fixture
        .store
        .unpin(&subject, left.pin_id(request.operation_id), "test.files")
        .await
        .unwrap();
    assert_eq!(
        fixture
            .store
            .get(&subject, right.pin_id(request.operation_id), "test.files")
            .await
            .unwrap(),
        Some(second_pin)
    );
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ExistingSettings {
    value: String,
}

impl Configuration for ExistingSettings {
    const COMPONENT: &'static str = "test.existing";
    const SCHEMA_VERSION: u32 = 1;
    const ADOPTION_BOUNDARY: AdoptionBoundary = AdoptionBoundary::Checkpoint;
    fn validate(
        &self,
        scope: &ConfigurationScope,
        secrets: &BTreeSet<String>,
    ) -> gesang_core::Result<()> {
        if !matches!(scope, ConfigurationScope::Owner(_)) || !secrets.is_empty() {
            return Err(Error::Forbidden);
        }
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn configuration_migration() {
    let store = database().await;
    let directory =
        std::env::temp_dir().join(format!("gesang-legacy-configuration-{}", Uuid::new_v4()));
    fs::create_dir(&directory).unwrap();
    for entry in fs::read_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations")).unwrap()
    {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let ordinal = name
            .to_str()
            .unwrap()
            .split('_')
            .next()
            .unwrap()
            .parse::<u32>()
            .unwrap();
        if ordinal < 12 {
            fs::copy(entry.path(), directory.join(name)).unwrap();
        }
    }
    sqlx::migrate::Migrator::new(directory.as_path())
        .await
        .unwrap()
        .run(store.pool())
        .await
        .unwrap();
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "legacy".into(),
            subject: "person".into(),
        })
        .await
        .unwrap();
    let scope = ConfigurationScope::Owner(owner.id);
    let access = ConfigurationAccess::Owner(owner);
    let center = ConfigurationCenter::new(store.clone(), ConfigurationKey::new([7; 32]).unwrap());
    let request = change(
        ExistingSettings {
            value: "preserved".into(),
        },
        None,
    );
    center.prepare(&access, &scope, &request).await.unwrap();
    let original = center
        .publish::<ExistingSettings>(&access, &scope, request.operation_id)
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let current = center
        .current::<ExistingSettings>(&access, &scope)
        .await
        .unwrap();
    assert_eq!(current.values, original.values);
    assert_eq!(current.operation_id, original.operation_id);
    assert_eq!(current.revision, original.revision);
    let catalog = PluginCatalog::new(store.clone(), ConfigurationKey::new([7; 32]).unwrap());
    let administrator = ConfigurationAccess::Administrator(VerifiedIdentity {
        issuer: "migration".into(),
        subject: "administrator".into(),
    });
    catalog
        .install(
            &administrator,
            installation(binding()),
            &change(settings(&file("new service")), None),
        )
        .await
        .unwrap();
    let rejected = sqlx::query(
        "INSERT INTO gesang_configuration_streams(id,scope_key,component) VALUES($1,$2,'invalid')",
    )
    .bind(Uuid::new_v4())
    .bind("services/integration/not-an-identity")
    .execute(store.pool())
    .await
    .unwrap_err();
    assert_eq!(
        rejected.as_database_error().unwrap().code().as_deref(),
        Some("23514")
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn resource_routes() {
    let fixture = Fixture::new().await;
    let binding = binding();
    let installation = installation(binding);
    let first = fixture
        .catalog
        .install(
            &fixture.access,
            installation,
            &change(settings(&file("original")), None),
        )
        .await
        .unwrap();
    fixture
        .catalog
        .publish(
            &fixture.access,
            binding,
            &change(
                ServiceSelection {
                    services: vec![choice(&first, "files")],
                },
                None,
            ),
        )
        .await
        .unwrap();
    let caller = Authority::service(binding, Uuid::new_v4(), []);
    let contract = interface("test.files");
    let request_id = Uuid::new_v4();
    let (left, right) = tokio::join!(
        fixture
            .catalog
            .reserve(&fixture.access, &caller, &contract, request_id, [1; 32]),
        fixture
            .catalog
            .reserve(&fixture.access, &caller, &contract, request_id, [1; 32]),
    );
    let original = left.unwrap();
    assert_eq!(original, right.unwrap());
    assert_eq!(original.endpoint, choice(&first, "files").endpoint);
    let mut replacement = settings(&file("replacement"));
    replacement.manifest.package.version = "2".into();
    replacement.manifest.package.digest = "content-b".into();
    let second = fixture
        .catalog
        .install(
            &fixture.access,
            installation,
            &change(replacement, Some(first.reference().revision)),
        )
        .await
        .unwrap();
    fixture
        .catalog
        .publish(
            &fixture.access,
            binding,
            &change(
                ServiceSelection {
                    services: vec![choice(&second, "files")],
                },
                Some(1),
            ),
        )
        .await
        .unwrap();
    let retried = fixture
        .catalog
        .reserve(&fixture.access, &caller, &contract, request_id, [1; 32])
        .await
        .unwrap();
    assert_eq!(retried, original);
    assert!(matches!(
        fixture
            .catalog
            .reserve(&fixture.access, &caller, &contract, request_id, [2; 32])
            .await,
        Err(CatalogError::Configuration(Error::RequestConflict))
    ));
    let next = fixture
        .catalog
        .reserve(&fixture.access, &caller, &contract, Uuid::new_v4(), [1; 32])
        .await
        .unwrap();
    assert_ne!(next.id, original.id);
    assert_eq!(next.endpoint, choice(&second, "files").endpoint);
    let foreign = Authority::service(binding, Uuid::new_v4(), []);
    assert!(matches!(
        fixture.store.resource_binding(&foreign, original.id).await,
        Err(Error::NotFound)
    ));
    let owned = fixture
        .catalog
        .reserve(&fixture.access, &foreign, &contract, request_id, [1; 32])
        .await
        .unwrap();
    assert_ne!(owned.id, original.id);
    let pin = fixture
        .store
        .get(
            &PinSubject::Resource {
                binding,
                resource_id: original.id,
            },
            original.id,
            "test.files",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pin.digest, "content-a");
    let options = fixture.store.pool().connect_options().as_ref().clone();
    fixture.store.pool().close().await;
    let reopened = Store::new(
        PgPoolOptions::new().connect_with(options).await.unwrap(),
        PayloadKey::new([42; 32]),
    );
    assert_eq!(
        reopened
            .resource_binding(&caller, original.id)
            .await
            .unwrap(),
        original
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn resource_atomicity() {
    let fixture = Fixture::new().await;
    let binding = binding();
    let installed = fixture
        .catalog
        .install(
            &fixture.access,
            installation(binding),
            &change(settings(&file("record")), None),
        )
        .await
        .unwrap();
    fixture
        .catalog
        .publish(
            &fixture.access,
            binding,
            &change(
                ServiceSelection {
                    services: vec![choice(&installed, "files")],
                },
                None,
            ),
        )
        .await
        .unwrap();
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION reject_resource_pin() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.subject->>'kind' = 'resource' THEN
                RAISE EXCEPTION 'injected retention failure';
            END IF;
            RETURN NEW;
        END;
        $$;
        CREATE TRIGGER reject_resource_pin
        BEFORE INSERT ON gesang_package_pins
        FOR EACH ROW EXECUTE FUNCTION reject_resource_pin();
        "#,
    )
    .execute(fixture.store.pool())
    .await
    .unwrap();
    let caller = Authority::service(binding, Uuid::new_v4(), []);
    let error = fixture
        .catalog
        .reserve(
            &fixture.access,
            &caller,
            &interface("test.files"),
            Uuid::new_v4(),
            [1; 32],
        )
        .await
        .unwrap_err();
    assert!(matches!(error,
        CatalogError::Configuration(Error::PackageReference(PinError::Storage(message)))
        if message.contains("injected retention failure")
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gesang_resource_bindings")
            .fetch_one(fixture.store.pool())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM gesang_package_pins WHERE subject->>'kind'='resource'"
        )
        .fetch_one(fixture.store.pool())
        .await
        .unwrap(),
        0
    );
}
