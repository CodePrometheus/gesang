//! Real HTTP, service selection, credentials, transactional intent, and local compute lifecycle.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![cfg(unix)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use chrono::{TimeDelta, Utc};
use gesang_core::{
    ConfigurationAccess, ConfigurationChange, ConfigurationKey, InstalledPlugin, PayloadKey,
    PluginCatalog, ServiceSelection, Store, VerifiedIdentity,
};
use gesang_host::SandboxGateway;
use gesang_plugins::{
    Authority, BindingScope, Graph, InstallationId, PackageVersion, PluginManifest, PluginSettings,
    PrincipalId, ServiceChoice, ServiceEndpoint, ServiceScope, Started,
};
use gesang_services::{
    Authenticate, ContainerConnection, ContainerProfile, ContainerSandbox, ContainerSettings,
    CreateSandbox, Credential, CredentialGrant, OperationStatus, Sandbox, SandboxOperation,
    SandboxState, SandboxView, ServiceAccess, authentication_interface, create_scope,
    observe_scope, sandbox_interface, stop_scope,
};
use serde::Deserialize;
use serde_json::json;
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{net::TcpListener, sync::watch};
use uuid::Uuid;

#[derive(Deserialize)]
struct RuntimeFixture {
    connection: ContainerConnection,
    profile: ContainerProfile,
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
    let store = Store::new(pool, PayloadKey::new([42; 32]));
    store.migrate().await.unwrap();
    ServiceAccess::new(store.clone()).migrate().await.unwrap();
    store
}

fn change<T>(values: T, expected_revision: Option<i64>) -> ConfigurationChange<T> {
    ConfigurationChange {
        operation_id: Uuid::new_v4(),
        expected_revision,
        values,
        secrets: BTreeMap::new(),
    }
}

fn settings(runtime: &RuntimeFixture) -> PluginSettings {
    PluginSettings {
        manifest: PluginManifest {
            format: 1,
            package: PackageVersion {
                package: "test.native-services".into(),
                version: "1".into(),
                digest: "native-test-artifact".into(),
            },
            provides: BTreeMap::from([
                ("authentication".into(), authentication_interface()),
                ("sandbox".into(), sandbox_interface()),
            ]),
            requires: BTreeMap::new(),
            configuration: ContainerSettings::schema(),
            secrets: BTreeSet::new(),
        },
        values: serde_json::to_value(ContainerSettings {
            connection: runtime.connection.clone(),
            profiles: BTreeMap::from([("test".into(), runtime.profile.clone())]),
        })
        .unwrap(),
        dependencies: BTreeMap::new(),
    }
}

fn client() -> reqwest::Client {
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(rustls::RootCertStore::empty())
    .with_no_client_auth();
    reqwest::Client::builder()
        .no_proxy()
        .use_preconfigured_tls(tls)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
        .unwrap()
}

async fn terminal(
    client: &reqwest::Client,
    url: &str,
    credential: &Credential,
    deadline: Duration,
) -> SandboxOperation {
    tokio::time::timeout(deadline, async {
        loop {
            let response = client
                .get(url)
                .bearer_auth(credential.expose())
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            assert_eq!(response.headers()["cache-control"], "no-store");
            let operation: SandboxOperation = response.json().await.unwrap();
            match operation.status {
                OperationStatus::Succeeded | OperationStatus::Failed => return operation,
                OperationStatus::Accepted | OperationStatus::Unknown => {
                    tokio::task::yield_now().await
                }
            }
        }
    })
    .await
    .unwrap()
}

async fn register_sandbox(
    platform: &Store,
    graph: &Graph,
    installed: &InstalledPlugin,
) -> (Arc<ContainerSandbox>, ServiceEndpoint) {
    let endpoint = installed.endpoint("sandbox", &sandbox_interface()).unwrap();
    let service = Arc::new(
        ContainerSandbox::open(platform.clone(), installed, "sandbox")
            .await
            .unwrap(),
    );
    let exported: Arc<dyn Sandbox> = service.clone();
    installed
        .register(graph, "sandbox", &sandbox_interface(), vec![], move |_| {
            let service = exported.clone();
            async move { Ok(Started::new(service, || async { Ok(()) })) }
        })
        .await
        .unwrap();
    (service, endpoint)
}

#[tokio::test]
#[ignore = "requires an owned real database and the configured local container runtime"]
async fn lifecycle() {
    let runtime: RuntimeFixture = serde_json::from_slice(
        &fs::read(std::env::var("GESANG_TEST_CONTAINER_CONFIG").expect("runtime fixture required"))
            .unwrap(),
    )
    .unwrap();
    let artifacts = PathBuf::from(
        std::env::var("GESANG_TEST_ARTIFACT_DIR").expect("evidence directory required"),
    );
    let platform = database().await;
    let access = ServiceAccess::new(platform.clone());
    let catalog = PluginCatalog::new(platform.clone(), ConfigurationKey::new([7; 32]).unwrap());
    let binding = BindingScope::Integration {
        deployment_id: Uuid::new_v4(),
        integration_id: Uuid::new_v4(),
    };
    let administration = ConfigurationAccess::Administrator(VerifiedIdentity {
        issuer: "sandbox-test".into(),
        subject: "administrator".into(),
    });
    let installed = catalog
        .install(
            &administration,
            InstallationId {
                binding,
                id: Uuid::new_v4(),
            },
            &change(settings(&runtime), None),
        )
        .await
        .unwrap();
    let graph = Graph::new(Arc::new(platform.clone()));
    let authenticator: Arc<dyn Authenticate> = Arc::new(access.clone());
    installed
        .register(
            &graph,
            "authentication",
            &authentication_interface(),
            vec![],
            move |_| {
                let value = authenticator.clone();
                async move { Ok(Started::new(value, || async { Ok(()) })) }
            },
        )
        .await
        .unwrap();
    let (sandbox, endpoint) = register_sandbox(&platform, &graph, &installed).await;
    assert!(graph.activate().await.unwrap().is_success());
    catalog
        .publish(
            &administration,
            binding,
            &change(
                ServiceSelection {
                    services: vec![
                        ServiceChoice {
                            interface: authentication_interface(),
                            endpoint: installed
                                .endpoint("authentication", &authentication_interface())
                                .unwrap(),
                        },
                        ServiceChoice {
                            interface: sandbox_interface(),
                            endpoint: endpoint.clone(),
                        },
                    ],
                },
                None,
            ),
        )
        .await
        .unwrap();
    let grant = CredentialGrant {
        binding,
        principal: PrincipalId::Service {
            service_id: Uuid::new_v4(),
        },
        scopes: BTreeSet::from([
            create_scope(&endpoint, "test").unwrap(),
            observe_scope(),
            stop_scope(),
        ]),
        expires_at: Utc::now() + TimeDelta::hours(1),
    };
    let (authorization, credential) = access.issue(&grant).await.unwrap();
    let mut foreign = grant.clone();
    foreign.principal = PrincipalId::Service {
        service_id: Uuid::new_v4(),
    };
    let (_, foreign_credential) = access.issue(&foreign).await.unwrap();
    let gateway = SandboxGateway::new(
        platform.clone(),
        catalog.clone(),
        graph.clone(),
        administration.clone(),
        Authority::service(binding, Uuid::new_v4(), []),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let router = axum::Router::new().nest("/api", gateway.router(NonZeroUsize::new(4096).unwrap()));
    let (shutdown, signal) = watch::channel(false);
    let mut server_signal = signal.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                while !*server_signal.borrow() && server_signal.changed().await.is_ok() {}
            })
            .await
    });
    let client = client();
    let create_url = format!("{origin}/api/v1/sandboxes");
    let input = CreateSandbox {
        request_id: Uuid::new_v4(),
        profile: "test".into(),
    };
    assert_eq!(
        client
            .post(&create_url)
            .json(&input)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let first_request = client
        .post(&create_url)
        .bearer_auth(credential.expose())
        .header("connection", "close")
        .json(&input)
        .send();
    let second_request = client
        .post(&create_url)
        .bearer_auth(credential.expose())
        .json(&input)
        .send();
    let (response, duplicate) = tokio::join!(first_request, second_request);
    let response = response.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let location = response.headers()["location"].to_str().unwrap().to_owned();
    assert!(location.starts_with("/api/v1/sandboxes/"));
    let accepted: SandboxOperation = response.json().await.unwrap();
    let duplicate: SandboxOperation = duplicate
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(duplicate.resource_id, accepted.resource_id);
    fs::write(artifacts.join(format!("http-{}.json", accepted.resource_id)), serde_json::to_vec_pretty(
        &json!({"resourceId":accepted.resource_id,"requestId":input.request_id,"endpoint":endpoint,"operationLocation":location})
    ).unwrap()).unwrap();
    let deadline = Duration::from_millis(runtime.connection.request_timeout_ms.get());
    sqlx::raw_sql(
        r#"
        CREATE TABLE sandbox_issuance (resource_id uuid NOT NULL);
        CREATE FUNCTION record_sandbox_issuance() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.launch = 'issued' THEN
                INSERT INTO sandbox_issuance(resource_id) VALUES(NEW.id);
            END IF;
            RETURN NEW;
        END;
        $$;
        CREATE TRIGGER record_sandbox_issuance AFTER UPDATE ON gesang_sandbox_instances
        FOR EACH ROW EXECUTE FUNCTION record_sandbox_issuance();
        "#,
    )
    .execute(platform.pool())
    .await
    .unwrap();
    let mut barrier = platform.begin().await.unwrap();
    let holder = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut *barrier)
        .await
        .unwrap();
    sqlx::query("SELECT id FROM gesang_service_credentials WHERE id=$1 FOR UPDATE")
        .bind(authorization.id)
        .fetch_one(&mut *barrier)
        .await
        .unwrap();
    let first_runner = sandbox.clone();
    let resource_id = accepted.resource_id;
    let mut first_run = tokio::spawn(async move { first_runner.reconcile(resource_id).await });
    let second_runner = sandbox.clone();
    let mut second_run = tokio::spawn(async move { second_runner.reconcile(resource_id).await });
    tokio::time::timeout(deadline, async {
        loop {
            tokio::select! {
                result = &mut first_run => panic!("first controller bypassed the admission lock: {result:?}"),
                result = &mut second_run => panic!("second controller bypassed the admission lock: {result:?}"),
                blocked = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))")
                    .bind(holder).fetch_one(platform.pool()) => if blocked.unwrap() == 2 { break; },
            }
        }
    }).await.unwrap();
    barrier.commit().await.unwrap();
    first_run.await.unwrap().unwrap();
    second_run.await.unwrap().unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sandbox_issuance WHERE resource_id=$1")
            .bind(accepted.resource_id)
            .fetch_one(platform.pool())
            .await
            .unwrap(),
        1
    );
    let runner = sandbox.clone();
    let worker = tokio::spawn(async move {
        runner
            .run(
                Duration::from_millis(100),
                NonZeroU32::new(8).unwrap(),
                signal,
            )
            .await
    });
    let completed = terminal(
        &client,
        &format!("{origin}{location}"),
        &credential,
        deadline,
    )
    .await;
    assert_eq!(completed.status, OperationStatus::Succeeded);
    assert!(!worker.is_finished());
    let lookup = format!("{origin}/api/v1/sandbox-requests/{}", input.request_id);
    let original: SandboxOperation = client
        .get(&lookup)
        .bearer_auth(credential.expose())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(original.resource_id, accepted.resource_id);
    let repeated: SandboxOperation = client
        .post(&create_url)
        .bearer_auth(credential.expose())
        .json(&input)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(repeated.resource_id, accepted.resource_id);
    let resource_url = format!("{origin}/api/v1/sandboxes/{}", accepted.resource_id);
    assert_eq!(
        client
            .get(&resource_url)
            .bearer_auth(foreign_credential.expose())
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    let view: SandboxView = client
        .get(&resource_url)
        .bearer_auth(credential.expose())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(view.state, SandboxState::Running);
    let changed = CreateSandbox {
        profile: "different".into(),
        ..input.clone()
    };
    assert_eq!(
        client
            .post(&create_url)
            .bearer_auth(credential.expose())
            .json(&changed)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    access
        .replace(
            &authorization,
            &BTreeSet::from([observe_scope(), stop_scope()]),
        )
        .await
        .unwrap();
    let denied = CreateSandbox {
        request_id: Uuid::new_v4(),
        profile: "test".into(),
    };
    assert_eq!(
        client
            .post(&create_url)
            .bearer_auth(credential.expose())
            .json(&denied)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    let stop_id = Uuid::new_v4();
    let stop = client
        .post(format!("{resource_url}/stop"))
        .bearer_auth(credential.expose())
        .json(&json!({"requestId":stop_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(stop.status(), reqwest::StatusCode::ACCEPTED);
    let stop_location = stop.headers()["location"].to_str().unwrap().to_owned();
    let stopped = terminal(
        &client,
        &format!("{origin}{stop_location}"),
        &credential,
        deadline,
    )
    .await;
    assert_eq!(stopped.status, OperationStatus::Succeeded);
    let view: SandboxView = client
        .get(&resource_url)
        .bearer_auth(credential.expose())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(view.state, SandboxState::Stopped);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gesang_owners")
            .fetch_one(platform.pool())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gesang_sandbox_instances")
            .fetch_one(platform.pool())
            .await
            .unwrap(),
        1
    );
    shutdown.send(true).unwrap();
    worker.await.unwrap().unwrap();
    server.await.unwrap().unwrap();
    assert!(
        graph
            .release(ServiceScope::shared(binding))
            .await
            .unwrap()
            .is_success()
    );
}

fn write_private(path: &std::path::Path, value: &serde_json::Value) {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};

    let temporary = path.with_extension(format!("{}.pending", Uuid::new_v4()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .unwrap();
    file.write_all(&serde_json::to_vec_pretty(value).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    fs::rename(temporary, path).unwrap();
}

async fn marker(path: &std::path::Path, process: &mut std::process::Child, deadline: Duration) {
    tokio::time::timeout(deadline, async {
        while !path.exists() {
            assert!(
                process.try_wait().unwrap().is_none(),
                "owned process exited before its marker"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

fn child(
    program: &std::ffi::OsStr,
    configuration: &std::path::Path,
    log: &std::path::Path,
) -> std::process::Child {
    let output = fs::File::create(log).unwrap();
    std::process::Command::new(program)
        .arg(configuration)
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .spawn()
        .unwrap()
}

#[tokio::test]
#[ignore = "requires an owned database, real runtime, and compiled sandbox driver"]
async fn process_recovery() {
    use sqlx::ConnectOptions;

    let driver = std::env::var_os("GESANG_TEST_SANDBOX_DRIVER").expect("compiled driver required");
    let mut runtime: RuntimeFixture = serde_json::from_slice(
        &fs::read(std::env::var("GESANG_TEST_CONTAINER_CONFIG").expect("runtime fixture required"))
            .unwrap(),
    )
    .unwrap();
    let directory = PathBuf::from(
        std::env::var("GESANG_TEST_ARTIFACT_DIR").expect("evidence directory required"),
    )
    .join(format!("recovery-{}", Uuid::new_v4()));
    fs::create_dir(&directory).unwrap();
    let deadline = Duration::from_millis(runtime.connection.request_timeout_ms.get());
    let rule_file = directory.join("rules.json");
    let allocated = directory.join("allocated.json");
    let release_create = directory.join("release-create");
    write_private(
        &rule_file,
        &json!({"id":"allocation","method":"POST","path":"/v1.56/containers/create",
        "marker":allocated,"release":release_create}),
    );
    let proxy_socket = std::env::temp_dir().join(format!("gs-{}.sock", Uuid::new_v4().simple()));
    let proxy_ready = directory.join("proxy-ready");
    let proxy_events = directory.join("proxy-events.jsonl");
    let proxy_config = directory.join("proxy.json");
    write_private(
        &proxy_config,
        &json!({"socket":proxy_socket,"upstream":runtime.connection.socket,
        "rules":rule_file,"events":proxy_events,"ready":proxy_ready,
        "timeoutMs":runtime.connection.request_timeout_ms,"pollMs":10}),
    );
    let proxy_output = fs::File::create(directory.join("proxy.log")).unwrap();
    let mut proxy = std::process::Command::new("python3")
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/container-proxy.py"))
        .arg(&proxy_config)
        .stdout(proxy_output.try_clone().unwrap())
        .stderr(proxy_output)
        .spawn()
        .unwrap();
    marker(&proxy_ready, &mut proxy, deadline).await;
    runtime.connection.socket = proxy_socket;
    let platform = database().await;
    let access = ServiceAccess::new(platform.clone());
    let catalog = PluginCatalog::new(platform.clone(), ConfigurationKey::new([7; 32]).unwrap());
    let binding = BindingScope::Integration {
        deployment_id: Uuid::new_v4(),
        integration_id: Uuid::new_v4(),
    };
    let administration = ConfigurationAccess::Administrator(VerifiedIdentity {
        issuer: "recovery-test".into(),
        subject: "administrator".into(),
    });
    let installed = catalog
        .install(
            &administration,
            InstallationId {
                binding,
                id: Uuid::new_v4(),
            },
            &change(settings(&runtime), None),
        )
        .await
        .unwrap();
    let graph = Graph::new(Arc::new(platform.clone()));
    let authenticator: Arc<dyn Authenticate> = Arc::new(access.clone());
    installed
        .register(
            &graph,
            "authentication",
            &authentication_interface(),
            vec![],
            move |_| {
                let value = authenticator.clone();
                async move { Ok(Started::new(value, || async { Ok(()) })) }
            },
        )
        .await
        .unwrap();
    let (_, endpoint) = register_sandbox(&platform, &graph, &installed).await;
    assert!(graph.activate().await.unwrap().is_success());
    catalog
        .publish(
            &administration,
            binding,
            &change(
                ServiceSelection {
                    services: vec![
                        ServiceChoice {
                            interface: authentication_interface(),
                            endpoint: installed
                                .endpoint("authentication", &authentication_interface())
                                .unwrap(),
                        },
                        ServiceChoice {
                            interface: sandbox_interface(),
                            endpoint: endpoint.clone(),
                        },
                    ],
                },
                None,
            ),
        )
        .await
        .unwrap();
    let grant = CredentialGrant {
        binding,
        principal: PrincipalId::Service {
            service_id: Uuid::new_v4(),
        },
        scopes: BTreeSet::from([
            create_scope(&endpoint, "test").unwrap(),
            observe_scope(),
            stop_scope(),
        ]),
        expires_at: Utc::now() + TimeDelta::hours(1),
    };
    let (authorization, credential) = access.issue(&grant).await.unwrap();
    let gateway = SandboxGateway::new(
        platform.clone(),
        catalog.clone(),
        graph.clone(),
        administration.clone(),
        Authority::service(binding, Uuid::new_v4(), []),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let router = gateway.router(NonZeroUsize::new(4096).unwrap());
    let (shutdown, mut shutdown_signal) = watch::channel(false);
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                while !*shutdown_signal.borrow() && shutdown_signal.changed().await.is_ok() {}
            })
            .await
    });
    let client = client();
    let input = CreateSandbox {
        request_id: Uuid::new_v4(),
        profile: "test".into(),
    };
    let accepted: SandboxOperation = client
        .post(format!("{origin}/v1/sandboxes"))
        .bearer_auth(credential.expose())
        .json(&input)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(accepted.status, OperationStatus::Accepted);
    let schema = sqlx::query_scalar::<_, String>("SELECT current_schema()")
        .fetch_one(platform.pool())
        .await
        .unwrap();
    let mut database_url = platform.pool().connect_options().to_url_lossy();
    database_url
        .query_pairs_mut()
        .append_pair("options[search_path]", &schema);
    let worker_config = directory.join("worker.json");
    let worker_ready = directory.join("worker-ready");
    write_private(
        &worker_config,
        &json!({"databaseUrl":database_url.as_str(),"payloadKey":vec![42u8;32],
        "configurationKey":vec![7u8;32],"endpoint":endpoint,
        "recoveryIntervalMs":100,"batchSize":8,"readyFile":worker_ready}),
    );
    let mut replacement = settings(&runtime);
    replacement.values["profiles"]["test"]["command"] = json!(["sh", "-c", "exit 17"]);
    catalog
        .install(
            &administration,
            installed.reference().installation,
            &change(replacement, Some(installed.reference().revision)),
        )
        .await
        .unwrap();
    let mut first = child(&driver, &worker_config, &directory.join("worker-first.log"));
    marker(&allocated, &mut first, deadline).await;
    let allocation: serde_json::Value =
        serde_json::from_slice(&fs::read(&allocated).unwrap()).unwrap();
    assert_eq!(allocation["status"], 201);
    let body: serde_json::Value =
        serde_json::from_str(allocation["body"].as_str().unwrap()).unwrap();
    let physical = body["Id"].as_str().unwrap().to_owned();
    let phase =
        sqlx::query_scalar::<_, String>("SELECT launch FROM gesang_sandbox_instances WHERE id=$1")
            .bind(accepted.resource_id)
            .fetch_one(platform.pool())
            .await
            .unwrap();
    assert_eq!(phase, "issued");
    first.kill().unwrap();
    assert!(!first.wait().unwrap().success());
    fs::write(&release_create, b"release").unwrap();
    let mut second = child(
        &driver,
        &worker_config,
        &directory.join("worker-second.log"),
    );
    let operation_url = format!(
        "{origin}/v1/sandboxes/{}/operations/{}",
        accepted.resource_id, input.request_id
    );
    let resumed = terminal(&client, &operation_url, &credential, deadline).await;
    assert_eq!(resumed.status, OperationStatus::Succeeded);
    let running: SandboxView = client
        .get(format!("{origin}/v1/sandboxes/{}", accepted.resource_id))
        .bearer_auth(credential.expose())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(running.state, SandboxState::Running);
    let stored = sqlx::query_scalar::<_, String>(
        "SELECT physical_id FROM gesang_sandbox_instances WHERE id=$1",
    )
    .bind(accepted.resource_id)
    .fetch_one(platform.pool())
    .await
    .unwrap();
    assert_eq!(stored, physical);
    let removed = directory.join("removed.json");
    let release_remove = directory.join("release-remove");
    write_private(
        &rule_file,
        &json!({"id":"removal","method":"DELETE",
        "path":format!("/v1.56/containers/{physical}"),"marker":removed,"release":release_remove}),
    );
    let stop_id = Uuid::new_v4();
    client
        .post(format!(
            "{origin}/v1/sandboxes/{}/stop",
            accepted.resource_id
        ))
        .bearer_auth(credential.expose())
        .json(&json!({"requestId":stop_id}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    marker(&removed, &mut second, deadline).await;
    let removal: serde_json::Value = serde_json::from_slice(&fs::read(&removed).unwrap()).unwrap();
    assert_eq!(removal["status"], 204);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT removal FROM gesang_sandbox_instances WHERE id=$1")
            .bind(accepted.resource_id)
            .fetch_one(platform.pool())
            .await
            .unwrap(),
        "issued"
    );
    second.kill().unwrap();
    assert!(!second.wait().unwrap().success());
    access.revoke(authorization.id).await.unwrap();
    let mut read_grant = grant;
    read_grant.scopes = BTreeSet::from([observe_scope()]);
    let (_, reader) = access.issue(&read_grant).await.unwrap();
    write_private(&rule_file, &json!({"blockEvents":true}));
    fs::write(&release_remove, b"release").unwrap();
    let mut third = child(&driver, &worker_config, &directory.join("worker-third.log"));
    let stop_url = format!(
        "{origin}/v1/sandboxes/{}/operations/{stop_id}",
        accepted.resource_id
    );
    tokio::time::timeout(deadline, async {
        loop {
            let receipt: SandboxOperation = client
                .get(&stop_url)
                .bearer_auth(reader.expose())
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_ne!(receipt.status, OperationStatus::Failed);
            assert_ne!(receipt.status, OperationStatus::Succeeded);
            if receipt.status == OperationStatus::Unknown {
                break;
            }
            assert!(third.try_wait().unwrap().is_none());
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    write_private(&rule_file, &json!({}));
    let stopped = terminal(&client, &stop_url, &reader, deadline).await;
    assert_eq!(stopped.status, OperationStatus::Succeeded);
    let view: SandboxView = client
        .get(format!("{origin}/v1/sandboxes/{}", accepted.resource_id))
        .bearer_auth(reader.expose())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(view.state, SandboxState::Stopped);
    let events: Vec<serde_json::Value> = fs::read_to_string(&proxy_events)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        events
            .iter()
            .filter(|event| event["kind"] == "response"
                && event["method"] == "POST"
                && event["path"]
                    .as_str()
                    .unwrap()
                    .starts_with("/v1.56/containers/create"))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["kind"] == "response" && event["method"] == "DELETE")
            .count(),
        1
    );
    fs::write(
        directory.join("result.json"),
        serde_json::to_vec_pretty(&json!({"resourceId":accepted.resource_id,
        "physicalId":physical,"allocationRequests":1,"removalRequests":1,"processRestarts":2,
        "lateRevocationReconciled":true,"unavailableEvidenceStayedUnknown":true}))
        .unwrap(),
    )
    .unwrap();
    third.kill().unwrap();
    third.wait().unwrap();
    proxy.kill().unwrap();
    proxy.wait().unwrap();
    shutdown.send(true).unwrap();
    server.await.unwrap().unwrap();
    assert!(
        graph
            .release(ServiceScope::shared(binding))
            .await
            .unwrap()
            .is_success()
    );
}
