//! Run a protected chat request through a real execution service and Linux worker.

use std::{net::Ipv4Addr, path::PathBuf, process::Stdio, sync::OnceLock, time::Duration};

use chopin_protocol::ChordClient;
use gesang_core::{
    Activity, InstanceIntent, InstanceObservation, InstanceProvider, Kernel, PreparationProgress,
    RuntimeClaim, RuntimePolicy,
};
use gesang_host::{
    ControlKey, ControlPlane, EngineAdapter, EngineConnectionPolicy, EngineConnector,
};
use jiff::Timestamp;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::Command,
    sync::oneshot,
    time::timeout,
};

use super::{
    native_worker::NativeWorker,
    node_client::{FixtureKeyVerifier, GoalControlPolicy},
    *,
};

#[path = "../support/chat-gateway.rs"]
mod chat_gateway;

struct Provisioned {
    runtime: OnceLock<Runtime>,
    port: OnceLock<u16>,
    launch: String,
}

impl Provisioned {
    fn port(&self, runtime: &Runtime) -> gesang_core::Result<Option<u16>> {
        if self.runtime.get() != Some(runtime) {
            return Err(Error::StaleGeneration);
        }
        Ok(self.port.get().copied())
    }
}

#[async_trait]
impl EngineConnector for Provisioned {
    async fn connect(&self, runtime: &Runtime) -> gesang_core::Result<ChordClient> {
        let port = self.port(runtime)?.ok_or(Error::RuntimeNotReady)?;
        ChordClient::connect(&format!("ws://127.0.0.1:{port}/"), &self.launch)
            .await
            .map_err(|_| {
                Error::Uncertain("could not authenticate the test execution service".into())
            })
    }
}

#[async_trait]
impl InstanceProvider for Provisioned {
    async fn plan(&self, _: &RuntimeClaim) -> gesang_core::Result<InstancePlan> {
        Ok(InstancePlan {
            purpose: InstancePurpose::Execution,
            provider: "native-chat-probe".into(),
            schema_version: 1,
            definition: json!({}),
        })
    }

    async fn allocate(&self, intent: &InstanceIntent) -> gesang_core::Result<Admission> {
        if intent.claim.previous.is_some() {
            return Err(Error::FencingRequired);
        }
        Ok(Admission {
            runtime_id: intent.id,
            locator: format!("native-chat:{}", intent.id),
            engine_owner: intent.claim.engine_owner.clone(),
            previous_writer: PreviousWriter::Absent,
        })
    }

    async fn activate(&self, intent: &InstanceIntent) -> gesang_core::Result<PreparationProgress> {
        let runtime = intent.runtime.as_ref().ok_or(Error::FencingRequired)?;
        self.runtime.get_or_init(|| runtime.clone());
        Ok(if self.port(runtime)?.is_some() {
            PreparationProgress::ExecutionAvailable
        } else {
            PreparationProgress::Pending
        })
    }

    async fn observe(&self, runtime: &Runtime) -> gesang_core::Result<InstanceObservation> {
        if let Some(port) = self.port(runtime)?
            && TcpStream::connect((Ipv4Addr::LOCALHOST, port))
                .await
                .is_ok()
        {
            return Ok(InstanceObservation::Live(Activity::Waiting));
        }
        Ok(InstanceObservation::Unknown)
    }

    async fn retain_until(&self, runtime: &Runtime, _: Timestamp) -> gesang_core::Result<()> {
        self.runtime.get_or_init(|| runtime.clone());
        self.port(runtime)?;
        Ok(())
    }

    async fn stop(&self, _: &Runtime) -> gesang_core::Result<()> {
        Err(Error::FencingRequired)
    }
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required for the native chat probe"))
}

#[tokio::test]
#[ignore = "requires a dedicated database, native worker, execution service and browser client"]
async fn publish_from_chat() {
    let root = PathBuf::from(required("GESANG_TEST_NATIVE_CHAT_DIR"));
    assert!(root.is_absolute() && root.is_dir());
    let probes = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../apps/demo/test");
    let ready = root.join("host.json");
    let milliseconds = required("GESANG_TEST_OPERATION_TIMEOUT_MS")
        .parse()
        .unwrap();
    let deadline = Duration::from_millis(milliseconds);
    let duration = SignedDuration::try_from(deadline).unwrap();
    let (store, schema) = Fixture::isolated_store().await;
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "native-chat".into(),
            subject: "person".into(),
        })
        .await
        .unwrap();
    let agent = store.ensure_agent(&owner, "chopin", "local").await.unwrap();
    store
        .bind_placement(
            &owner,
            agent.id,
            &[format!("native-chat:{schema}")],
            "native-chat",
        )
        .await
        .unwrap();
    let grant = store
        .select_grant(
            &owner,
            agent.id,
            &GrantSelection {
                mode: PermissionMode::FullAccess,
                scopes: vec![],
            },
        )
        .await
        .unwrap();
    let allocation = Arc::new(Provisioned {
        runtime: OnceLock::new(),
        port: OnceLock::new(),
        launch: Uuid::new_v4().to_string(),
    });
    let engine = Arc::new(
        EngineAdapter::new(
            store.clone(),
            allocation.clone(),
            EngineConnectionPolicy {
                operation_timeout: deadline,
                idle_timeout: deadline,
            },
        )
        .unwrap(),
    );
    let kernel = Arc::new(
        Kernel::new(
            store.clone(),
            allocation.clone(),
            engine.clone(),
            RuntimePolicy {
                lease: duration,
                delivery_lease: duration,
                retry_delay: SignedDuration::from_millis(100),
                recovery_interval: SignedDuration::from_millis(250),
            },
        )
        .unwrap(),
    );
    let pending = kernel.ensure_runtime(agent.id).await;
    assert!(
        matches!(pending, Err(Error::RuntimeNotReady)),
        "{pending:?}"
    );
    let runtime = allocation.runtime.get().unwrap();
    let mut f = Fixture::with_runtime(store.clone(), owner.clone(), runtime.clone(), grant.clone());
    let control_key = Arc::new(ControlKey::new([19; 32]));
    let custody = Arc::new(gesang_core::Custody::new(
        f.store.clone(),
        gesang_core::CustodyKey::new([31; 32]).unwrap(),
    ));
    custody
        .enroll(
            &owner,
            agent.id,
            &gesang_core::CustodyEnrollment {
                operation_id: Uuid::new_v4(),
                expected_revision: 0,
                service_key: gesang_core::ServiceKey::new([29; 32]),
            },
            &FixtureKeyVerifier,
        )
        .await
        .unwrap();
    let plane = ControlPlane::new(
        f.store.clone(),
        control_key.clone(),
        Arc::new(GoalControlPolicy),
        SignedDuration::from_millis(f.settings.recovery_interval_ms.try_into().unwrap()),
    )
    .unwrap()
    .with_execution_custody(custody);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let worker = NativeWorker::start(&mut f, &base, milliseconds).await;
    let router = plane
        .router(NonZeroUsize::new(f.settings.max_body_bytes).unwrap())
        .nest("/commands", Arc::new(f.broker).router());
    let (stop_control, control_stopped) = oneshot::channel();
    let control = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = control_stopped.await;
            })
            .await
            .unwrap();
    });
    let policy = json!({
        "pollAfterMs": 30,
        "outputPageBytes": f.settings.output_page_bytes,
        "scanPageSize": 16,
        "output": { "maxBytes": 8192, "maxLines": 64 },
        "artifactMaxBytes": 65536,
    });
    let mut execution = Command::new("node")
        .arg(probes.join("native-execution.mjs"))
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap())
        .env("GESANG_TEST_NATIVE_CHAT_DIR", &root)
        .env("CHOPIN_TEST_COMMAND_POLICY", policy.to_string())
        .env(
            "CHOPIN_TEST_STORE_KEY",
            BASE64_URL_SAFE_NO_PAD.encode([29; 32]),
        )
        .env("CHOPIN_AGENT_ID", agent.id.to_string())
        .env("CHOPIN_RUNTIME_EPOCH", runtime.generation.to_string())
        .env("CHOPIN_ENGINE_OWNER", &runtime.engine_owner)
        .env("CHOPIN_CONTROL_URL", &base)
        .env(
            "CHOPIN_CONTROL_TOKEN",
            control_key.issue(runtime).unwrap().expose(),
        )
        .env("CHOPIN_TEST_JOB_URL", format!("{base}commands/"))
        .env("CHOPIN_TEST_JOB_DOMAIN", runtime.id.to_string())
        .env(
            "CHOPIN_TEST_JOB_CREDENTIAL",
            f.key.issue(runtime).unwrap().expose(),
        )
        .env("CHOPIN_TEST_LAUNCH_TOKEN", &allocation.launch)
        .env(
            "GESANG_TEST_CHOPIN_SOURCE",
            required("GESANG_TEST_CHOPIN_SOURCE"),
        )
        .env("CHOPIN_STORAGE_NATIVE", required("CHOPIN_STORAGE_NATIVE"))
        .env("GESANG_TEST_MODEL_URL", required("GESANG_TEST_MODEL_URL"))
        .env("GESANG_TEST_MODEL_ID", required("GESANG_TEST_MODEL_ID"))
        .envs(
            [
                "GESANG_TEST_MODEL_MAX_OUTPUT_TOKENS",
                "GESANG_TEST_MODEL_REASONING_OFF_VALUE",
                "GESANG_TEST_MODEL_MAX_TOKENS_FIELD",
            ]
            .into_iter()
            .filter_map(|name| std::env::var_os(name).map(|value| (name, value))),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let port = timeout(deadline, async {
        let mut output = BufReader::new(execution.stdout.take().unwrap()).lines();
        let ready: Value = serde_json::from_str(
            &output
                .next_line()
                .await
                .unwrap()
                .expect("execution exited before readiness"),
        )
        .unwrap();
        assert_eq!(ready["state"], "restored");
        u16::try_from(ready["port"].as_u64().unwrap()).unwrap()
    })
    .await
    .unwrap();
    allocation.port.set(port).unwrap();
    timeout(deadline, async {
        loop {
            match kernel.ensure_runtime(agent.id).await {
                Ok(current) => {
                    assert_eq!(&current, runtime);
                    break;
                }
                Err(Error::RuntimeNotReady) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(error) => panic!("could not admit the protected execution process: {error}"),
            }
        }
    })
    .await
    .unwrap();
    let (stop_chat, chat_stopped) = oneshot::channel();
    let browser = Command::new("node")
        .arg(probes.join("native-browser.mjs"))
        .arg(&root)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let host = tokio::spawn(async move {
        chat_gateway::serve(&store, &owner, kernel, engine, &grant, &ready, async {
            let _ = chat_stopped.await;
        })
        .await
        .unwrap();
    });
    let result = timeout(deadline, browser.wait_with_output()).await;
    stop_chat.send(()).unwrap();
    host.await.unwrap();
    execution
        .stdin
        .take()
        .unwrap()
        .write_all(b"stop\n")
        .await
        .unwrap();
    assert!(
        timeout(deadline, execution.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    worker.close().await;
    stop_control.send(()).unwrap();
    control.await.unwrap();
    assert!(
        result.unwrap().unwrap().status.success(),
        "the browser did not recover the published file"
    );
    assert!(f.worker.starts.load(Ordering::SeqCst) >= 2);
}
