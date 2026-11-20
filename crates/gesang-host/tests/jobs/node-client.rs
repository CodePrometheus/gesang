//! Actual Node command and encrypted goal recovery through shared broker authority.

use std::{process::Stdio, time::Duration};

use axum::Router;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    process::Command,
    sync::oneshot,
    time::timeout,
};

use super::*;

struct GoalControlPolicy;

#[async_trait]
impl gesang_host::ControlPolicy for GoalControlPolicy {
    async fn verify_execution(
        &self,
        binding: &gesang_core::AgentBinding,
        runtime: &gesang_core::Runtime,
    ) -> gesang_core::Result<()> {
        assert_eq!(binding.agent.id, runtime.agent_id);
        assert_eq!(binding.engine_owner, runtime.engine_owner);
        Ok(())
    }
    async fn requirements(
        &self,
        binding: &gesang_core::AgentBinding,
        _: &gesang_host::ExecutionAction,
    ) -> gesang_core::Result<Vec<CapabilityScope>> {
        Ok(vec![CapabilityScope {
            capability: "command".into(),
            resource: binding.agent.id.to_string(),
        }])
    }
}

struct FixtureKeyVerifier;

#[async_trait]
impl gesang_core::ServiceKeyVerifier for FixtureKeyVerifier {
    async fn verify(
        &self,
        _: &gesang_core::AgentBinding,
        key: &gesang_core::ServiceKey,
    ) -> gesang_core::Result<()> {
        assert_eq!(key.expose(), &[29; 32]);
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires isolated database, compiled Node command client and explicit deadline"]
async fn interrupted_submit() {
    let probe = std::env::var("GESANG_TEST_JOB_PROBE").expect("built command probe required");
    let milliseconds: u64 = std::env::var("GESANG_TEST_OPERATION_TIMEOUT_MS")
        .expect("explicit integration deadline required")
        .parse()
        .unwrap();
    assert!(milliseconds > 0);
    let f = Fixture::new().await;
    let caller = f.caller(&f.runtime).await;
    let request = f.request();
    let operation = Uuid::new_v4();
    let credential = f.key.issue(&f.runtime).unwrap();
    let broker = Arc::new(f.broker);
    let router = Router::new().nest("/commands", broker.clone().router());
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = format!("http://{}/commands/", listener.local_addr().unwrap());
    let (shutdown, stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let output = b"\0\xfforiginal output\n";
    let mut child = Command::new("node")
        .arg(probe)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap())
        .env("CHOPIN_TEST_JOB_URL", address)
        .env("CHOPIN_TEST_JOB_SCOPE", caller.scope.to_string())
        .env("CHOPIN_TEST_JOB_CREDENTIAL", credential.expose())
        .env("CHOPIN_TEST_JOB_OPERATION", operation.to_string())
        .env(
            "CHOPIN_TEST_JOB_REQUEST",
            serde_json::to_string(&request).unwrap(),
        )
        .env("CHOPIN_TEST_JOB_PAGE_BYTES", "3")
        .env(
            "CHOPIN_TEST_JOB_OUTPUT",
            BASE64_URL_SAFE_NO_PAD.encode(output),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut stderr = child.stderr.take().unwrap();
    let mut diagnostic = tokio::spawn(async move {
        let mut text = String::new();
        stderr.read_to_string(&mut text).await.unwrap();
        text
    });
    let result = timeout(Duration::from_millis(milliseconds), async {
        let first = lines.next_line().await.unwrap();
        if first.is_none() {
            panic!(
                "command probe exited before interruption: {}",
                (&mut diagnostic).await.unwrap()
            );
        }
        assert_eq!(first.as_deref(), Some("interrupted"));
        assert_eq!(f.worker.starts.load(Ordering::SeqCst), 1);
        let callback = f
            .worker
            .launch
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .callback
            .credential
            .clone();
        let callback = f
            .authority
            .authenticate_worker(callback.expose())
            .await
            .unwrap();
        let process = ProcessIdentity {
            domain: request.domain,
            boot: Uuid::new_v4(),
            init_pid: 42,
            start_ticks: 11,
            namespace_inode: 23,
        };
        for event in [
            WorkerEvent::Attached {
                process: process.clone(),
            },
            WorkerEvent::Release,
            WorkerEvent::Started,
        ] {
            broker.worker_event(&callback, event).await.unwrap();
        }
        f.store
            .revoke_grant(&f.owner, caller.scope, f.grant.id)
            .await
            .unwrap();
        for event in [
            WorkerEvent::Output {
                sequence: 1,
                stream: Stream::Stdout,
                data: BASE64_URL_SAFE_NO_PAD.encode(output),
            },
            WorkerEvent::Exited {
                exit: ExitStatus::Code(23),
            },
            WorkerEvent::OutputComplete { sequence: 1 },
            WorkerEvent::Drained {
                process,
                sequence: 1,
            },
        ] {
            broker.worker_event(&callback, event).await.unwrap();
        }
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"resume\n")
            .await
            .unwrap();
        let line = lines.next_line().await.unwrap();
        let status = child.wait().await.unwrap();
        (status, line)
    })
    .await;
    if result.is_err() {
        child.kill().await.unwrap();
    }
    let diagnostic = diagnostic.await.unwrap();
    shutdown.send(()).unwrap();
    server.await.unwrap();
    let (status, line) =
        result.expect("the real command client did not finish within the configured deadline");
    assert!(status.success(), "command probe failed: {diagnostic}");
    let result: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
    for name in [
        "interrupted",
        "originalOperation",
        "binaryOutput",
        "revokedNewWork",
        "drained",
    ] {
        assert_eq!(result[name], true, "command probe did not prove {name}");
    }
    assert_eq!(f.worker.starts.load(Ordering::SeqCst), 1);
    assert!(!broker.has_pending(&caller).await.unwrap());
    f.store.pool().close().await;
}

#[tokio::test]
#[ignore = "requires isolated database, compiled goal service, native store and explicit deadline"]
async fn goal_restart() {
    let probe = std::env::var("GESANG_TEST_COMMAND_GOAL_PROBE")
        .expect("compiled command goal probe required");
    let native = std::env::var("CHOPIN_STORAGE_NATIVE").expect("compiled native store required");
    let milliseconds: u64 = std::env::var("GESANG_TEST_OPERATION_TIMEOUT_MS")
        .expect("explicit integration deadline required")
        .parse()
        .unwrap();
    assert!(milliseconds > 0);
    let f = Fixture::new().await;
    let caller = f.caller(&f.runtime).await;
    let request = f.request();
    let goal = Uuid::new_v4();
    let key = Arc::new(gesang_host::ControlKey::new([19; 32]));
    let control_credential = key.issue(&f.runtime).unwrap();
    let job_credential = f.key.issue(&f.runtime).unwrap();
    let custody = Arc::new(gesang_core::Custody::new(
        f.store.clone(),
        gesang_core::CustodyKey::new([31; 32]).unwrap(),
    ));
    custody
        .enroll(
            &f.owner,
            f.runtime.agent_id,
            &gesang_core::CustodyEnrollment {
                operation_id: Uuid::new_v4(),
                expected_revision: 0,
                service_key: gesang_core::ServiceKey::new([29; 32]),
            },
            &FixtureKeyVerifier,
        )
        .await
        .unwrap();
    let plane = gesang_host::ControlPlane::new(
        f.store.clone(),
        key,
        Arc::new(GoalControlPolicy),
        SignedDuration::from_millis(f.settings.recovery_interval_ms.try_into().unwrap()),
    )
    .unwrap()
    .with_execution_custody(custody);
    let broker = Arc::new(f.broker);
    let router = Router::new()
        .nest("/commands", broker.clone().router())
        .merge(plane.router(NonZeroUsize::new(f.settings.max_body_bytes).unwrap()));
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let (shutdown, stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let output = b"shared goal result\n";
    let directory = std::env::temp_dir().join(format!("gesang-command-goal-{goal}"));
    let policy = serde_json::json!({
        "pollAfterMs": f.settings.poll_after_ms,
        "outputPageBytes": f.settings.output_page_bytes,
        "scanPageSize": 1,
        "output": {"maxBytes": f.settings.output_page_bytes, "maxLines": output.iter().filter(|&&byte| byte == b'\n').count() + 1},
    });
    let mut child = Command::new("node")
        .arg(probe)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap())
        .env("CHOPIN_CONTROL_URL", &base)
        .env("CHOPIN_CONTROL_TOKEN", control_credential.expose())
        .env("CHOPIN_AGENT_ID", f.runtime.agent_id.to_string())
        .env("CHOPIN_RUNTIME_EPOCH", f.runtime.generation.to_string())
        .env("CHOPIN_ENGINE_OWNER", &f.runtime.engine_owner)
        .env("CHOPIN_TEST_STORAGE_DIR", directory)
        .env(
            "CHOPIN_TEST_STORE_KEY",
            BASE64_URL_SAFE_NO_PAD.encode([29; 32]),
        )
        .env("CHOPIN_STORAGE_NATIVE", native)
        .env("CHOPIN_TEST_JOB_URL", format!("{base}commands/"))
        .env("CHOPIN_TEST_JOB_CREDENTIAL", job_credential.expose())
        .env("CHOPIN_TEST_JOB_DOMAIN", f.runtime.id.to_string())
        .env("CHOPIN_TEST_COMMAND_POLICY", policy.to_string())
        .env(
            "CHOPIN_TEST_VAULT_SYNC_MS",
            f.settings.recovery_interval_ms.to_string(),
        )
        .env("CHOPIN_TEST_GOAL_REQUEST", goal.to_string())
        .env(
            "CHOPIN_TEST_GOAL_GRANT",
            serde_json::json!({"id": f.grant.id, "version": f.grant.version}).to_string(),
        )
        .env(
            "CHOPIN_TEST_JOB_ARGV",
            serde_json::to_string(&request.argv).unwrap(),
        )
        .env(
            "CHOPIN_TEST_JOB_OUTPUT",
            BASE64_URL_SAFE_NO_PAD.encode(output),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut stderr = child.stderr.take().unwrap();
    let mut diagnostic = tokio::spawn(async move {
        let mut text = String::new();
        stderr.read_to_string(&mut text).await.unwrap();
        text
    });
    let result = timeout(Duration::from_millis(milliseconds), async {
        let first = lines.next_line().await.unwrap();
        if first.is_none() { panic!("command goal probe exited before interruption: {}", (&mut diagnostic).await.unwrap()); }
        assert_eq!(first.as_deref(), Some("interrupted"));
        let launch = f.worker.launch.lock().unwrap().clone().unwrap();
        assert!(matches!(launch.request.source, Source::Tool { ref goal_id, conversation_id, task_id, .. } if goal_id == &goal.to_string() && conversation_id > 0 && task_id > 0));
        assert_eq!(launch.request.argv, request.argv);
        assert!(launch.request.env.is_empty()); assert!(!launch.request.inherit_env);
        assert_eq!(f.worker.starts.load(Ordering::SeqCst), 1);
        let callback = f.authority.authenticate_worker(launch.callback.credential.expose()).await.unwrap();
        let process = ProcessIdentity { domain: request.domain, boot: Uuid::new_v4(), init_pid: 42, start_ticks: 11, namespace_inode: 23 };
        for event in [
            WorkerEvent::Attached { process: process.clone() }, WorkerEvent::Release, WorkerEvent::Started,
            WorkerEvent::Output { sequence: 1, stream: Stream::Stdout, data: BASE64_URL_SAFE_NO_PAD.encode(output) },
            WorkerEvent::Exited { exit: ExitStatus::Code(0) }, WorkerEvent::OutputComplete { sequence: 1 },
            WorkerEvent::Drained { process, sequence: 1 },
        ] { broker.worker_event(&callback, event).await.unwrap(); }
        child.stdin.take().unwrap().write_all(b"resume\n").await.unwrap();
        let line = lines.next_line().await.unwrap();
        let status = child.wait().await.unwrap();
        (status, line)
    }).await;
    if result.is_err() {
        child.kill().await.unwrap();
    }
    let diagnostic = diagnostic.await.unwrap();
    shutdown.send(()).unwrap();
    server.await.unwrap();
    let (status, line) =
        result.expect("the real goal did not recover within the configured deadline");
    assert!(status.success(), "command goal probe failed: {diagnostic}");
    let result: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
    for name in [
        "interrupted",
        "encrypted",
        "anchored",
        "originalGoal",
        "committedToolOutput",
        "originalRequest",
        "reusedIntent",
        "custodyUnlock",
        "completeOutputRead",
        "completed",
    ] {
        assert_eq!(
            result[name], true,
            "command goal probe did not prove {name}"
        );
    }
    assert_eq!(f.worker.starts.load(Ordering::SeqCst), 1);
    assert!(!broker.has_pending(&caller).await.unwrap());
    assert!(f.store.anchor(&f.runtime).await.unwrap().head.sequence > 0);
    let releases: i64 = sqlx::query_scalar("SELECT count(*) FROM gesang_custody_releases WHERE agent_id=$1 AND runtime_id=$2 AND generation=$3")
        .bind(f.runtime.agent_id).bind(f.runtime.id).bind(f.runtime.generation)
        .fetch_one(f.store.pool()).await.unwrap();
    assert_eq!(
        releases, 2,
        "each execution process must obtain its own current custody release"
    );
    f.store.pool().close().await;
}
