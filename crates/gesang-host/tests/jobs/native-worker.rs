//! Opt-in native worker used by the encrypted execution-process restart probe.

use std::{net::TcpListener as LocalListener, process::Stdio, time::Duration};

use chopin_job_broker::{ControlClient, HttpWorkerTransport, TransportPolicy, WorkerDirectory};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};

use super::*;

struct Directory {
    domain: Uuid,
    endpoint: WorkerEndpoint,
}

#[async_trait]
impl WorkerDirectory for Directory {
    async fn locate(
        &self,
        caller: &Caller,
        request: &Request,
        expected: Option<Uuid>,
    ) -> chopin_job_broker::Result<WorkerEndpoint> {
        if caller.domain != self.domain
            || request.domain != self.domain
            || expected.is_some_and(|worker| worker != self.endpoint.worker())
        {
            return Err(JobError::Conflict);
        }
        Ok(self.endpoint.clone())
    }
}

pub(super) struct NativeWorker {
    runner: Child,
    messages: Lines<BufReader<ChildStdout>>,
    input: ChildStdin,
    deadline: Duration,
}

impl NativeWorker {
    pub(super) async fn start(fixture: &mut Fixture, base: &str, milliseconds: u64) -> Self {
        let runner = std::env::var_os("GESANG_TEST_NATIVE_WORKER_RUNNER")
            .expect("isolated Linux worker runner required");
        let port = LocalListener::bind(("127.0.0.1", 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let worker_id = fixture.worker.id;
        let registration = Credential::new(format!("worker-{}", Uuid::new_v4())).unwrap();
        let scope = fixture.runtime.agent_id;
        let domain = fixture.runtime.id;
        let callback = format!("{base}commands/");
        let mut child = Command::new(runner)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut messages = BufReader::new(child.stdout.take().unwrap()).lines();
        let request = json!({
            "worker": worker_id, "scope": scope, "domain": domain,
            "registration": registration.expose(), "callbackBase": callback,
            "workerPort": port, "timeoutMs": milliseconds,
        });
        input
            .write_all(request.to_string().as_bytes())
            .await
            .unwrap();
        input.write_all(b"\n").await.unwrap();
        let ready = timeout(Duration::from_millis(milliseconds), messages.next_line())
            .await
            .unwrap()
            .unwrap()
            .expect("native worker runner exited before readiness");
        let ready: Value = serde_json::from_str(&ready).unwrap();
        assert_eq!(ready["state"], "ready");
        let endpoint = WorkerEndpoint::new(
            &format!("http://127.0.0.1:{port}/"),
            registration,
            worker_id,
        )
        .unwrap();
        fixture.settings.request_timeout_ms = milliseconds;
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
        let control = ControlClient::new(
            reqwest::Client::builder()
                .no_proxy()
                .use_preconfigured_tls(tls)
                .timeout(Duration::from_millis(milliseconds)),
            TransportPolicy {
                response_bytes: NonZeroUsize::new(4096).unwrap(),
                retry_after_ms: NonZeroU64::new(fixture.settings.poll_after_ms).unwrap(),
            },
        )
        .unwrap();
        let directory = Arc::new(Directory { domain, endpoint });
        fixture.worker = Arc::new(Worker {
            id: worker_id,
            starts: AtomicUsize::new(0),
            launch: Mutex::new(None),
            lookups: Mutex::new(vec![]),
            forward: Some(Arc::new(HttpWorkerTransport::new(directory, control))),
        });
        fixture.broker = Broker::new(
            fixture.store.pool().clone(),
            Ledger::new(JobKey::new([31; 32])),
            fixture.authority.clone(),
            fixture.worker.clone(),
            fixture.settings.clone(),
            &callback,
        )
        .unwrap();
        Self {
            runner: child,
            messages,
            input,
            deadline: Duration::from_millis(milliseconds),
        }
    }

    pub(super) async fn release(&mut self) {
        self.input.write_all(b"release\n").await.unwrap();
        assert_eq!(
            self.messages.next_line().await.unwrap().as_deref(),
            Some("released")
        );
    }

    pub(super) async fn verify_effect(&mut self) {
        self.input.write_all(b"effect\n").await.unwrap();
        assert_eq!(
            self.messages.next_line().await.unwrap().as_deref(),
            Some("once")
        );
    }

    pub(super) async fn close(self) {
        let Self {
            mut runner,
            input,
            deadline,
            ..
        } = self;
        drop(input);
        let status = timeout(deadline, runner.wait())
            .await
            .expect("native runner did not shut down after EOF")
            .unwrap();
        assert!(status.success(), "native runner exited with {status}");
    }
}
