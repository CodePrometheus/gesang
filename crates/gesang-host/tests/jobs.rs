//! Real broker metadata and authorization; physical worker observations are explicit fixtures.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    collections::BTreeMap,
    num::{NonZeroU64, NonZeroUsize},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request as HttpRequest, StatusCode, header},
};
use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use chopin_job_broker::{
    Action, Broker, Caller, Credential, Error as JobError, ExitStatus, GrantReference,
    HostAuthority, JobId, JobKey, Ledger, Phase, ProcessIdentity, Request, Settings, Snapshot,
    Source, Stream, WorkerEndpoint, WorkerEvent, WorkerObservation, WorkerStart, WorkerState,
    WorkerTransport,
};
use chrono::TimeDelta;
use gesang_api::{CapabilityScope, Grant, GrantPurpose, GrantSelection, PermissionMode};
use gesang_core::{
    Admission, Error, InstancePhase, InstancePlan, InstancePurpose, InstanceRecord, Owner,
    PayloadKey, PreviousWriter, Runtime, Store, VerifiedIdentity,
};
use gesang_host::{JobAuthority, JobControlKey, JobPolicy};
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tower::ServiceExt as _;
use uuid::Uuid;

#[path = "jobs/node-client.rs"]
mod node_client;

#[path = "jobs/sqlite.rs"]
mod sqlite;

#[derive(Default)]
struct Policy {
    deny: AtomicBool,
}

#[async_trait]
impl JobPolicy for Policy {
    async fn requirements(
        &self,
        _tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        instance: &InstanceRecord,
        request: &Request,
    ) -> gesang_core::Result<Vec<CapabilityScope>> {
        if self.deny.load(Ordering::SeqCst)
            || request.cwd != "/workspace"
            || request.policy_revision != "fixture"
        {
            return Err(Error::Forbidden);
        }
        Ok(vec![CapabilityScope {
            capability: "command".into(),
            resource: instance.binding.agent.id.to_string(),
        }])
    }
}

struct Worker {
    id: Uuid,
    starts: AtomicUsize,
    launch: Mutex<Option<WorkerStart>>,
    lookups: Mutex<Vec<(Uuid, Option<Uuid>)>>,
}

impl Worker {
    fn observation(&self, id: JobId) -> WorkerObservation {
        WorkerObservation {
            id,
            worker: self.id,
            state: WorkerState::Accepted,
        }
    }
}

#[async_trait]
impl WorkerTransport for Worker {
    async fn locate(
        &self,
        _caller: &Caller,
        request: &Request,
        expected: Option<Uuid>,
    ) -> chopin_job_broker::Result<WorkerEndpoint> {
        self.lookups
            .lock()
            .unwrap()
            .push((request.domain, expected));
        if expected.is_some_and(|id| id != self.id) {
            return Err(JobError::Conflict);
        }
        WorkerEndpoint::new(
            "http://worker.invalid/",
            Credential::new("fixture".into())?,
            self.id,
        )
    }

    async fn start(
        &self,
        _: &WorkerEndpoint,
        launch: &WorkerStart,
    ) -> chopin_job_broker::Result<WorkerObservation> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        *self.launch.lock().unwrap() = Some(launch.clone());
        Ok(self.observation(launch.id))
    }

    async fn observe(
        &self,
        _: &WorkerEndpoint,
        id: JobId,
    ) -> chopin_job_broker::Result<Option<WorkerObservation>> {
        Ok(self
            .launch
            .lock()
            .unwrap()
            .as_ref()
            .filter(|launch| launch.id == id)
            .map(|_| self.observation(id)))
    }

    async fn stop(
        &self,
        _: &WorkerEndpoint,
        id: JobId,
    ) -> chopin_job_broker::Result<WorkerObservation> {
        Ok(self.observation(id))
    }
}

struct Fixture {
    store: Store,
    owner: Owner,
    runtime: Runtime,
    grant: Grant,
    key: Arc<JobControlKey>,
    authority: Arc<JobAuthority>,
    policy: Arc<Policy>,
    worker: Arc<Worker>,
    settings: Settings,
    broker: Broker,
}

async fn allocate(
    store: &Store,
    owner: &Owner,
    agent_id: Uuid,
    purpose: InstancePurpose,
) -> Runtime {
    let claim = store
        .claim_runtime(agent_id, Uuid::new_v4(), TimeDelta::minutes(1))
        .await
        .unwrap();
    assert_eq!(claim.agent.owner_id, owner.id);
    let intent = store
        .begin_instance(
            &claim,
            &InstancePlan {
                purpose,
                provider: "metadata-fixture".into(),
                schema_version: 1,
                definition: serde_json::json!({}),
            },
        )
        .await
        .unwrap();
    let mut intent = store
        .admit_instance(
            &intent,
            &Admission {
                runtime_id: intent.id,
                locator: format!("fixture:{}", intent.id),
                engine_owner: claim.engine_owner.clone(),
                previous_writer: claim
                    .previous
                    .as_ref()
                    .map_or(PreviousWriter::Absent, |old| {
                        PreviousWriter::Terminated(old.id)
                    }),
            },
        )
        .await
        .unwrap();
    if purpose == InstancePurpose::Execution {
        intent = store
            .transition_instance(&intent, InstancePhase::Unlocking)
            .await
            .unwrap();
        intent = store
            .transition_instance(&intent, InstancePhase::Ready)
            .await
            .unwrap();
    }
    intent.runtime.unwrap()
}

impl Fixture {
    async fn new() -> Self {
        let url = std::env::var("GESANG_TEST_DATABASE_URL").expect("integration database required");
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
        sqlx::raw_sql(chopin_job_broker::SCHEMA_V1)
            .execute(store.pool())
            .await
            .unwrap();
        let owner = store
            .bind_owner(&VerifiedIdentity {
                issuer: "jobs".into(),
                subject: "person".into(),
            })
            .await
            .unwrap();
        let agent = store.ensure_agent(&owner, "engine", "local").await.unwrap();
        store
            .bind_placement(
                &owner,
                agent.id,
                &[format!("fixture:{schema}")],
                "command-fixture",
            )
            .await
            .unwrap();
        let runtime = allocate(&store, &owner, agent.id, InstancePurpose::Execution).await;
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
        let key = Arc::new(JobControlKey::new([17; 32]));
        let policy = Arc::new(Policy::default());
        let authority = Arc::new(JobAuthority::new(
            store.clone(),
            key.clone(),
            policy.clone(),
            NonZeroU64::new(7).unwrap(),
        ));
        let worker = Arc::new(Worker {
            id: Uuid::new_v4(),
            starts: AtomicUsize::new(0),
            launch: Mutex::new(None),
            lookups: Mutex::new(vec![]),
        });
        let settings = Settings {
            version: 1,
            revision: "fixture".into(),
            poll_after_ms: 1,
            recovery_interval_ms: 7,
            request_timeout_ms: 1000,
            frame_bytes: 32,
            output_page_bytes: 64,
            max_body_bytes: 4096,
        };
        let broker = Broker::new(
            store.pool().clone(),
            Ledger::new(JobKey::new([31; 32])),
            authority.clone(),
            worker.clone(),
            settings.clone(),
            "http://broker.invalid/internal/jobs/",
        )
        .unwrap();
        Self {
            store,
            owner,
            runtime,
            grant,
            key,
            authority,
            policy,
            worker,
            settings,
            broker,
        }
    }

    async fn caller(&self, runtime: &Runtime) -> Caller {
        self.authority
            .authenticate(self.key.issue(runtime).unwrap().expose())
            .await
            .unwrap()
    }

    fn request(&self) -> Request {
        Request {
            version: 1,
            domain: self.runtime.id,
            source: Source::Tool {
                goal_id: "goal".into(),
                conversation_id: 1,
                task_id: 1,
                call_id: "call".into(),
            },
            grant: GrantReference {
                id: self.grant.id,
                version: self.grant.version.try_into().unwrap(),
            },
            argv: vec!["sh".into(), "-c".into(), "fixture command".into()],
            cwd: "/workspace".into(),
            env: BTreeMap::new(),
            inherit_env: false,
            timeout_ms: None,
            policy_revision: "fixture".into(),
        }
    }

    async fn callback(&self) -> chopin_job_broker::WorkerCaller {
        let credential = self
            .worker
            .launch
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .callback
            .credential
            .clone();
        self.authority
            .authenticate_worker(credential.expose())
            .await
            .unwrap()
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn cancelled_before_submit() {
    let f = Fixture::new().await;
    let caller = f.caller(&f.runtime).await;
    assert!(!f.broker.has_pending(&caller).await.unwrap());
    let operation = Uuid::new_v4();
    let mut request = f.request();
    request.grant.id = Uuid::new_v4();
    f.policy.deny.store(true, Ordering::SeqCst);
    let cancelled = f.broker.cancel(&caller, operation, &request).await.unwrap();
    assert_eq!(cancelled.state.phase, Phase::Drained);
    assert!(cancelled.state.stop_requested);
    assert!(cancelled.state.output_complete);
    assert_eq!(cancelled.state.worker, None);
    let repeated = f.broker.cancel(&caller, operation, &request).await.unwrap();
    assert_eq!(repeated, cancelled);
    let late = f.broker.submit(&caller, operation, &request).await.unwrap();
    assert_eq!(late, cancelled);
    assert_eq!(f.worker.starts.load(Ordering::SeqCst), 0);
    assert!(f.worker.lookups.lock().unwrap().is_empty());
    assert!(!f.broker.has_pending(&caller).await.unwrap());
    request.argv.push("different".into());
    assert!(matches!(
        f.broker.cancel(&caller, operation, &request).await,
        Err(JobError::Conflict)
    ));
    assert!(matches!(
        f.broker.submit(&caller, operation, &request).await,
        Err(JobError::Conflict)
    ));
    assert_eq!(f.worker.starts.load(Ordering::SeqCst), 0);
    f.store.pool().close().await;
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn revoked_authority() {
    let f = Fixture::new().await;
    let caller = f.caller(&f.runtime).await;
    let request = f.request();
    let operation = Uuid::new_v4();
    f.broker.submit(&caller, operation, &request).await.unwrap();
    let callback = f.callback().await;
    let process = ProcessIdentity {
        domain: request.domain,
        boot: Uuid::new_v4(),
        init_pid: 42,
        start_ticks: 11,
        namespace_inode: 23,
    };
    f.broker
        .worker_event(
            &callback,
            WorkerEvent::Attached {
                process: process.clone(),
            },
        )
        .await
        .unwrap();
    f.broker
        .worker_event(&callback, WorkerEvent::Release)
        .await
        .unwrap();
    f.broker
        .worker_event(&callback, WorkerEvent::Started)
        .await
        .unwrap();
    f.store
        .revoke_grant(&f.owner, caller.scope, f.grant.id)
        .await
        .unwrap();
    assert!(matches!(
        f.broker.worker_event(&callback, WorkerEvent::Release).await,
        Err(JobError::Forbidden)
    ));
    assert!(matches!(
        f.broker.submit(&caller, Uuid::new_v4(), &request).await,
        Err(JobError::Forbidden)
    ));
    f.broker.submit(&caller, operation, &request).await.unwrap();
    assert_eq!(f.worker.starts.load(Ordering::SeqCst), 1);
    f.broker
        .worker_event(
            &callback,
            WorkerEvent::Output {
                sequence: 1,
                stream: Stream::Stdout,
                data: BASE64_URL_SAFE_NO_PAD.encode(b"result"),
            },
        )
        .await
        .unwrap();
    assert!(
        f.broker
            .stop(&caller, operation)
            .await
            .unwrap()
            .state
            .stop_requested
    );
    f.broker
        .worker_event(
            &callback,
            WorkerEvent::Exited {
                exit: ExitStatus::Code(23),
            },
        )
        .await
        .unwrap();
    f.broker
        .worker_event(&callback, WorkerEvent::OutputComplete { sequence: 1 })
        .await
        .unwrap();
    let drained = f
        .broker
        .worker_event(
            &callback,
            WorkerEvent::Drained {
                process,
                sequence: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(drained.state.phase, Phase::Drained);
    assert_eq!(drained.state.primary_exit, Some(ExitStatus::Code(23)));
    let page = f
        .broker
        .output(&caller, operation, None, NonZeroUsize::new(64).unwrap())
        .await
        .unwrap();
    assert_eq!(BASE64_URL_SAFE_NO_PAD.decode(page.data).unwrap(), b"result");
    let wrong = f
        .authority
        .worker_credential(
            &caller,
            JobId {
                scope: caller.scope,
                operation,
            },
            Uuid::new_v4(),
        )
        .unwrap();
    let wrong = f
        .authority
        .authenticate_worker(wrong.expose())
        .await
        .unwrap();
    assert!(matches!(
        f.broker.worker_event(&wrong, WorkerEvent::Read).await,
        Err(JobError::Forbidden)
    ));
    let engine = f.key.issue(&f.runtime).unwrap();
    assert!(matches!(
        f.authority.authenticate_worker(engine.expose()).await,
        Err(JobError::Unauthorized)
    ));
    let worker_token = f
        .worker
        .launch
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .callback
        .credential
        .clone();
    assert!(matches!(
        f.authority.authenticate(worker_token.expose()).await,
        Err(JobError::Unauthorized)
    ));
    let another = JobAuthority::new(
        f.store.clone(),
        Arc::new(JobControlKey::new([18; 32])),
        f.policy.clone(),
        NonZeroU64::new(7).unwrap(),
    );
    assert!(matches!(
        another.authenticate(engine.expose()).await,
        Err(JobError::Unauthorized)
    ));
    f.store.pool().close().await;
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn replacement_runtime() {
    let f = Fixture::new().await;
    let old = f.caller(&f.runtime).await;
    let request = f.request();
    let operation = Uuid::new_v4();
    f.broker.submit(&old, operation, &request).await.unwrap();
    let callback = f.callback().await;
    for event in [
        WorkerEvent::Attached {
            process: ProcessIdentity {
                domain: request.domain,
                boot: Uuid::new_v4(),
                init_pid: 42,
                start_ticks: 11,
                namespace_inode: 23,
            },
        },
        WorkerEvent::Release,
        WorkerEvent::Started,
        WorkerEvent::Output {
            sequence: 1,
            stream: Stream::Stdout,
            data: BASE64_URL_SAFE_NO_PAD.encode(b"before"),
        },
    ] {
        f.broker.worker_event(&callback, event).await.unwrap();
    }
    let first = f
        .broker
        .output(&old, operation, None, NonZeroUsize::new(3).unwrap())
        .await
        .unwrap();
    assert_eq!(BASE64_URL_SAFE_NO_PAD.decode(first.data).unwrap(), b"bef");
    sqlx::query("UPDATE gesang_agents SET lease_until=clock_timestamp() WHERE id=$1")
        .bind(old.scope)
        .execute(f.store.pool())
        .await
        .unwrap();
    assert!(matches!(
        f.broker.get(&old, operation).await,
        Err(JobError::Forbidden)
    ));
    assert!(matches!(
        f.broker.has_pending(&old).await,
        Err(JobError::Forbidden)
    ));
    f.broker
        .worker_event(&callback, WorkerEvent::Read)
        .await
        .unwrap();
    assert!(matches!(
        f.broker.worker_event(&callback, WorkerEvent::Release).await,
        Err(JobError::Forbidden)
    ));
    let replacement = allocate(&f.store, &f.owner, old.scope, InstancePurpose::Execution).await;
    let current = f.caller(&replacement).await;
    assert_ne!(current.domain, request.domain);
    f.broker.get(&current, operation).await.unwrap();
    assert!(f.broker.has_pending(&current).await.unwrap());
    let resumed = f
        .broker
        .output(
            &current,
            operation,
            Some(&first.cursor),
            NonZeroUsize::new(64).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(BASE64_URL_SAFE_NO_PAD.decode(resumed.data).unwrap(), b"ore");
    f.broker
        .worker_event(
            &callback,
            WorkerEvent::Output {
                sequence: 2,
                stream: Stream::Stderr,
                data: BASE64_URL_SAFE_NO_PAD.encode(b"after"),
            },
        )
        .await
        .unwrap();
    let late = f
        .broker
        .output(
            &current,
            operation,
            Some(&resumed.cursor),
            NonZeroUsize::new(64).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(late.stream, Some(Stream::Stderr));
    assert_eq!(BASE64_URL_SAFE_NO_PAD.decode(late.data).unwrap(), b"after");
    f.broker.stop(&current, operation).await.unwrap();
    assert_eq!(
        f.worker.lookups.lock().unwrap().last().copied(),
        Some((request.domain, Some(f.worker.id)))
    );
    assert_eq!(f.worker.starts.load(Ordering::SeqCst), 1);
    f.broker
        .worker_event(&callback, WorkerEvent::Read)
        .await
        .unwrap();
    assert!(matches!(
        f.broker.worker_event(&callback, WorkerEvent::Release).await,
        Err(JobError::Forbidden)
    ));
    assert!(matches!(
        f.broker.submit(&current, Uuid::new_v4(), &request).await,
        Err(JobError::Forbidden)
    ));
    let mut foreign = request;
    foreign.domain = Uuid::new_v4();
    let mut tx = f.store.pool().begin().await.unwrap();
    assert!(matches!(
        f.authority
            .authorize(&mut tx, &current, &foreign, Action::Read)
            .await,
        Err(JobError::Forbidden)
    ));
    tx.rollback().await.unwrap();
    f.store.pool().close().await;
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn private_routes() {
    let f = Fixture::new().await;
    let request = f.request();
    let credential = f.key.issue(&f.runtime).unwrap();
    let operation = Uuid::new_v4();
    let path = format!("/{operation}");
    let router = Arc::new(f.broker).router();
    let accepted = router
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("PUT")
                .uri(&path)
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", credential.expose()),
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);
    assert_eq!(accepted.headers()[header::CACHE_CONTROL], "no-store");
    let accepted: Snapshot =
        serde_json::from_slice(&to_bytes(accepted.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(accepted.operation_id, operation);
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
    for (method, uri, token, expected) in [
        ("GET", path.clone(), Some(&credential), StatusCode::OK),
        (
            "POST",
            format!("{path}/worker"),
            Some(&callback),
            StatusCode::OK,
        ),
        (
            "GET",
            path.clone(),
            Some(&callback),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            format!("{path}/worker"),
            Some(&credential),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            format!("/{}/worker", Uuid::new_v4()),
            Some(&callback),
            StatusCode::FORBIDDEN,
        ),
        ("GET", path.clone(), None, StatusCode::UNAUTHORIZED),
    ] {
        let mut call = HttpRequest::builder().method(method).uri(uri);
        if let Some(token) = token {
            call = call.header(header::AUTHORIZATION, format!("Bearer {}", token.expose()));
        }
        let response = router
            .clone()
            .oneshot(
                call.header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&WorkerEvent::Read).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    }
    assert_eq!(f.worker.starts.load(Ordering::SeqCst), 1);
    f.store.pool().close().await;
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn required_policy() {
    let f = Fixture::new().await;
    let caller = f.caller(&f.runtime).await;
    let mut request = f.request();
    for field in ["cwd", "policy"] {
        let mut invalid = request.clone();
        if field == "cwd" {
            invalid.cwd = "/protected".into();
        } else {
            invalid.policy_revision = "unpublished".into();
        }
        assert!(matches!(
            f.broker.submit(&caller, Uuid::new_v4(), &invalid).await,
            Err(JobError::Forbidden)
        ));
    }
    request.source = Source::Learning {
        run_id: Uuid::new_v4(),
    };
    assert!(matches!(
        f.broker.submit(&caller, Uuid::new_v4(), &request).await,
        Err(JobError::Forbidden)
    ));
    let grant = f
        .store
        .select_grant_for(
            &f.owner,
            caller.scope,
            GrantPurpose::Improvement,
            &GrantSelection {
                mode: PermissionMode::FullAccess,
                scopes: vec![],
            },
        )
        .await
        .unwrap();
    request.grant = GrantReference {
        id: grant.id,
        version: grant.version.try_into().unwrap(),
    };
    f.broker
        .submit(&caller, Uuid::new_v4(), &request)
        .await
        .unwrap();
    let callback = f.callback().await;
    f.policy.deny.store(true, Ordering::SeqCst);
    assert!(matches!(
        f.broker.worker_event(&callback, WorkerEvent::Release).await,
        Err(JobError::Forbidden)
    ));
    f.policy.deny.store(false, Ordering::SeqCst);
    request.source = f.request().source;
    assert!(matches!(
        f.broker.submit(&caller, Uuid::new_v4(), &request).await,
        Err(JobError::Forbidden)
    ));
    let other = f
        .store
        .bind_owner(&VerifiedIdentity {
            issuer: "jobs".into(),
            subject: "migrator".into(),
        })
        .await
        .unwrap();
    let agent = f
        .store
        .ensure_agent(&other, "engine", "local")
        .await
        .unwrap();
    let migration = allocate(&f.store, &other, agent.id, InstancePurpose::Migration).await;
    let migration_caller = f.caller(&migration).await;
    request.domain = migration.id;
    assert!(matches!(
        f.broker
            .submit(&migration_caller, Uuid::new_v4(), &request)
            .await,
        Err(JobError::Forbidden)
    ));
    assert!(matches!(
        f.broker.get(&migration_caller, callback.operation).await,
        Err(JobError::NotFound)
    ));
    f.store.pool().close().await;
}
