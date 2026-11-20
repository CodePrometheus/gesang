use std::{path::Path, process::Stdio, time::Duration};

use chopin_job_broker::{LedgerStore, SQLITE_SCHEMA_V1};
use gesang_core::{CoordinationTransaction, GrantRequirement};
use gesang_host::TransactionalJobPolicy;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};

use super::*;

const DATABASE: &str = "GESANG_TEST_JOB_RESTART_DATABASE";
const INPUT: &str = "GESANG_TEST_JOB_RESTART_INPUT";
const READY: &str = "sqlite command and output committed";

struct SqlitePolicy;

#[async_trait]
impl TransactionalJobPolicy for SqlitePolicy {
    async fn requirements(
        &self,
        tx: &mut CoordinationTransaction<'_, '_>,
        instance: &InstanceRecord,
        request: &Request,
    ) -> gesang_core::Result<Vec<CapabilityScope>> {
        let CoordinationTransaction::Sqlite(tx) = tx else {
            return Err(Error::StorageBackend);
        };
        let allowed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM host_job_policy WHERE domain=$1 AND revision=$2)",
        )
        .bind(instance.runtime.id)
        .bind(&request.policy_revision)
        .fetch_one(&mut ***tx)
        .await?;
        if !allowed || request.cwd != "/workspace" {
            return Err(Error::Forbidden);
        }
        Ok(vec![CapabilityScope {
            capability: "command".into(),
            resource: instance.runtime.agent_id.to_string(),
        }])
    }
}

async fn open(path: &Path) -> Store {
    Store::open_sqlite(path, PayloadKey::new([42; 32]))
        .await
        .unwrap()
}

fn request(runtime: &Runtime, grant: &Grant) -> Request {
    Request {
        version: 1,
        domain: runtime.id,
        source: Source::Tool {
            goal_id: "goal".into(),
            conversation_id: 1,
            task_id: 1,
            call_id: "call".into(),
        },
        grant: GrantReference {
            id: grant.id,
            version: grant.version.try_into().unwrap(),
        },
        argv: vec!["sh".into(), "-c".into(), "printf evidence".into()],
        cwd: "/workspace".into(),
        env: BTreeMap::new(),
        inherit_env: false,
        timeout_ms: None,
        policy_revision: "fixture".into(),
    }
}

fn broker(
    store: &Store,
    worker_id: Uuid,
) -> (Broker, Arc<Worker>, Arc<JobControlKey>, Arc<JobAuthority>) {
    let key = Arc::new(JobControlKey::new([17; 32]));
    let authority = Arc::new(JobAuthority::with_transactional_policy(
        store.clone(),
        key.clone(),
        Arc::new(SqlitePolicy),
        NonZeroU64::new(7).unwrap(),
    ));
    let worker = Arc::new(Worker {
        id: worker_id,
        starts: AtomicUsize::new(0),
        launch: Mutex::new(None),
        lookups: Mutex::new(vec![]),
    });
    let broker = Broker::with_ledger(
        LedgerStore::Sqlite(store.sqlite_pool().unwrap().clone()),
        Ledger::new(JobKey::new([31; 32])),
        authority.clone(),
        worker.clone(),
        Settings {
            version: 1,
            revision: "fixture".into(),
            poll_after_ms: 1,
            recovery_interval_ms: 7,
            request_timeout_ms: 1000,
            frame_bytes: 32,
            output_page_bytes: 64,
            max_body_bytes: 4096,
        },
        "http://broker.invalid/internal/jobs/",
    )
    .unwrap();
    (broker, worker, key, authority)
}

async fn sqlite_writer_child(path: &Path) {
    let (runtime, request, operation, worker_id, process): (
        Runtime,
        Request,
        Uuid,
        Uuid,
        ProcessIdentity,
    ) = serde_json::from_str(&std::env::var(INPUT).unwrap()).unwrap();
    let store = open(path).await;
    let (broker, worker, key, authority) = broker(&store, worker_id);
    let caller = authority
        .authenticate(key.issue(&runtime).unwrap().expose())
        .await
        .unwrap();
    broker.submit(&caller, operation, &request).await.unwrap();
    let credential = worker
        .launch
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .callback
        .credential
        .clone();
    let callback = authority
        .authenticate_worker(credential.expose())
        .await
        .unwrap();
    for event in [
        WorkerEvent::Attached {
            process: process.clone(),
        },
        WorkerEvent::Release,
        WorkerEvent::Started,
        WorkerEvent::Output {
            sequence: 1,
            stream: Stream::Stdout,
            data: BASE64_URL_SAFE_NO_PAD.encode(b"evidence"),
        },
    ] {
        broker.worker_event(&callback, event).await.unwrap();
    }
    let state = broker.get(&caller, operation).await.unwrap();
    assert_eq!(state.state.process, Some(process));
    assert_eq!(state.state.last_sequence, 1);
    println!("{READY}");
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn sqlite_reopens_original_operation_and_fences_new_execution() {
    if let Some(path) = std::env::var_os(DATABASE) {
        sqlite_writer_child(Path::new(&path)).await;
        return;
    }
    let path = std::env::temp_dir().join(format!("gesang-jobs-{}.sqlite", Uuid::new_v4()));
    let store = open(&path).await;
    let pool = store.sqlite_pool().unwrap();
    let mut schema = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::raw_sql(SQLITE_SCHEMA_V1)
        .execute(&mut *schema)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE host_job_policy(domain BLOB PRIMARY KEY, revision TEXT NOT NULL) STRICT",
    )
    .execute(&mut *schema)
    .await
    .unwrap();
    schema.commit().await.unwrap();

    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "test".into(),
            subject: "sqlite".into(),
        })
        .await
        .unwrap();
    let agent = store.ensure_agent(&owner, "engine", "local").await.unwrap();
    let volumes = vec!["volume:b".into(), "volume:a".into()];
    store
        .bind_placement(&owner, agent.id, &volumes, "workspace")
        .await
        .unwrap();
    assert_eq!(
        store.agent_binding(agent.id).await.unwrap().workspace_keys,
        ["volume:a", "volume:b"]
    );
    let runtime = allocate(&store, &owner, agent.id, InstancePurpose::Execution).await;
    let grant = store
        .select_grant(
            &owner,
            agent.id,
            &GrantSelection {
                mode: PermissionMode::Scoped,
                scopes: vec![CapabilityScope {
                    capability: "command".into(),
                    resource: agent.id.to_string(),
                }],
            },
        )
        .await
        .unwrap();
    sqlx::query("INSERT INTO host_job_policy(domain,revision) VALUES($1,'fixture')")
        .bind(runtime.id)
        .execute(pool)
        .await
        .unwrap();

    pool.close().await;
    let original = request(&runtime, &grant);
    let operation = Uuid::new_v4();
    let worker_id = Uuid::new_v4();
    let process = ProcessIdentity {
        domain: runtime.id,
        boot: Uuid::new_v4(),
        init_pid: 42,
        start_ticks: 11,
        namespace_inode: 23,
    };
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "sqlite::sqlite_reopens_original_operation_and_fences_new_execution",
            "--nocapture",
        ])
        .env(DATABASE, &path)
        .env(
            INPUT,
            serde_json::to_string(&(&runtime, &original, operation, worker_id, &process)).unwrap(),
        )
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    tokio::time::timeout(Duration::from_secs(60), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if line == READY {
                return;
            }
        }
        panic!("writer exited before committing");
    })
    .await
    .unwrap();
    child.kill().await.unwrap();
    let status = child.wait().await.unwrap();
    assert!(!status.success());

    let reopened = open(&path).await;
    let pool = reopened.sqlite_pool().unwrap();
    let (broker, worker, key, authority) = broker(&reopened, worker_id);
    let caller = authority
        .authenticate(key.issue(&runtime).unwrap().expose())
        .await
        .unwrap();
    let original_state = broker.get(&caller, operation).await.unwrap();
    assert_eq!(original_state.state.phase, Phase::Running);
    assert_eq!(original_state.state.process, Some(process));
    assert_eq!(
        original_state,
        broker.submit(&caller, operation, &original).await.unwrap()
    );
    let first = broker
        .output(&caller, operation, None, NonZeroUsize::new(3).unwrap())
        .await
        .unwrap();
    let rest = broker
        .output(
            &caller,
            operation,
            Some(&first.cursor),
            NonZeroUsize::new(64).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(BASE64_URL_SAFE_NO_PAD.decode(first.data).unwrap(), b"evi");
    assert_eq!(BASE64_URL_SAFE_NO_PAD.decode(rest.data).unwrap(), b"dence");
    let mut changed = original.clone();
    changed.argv.push("changed".into());
    assert!(matches!(
        broker.submit(&caller, operation, &changed).await,
        Err(JobError::Conflict)
    ));

    // Authorization, including host-owned policy, can see writes in the ledger's transaction.
    let mut tx = LedgerStore::Sqlite(pool.clone()).begin().await.unwrap();
    let chopin_job_broker::StoreTransaction::Sqlite(sqlite) = &mut tx else {
        unreachable!()
    };
    sqlx::query("UPDATE host_job_policy SET revision='new' WHERE domain=$1")
        .bind(runtime.id)
        .execute(&mut **sqlite)
        .await
        .unwrap();
    assert!(matches!(
        chopin_job_broker::TransactionalHostAuthority::authorize(
            &*authority,
            &mut tx,
            &caller,
            &original,
            Action::Submit
        )
        .await,
        Err(JobError::Forbidden)
    ));
    tx.rollback().await.unwrap();

    reopened
        .revoke_grant(&owner, agent.id, grant.id)
        .await
        .unwrap();
    let denied = Uuid::new_v4();
    assert!(matches!(
        broker.submit(&caller, denied, &original).await,
        Err(JobError::Forbidden)
    ));
    assert!(matches!(
        broker.get(&caller, denied).await,
        Err(JobError::NotFound)
    ));
    assert_eq!(
        original_state,
        broker.submit(&caller, operation, &original).await.unwrap()
    );
    let callback = authority
        .worker_credential(
            &caller,
            JobId {
                scope: caller.scope,
                operation,
            },
            worker_id,
        )
        .unwrap();
    let callback = authority
        .authenticate_worker(callback.expose())
        .await
        .unwrap();
    assert!(matches!(
        broker.worker_event(&callback, WorkerEvent::Release).await,
        Err(JobError::Forbidden)
    ));
    assert_eq!(
        broker
            .worker_event(&callback, WorkerEvent::Read)
            .await
            .unwrap(),
        original_state
    );

    let next = reopened
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
    let mut still_authorized = original.clone();
    still_authorized.grant = GrantReference {
        id: next.id,
        version: next.version.try_into().unwrap(),
    };
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let required = GrantRequirement {
        id: next.id,
        version: next.version,
        purpose: GrantPurpose::Task,
        scopes: &[],
    };
    reopened
        .authorize_runtime_coordination_in(&mut (&mut tx).into(), &runtime, &required)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(worker.starts.load(Ordering::SeqCst), 0);
    let awaiting_release = Uuid::new_v4();
    broker
        .submit(&caller, awaiting_release, &still_authorized)
        .await
        .unwrap();
    let release_token = worker
        .launch
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .callback
        .credential
        .clone();
    let release_callback = authority
        .authenticate_worker(release_token.expose())
        .await
        .unwrap();
    let original_process = original_state.state.process.as_ref().unwrap();
    broker
        .worker_event(
            &release_callback,
            WorkerEvent::Attached {
                process: ProcessIdentity {
                    init_pid: original_process.init_pid + 1,
                    ..original_process.clone()
                },
            },
        )
        .await
        .unwrap();
    let controller: Uuid =
        sqlx::query_scalar("SELECT controller_id FROM gesang_agents WHERE id=$1")
            .bind(agent.id)
            .fetch_one(pool)
            .await
            .unwrap();
    let claim = reopened
        .claim_runtime(agent.id, controller, TimeDelta::minutes(1))
        .await
        .unwrap();
    let replacement = reopened.advance_fenced_generation(&claim).await.unwrap();
    assert_eq!(replacement.generation, runtime.generation + 1);
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    assert!(matches!(
        reopened
            .authorize_runtime_coordination_in(&mut (&mut tx).into(), &runtime, &required)
            .await,
        Err(Error::StaleGeneration)
    ));
    tx.rollback().await.unwrap();
    let denied = Uuid::new_v4();
    assert!(matches!(
        broker.submit(&caller, denied, &still_authorized).await,
        Err(JobError::Forbidden)
    ));
    let record_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chopin_jobs WHERE operation=$1)")
            .bind(denied)
            .fetch_one(pool)
            .await
            .unwrap();
    assert!(!record_exists);
    assert_eq!(
        broker
            .worker_event(&callback, WorkerEvent::Read)
            .await
            .unwrap(),
        original_state
    );
    assert!(matches!(
        broker
            .worker_event(&release_callback, WorkerEvent::Release)
            .await,
        Err(JobError::Forbidden)
    ));
    assert!(
        !broker
            .worker_event(&release_callback, WorkerEvent::Read)
            .await
            .unwrap()
            .state
            .released
    );
    assert_eq!(worker.starts.load(Ordering::SeqCst), 1);
    pool.close().await;
}
