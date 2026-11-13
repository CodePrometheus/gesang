//! Real durable instance-stage and controller recovery checks.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use gesang_api::{GrantSelection, PermissionMode};
use gesang_core::{
    Activity, Admission, AnchorHead, CommandRequirements, Dispatch, EngineBridge,
    EnginePreparation, Error, ExecutionResponse, InstanceIntent, InstanceObservation,
    InstancePhase, InstancePlan, InstanceProvider, InstancePurpose, Kernel, Owner, PayloadKey,
    PendingAnchor, PreparationProgress, PreviousWriter, Result, Runtime, RuntimeClaim,
    RuntimePolicy, Store, VerifiedIdentity, Wake,
};
use serde_json::{Value, json};
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

struct Fixture {
    store: Store,
    owner: Owner,
    agent_id: Uuid,
}

impl Fixture {
    async fn new() -> Self {
        Self::migrated_to(None).await
    }

    async fn migrated_to(version: Option<i64>) -> Self {
        let url = std::env::var("GESANG_TEST_DATABASE_URL").expect("integration database required");
        let schema = format!("test_{}", Uuid::new_v4().simple());
        let bootstrap = PgPoolOptions::new().connect(&url).await.unwrap();
        bootstrap
            .execute(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .await
            .unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                PgConnectOptions::from_str(&url)
                    .unwrap()
                    .options([("search_path", schema.as_str())]),
            )
            .await
            .unwrap();
        bootstrap.close().await;
        let store = Store::new(pool, PayloadKey::new([32; 32]));
        match version {
            Some(version) => sqlx::migrate!("./migrations")
                .run_to(version, store.pool())
                .await
                .unwrap(),
            None => store.migrate().await.unwrap(),
        }
        let owner = store
            .bind_owner(&VerifiedIdentity {
                issuer: "lifecycle-fixture".into(),
                subject: "person".into(),
            })
            .await
            .unwrap();
        let agent_id = store
            .ensure_agent(&owner, "execution-v1", "local")
            .await
            .unwrap()
            .id;
        Self {
            store,
            owner,
            agent_id,
        }
    }

    async fn claim(&self, controller: Uuid) -> RuntimeClaim {
        self.store
            .claim_runtime(self.agent_id, controller, TimeDelta::minutes(1))
            .await
            .unwrap()
    }
}

fn plan(purpose: InstancePurpose) -> InstancePlan {
    InstancePlan {
        purpose,
        provider: "lifecycle-fixture".into(),
        schema_version: 1,
        definition: json!({"imageDigest":"fixture-content-digest","settingsRevision":1}),
    }
}

fn admission(intent: &InstanceIntent) -> Admission {
    Admission {
        runtime_id: intent.id,
        locator: format!("gated-allocation:{}", intent.id),
        engine_owner: intent.claim.engine_owner.clone(),
        previous_writer: intent
            .claim
            .previous
            .as_ref()
            .map_or(PreviousWriter::Absent, |old| {
                PreviousWriter::Terminated(old.id)
            }),
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn actual_writer_admission_precedes_unlock_but_never_implies_execution_readiness() {
    let fixture = Fixture::new().await;
    let controller = Uuid::new_v4();
    let claim = fixture.claim(controller).await;
    let intent = fixture
        .store
        .begin_instance(&claim, &plan(InstancePurpose::Execution))
        .await
        .unwrap();
    assert_eq!(intent.phase, InstancePhase::Allocating);
    assert!(intent.runtime.is_none());
    assert!(matches!(
        fixture.store.writer(fixture.agent_id).await,
        Err(Error::StaleGeneration)
    ));
    let retry = fixture
        .store
        .begin_instance(&claim, &plan(InstancePurpose::Execution))
        .await
        .unwrap();
    assert_eq!(retry.id, intent.id);
    let mut changed = plan(InstancePurpose::Execution);
    changed.definition["settingsRevision"] = json!(2);
    assert!(matches!(
        fixture.store.begin_instance(&claim, &changed).await,
        Err(Error::RequestConflict)
    ));
    let mut wrong = admission(&intent);
    wrong.runtime_id = Uuid::new_v4();
    assert!(matches!(
        fixture.store.admit_instance(&intent, &wrong).await,
        Err(Error::FencingRequired)
    ));
    let starting = fixture
        .store
        .admit_instance(&intent, &admission(&intent))
        .await
        .unwrap();
    let runtime = starting.runtime.clone().unwrap();
    assert_eq!(
        fixture.store.writer(fixture.agent_id).await.unwrap(),
        runtime
    );
    assert!(matches!(
        fixture.store.runtime(fixture.agent_id).await,
        Err(Error::RuntimeNotReady)
    ));
    assert!(matches!(
        fixture.store.verify_instance_intent(&intent).await,
        Err(Error::VersionConflict)
    ));
    assert!(matches!(
        fixture
            .store
            .transition_instance(&starting, InstancePhase::Ready)
            .await,
        Err(Error::InvalidCommand(_))
    ));
    fixture
        .store
        .initialize_anchor(&runtime, &[0; 32])
        .await
        .unwrap();
    let grant = fixture
        .store
        .select_grant(
            &fixture.owner,
            fixture.agent_id,
            &GrantSelection {
                mode: PermissionMode::FullAccess,
                scopes: vec![],
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .authorize_runtime(&runtime, grant.id, grant.version, &[])
            .await,
        Err(Error::RuntimeNotReady)
    ));
    let unlocking = fixture
        .store
        .transition_instance(&starting, InstancePhase::Unlocking)
        .await
        .unwrap();
    let renewed = fixture.claim(controller).await;
    let reopened = fixture
        .store
        .instance_intent(&renewed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reopened.id, intent.id);
    assert_eq!(reopened.phase, InstancePhase::Unlocking);
    fixture
        .store
        .verify_instance_intent(&reopened)
        .await
        .unwrap();
    let ready = fixture
        .store
        .transition_instance(&reopened, InstancePhase::Ready)
        .await
        .unwrap();
    assert_eq!(
        fixture.store.runtime(fixture.agent_id).await.unwrap(),
        runtime
    );
    fixture
        .store
        .authorize_runtime(&runtime, grant.id, grant.version, &[])
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .transition_instance(&unlocking, InstancePhase::Ready)
            .await,
        Err(Error::VersionConflict)
    ));
    let draining = fixture
        .store
        .transition_instance(&ready, InstancePhase::Draining)
        .await
        .unwrap();
    assert!(matches!(
        fixture.store.verify_execution(&runtime).await,
        Err(Error::RuntimeNotReady)
    ));
    fixture.store.anchor(&runtime).await.unwrap();
    assert!(matches!(
        fixture
            .store
            .finish_instance(
                &draining,
                &PreviousWriter::StorageRevoked(runtime.id),
                None,
                None
            )
            .await,
        Err(Error::FencingRequired)
    ));
    fixture
        .store
        .finish_instance(
            &draining,
            &PreviousWriter::Terminated(runtime.id),
            None,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        fixture.store.anchor(&runtime).await,
        Err(Error::StaleGeneration)
    ));
    let phases: Vec<String> = sqlx::query_scalar(
        "SELECT phase FROM gesang_instance_events WHERE instance_id=$1 ORDER BY revision",
    )
    .bind(intent.id)
    .fetch_all(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        phases,
        [
            "allocating",
            "starting",
            "unlocking",
            "ready",
            "draining",
            "stopped"
        ]
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn migration_is_storage_only_and_completes_only_at_the_confirmed_target_head() {
    let fixture = Fixture::new().await;
    let claim = fixture.claim(Uuid::new_v4()).await;
    assert!(
        fixture
            .store
            .latest_completed_migration(&claim)
            .await
            .unwrap()
            .is_none()
    );
    let intent = fixture
        .store
        .begin_instance(&claim, &plan(InstancePurpose::Migration))
        .await
        .unwrap();
    let migrating = fixture
        .store
        .admit_instance(&intent, &admission(&intent))
        .await
        .unwrap();
    let runtime = migrating.runtime.clone().unwrap();
    assert_eq!(migrating.phase, InstancePhase::Migrating);
    assert!(matches!(
        fixture.store.verify_execution(&runtime).await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        fixture
            .store
            .transition_instance(&migrating, InstancePhase::Ready)
            .await,
        Err(Error::InvalidCommand(_))
    ));
    assert!(matches!(
        fixture
            .store
            .report_wake(
                &runtime,
                &Wake {
                    active: true,
                    at: None
                },
                TimeDelta::seconds(1)
            )
            .await,
        Err(Error::Forbidden)
    ));
    let anchor = fixture
        .store
        .initialize_anchor(&runtime, &[0; 32])
        .await
        .unwrap();
    let pending = PendingAnchor {
        operation_id: Uuid::new_v4(),
        generation: runtime.generation,
        next: AnchorHead {
            sequence: 1,
            digest: vec![1; 32],
        },
    };
    fixture
        .store
        .prepare_anchor(&runtime, &anchor.head, &pending)
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .finish_instance(
                &migrating,
                &PreviousWriter::Terminated(runtime.id),
                Some(&pending.next),
                None
            )
            .await,
        Err(Error::FreshnessMismatch)
    ));
    fixture
        .store
        .confirm_anchor(&runtime, &pending, &pending.next)
        .await
        .unwrap();
    assert!(
        fixture
            .store
            .latest_completed_migration(&claim)
            .await
            .unwrap()
            .is_none()
    );
    let wrong = AnchorHead {
        sequence: 1,
        digest: vec![2; 32],
    };
    assert!(matches!(
        fixture
            .store
            .finish_instance(
                &migrating,
                &PreviousWriter::Terminated(runtime.id),
                Some(&wrong),
                None
            )
            .await,
        Err(Error::FreshnessMismatch)
    ));
    let done = fixture
        .store
        .finish_instance(
            &migrating,
            &PreviousWriter::Terminated(runtime.id),
            Some(&pending.next),
            None,
        )
        .await
        .unwrap();
    assert_eq!(done.phase, InstancePhase::Stopped);
    assert_eq!(done.migration_head, Some(pending.next));
    assert!(matches!(
        fixture.store.writer(fixture.agent_id).await,
        Err(Error::StaleGeneration)
    ));
    let completed = fixture
        .store
        .latest_completed_migration(&claim)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.runtime, runtime);
    assert_eq!(completed.plan, migrating.plan);
    assert_eq!(Some(completed.head.clone()), done.migration_head);
    let current = fixture.claim(claim.controller_id).await;
    let next = fixture
        .store
        .advance_fenced_generation(&current)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .store
            .latest_completed_migration(&next)
            .await
            .unwrap(),
        Some(completed.clone())
    );
    assert!(matches!(
        fixture.store.latest_completed_migration(&claim).await,
        Err(Error::StaleGeneration)
    ));
    let mut foreign = next.clone();
    foreign.agent.owner_id = Uuid::new_v4();
    assert!(matches!(
        fixture.store.latest_completed_migration(&foreign).await,
        Err(Error::StaleGeneration)
    ));
    let mut wrong_resources = next.clone();
    wrong_resources
        .workspace_keys
        .push("another-workspace".into());
    assert!(matches!(
        fixture
            .store
            .latest_completed_migration(&wrong_resources)
            .await,
        Err(Error::StaleGeneration)
    ));
    let retry = fixture
        .store
        .begin_instance(&next, &plan(InstancePurpose::Migration))
        .await
        .unwrap();
    let retry = fixture
        .store
        .admit_instance(&retry, &admission(&retry))
        .await
        .unwrap();
    fixture
        .store
        .finish_instance(&retry, &PreviousWriter::Terminated(retry.id), None, None)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .store
            .latest_completed_migration(&next)
            .await
            .unwrap(),
        Some(completed)
    );
}

struct Provider {
    store: Store,
    plans: AtomicUsize,
    allocation_loss: AtomicBool,
    activation_pending: AtomicBool,
    migration_needed: AtomicBool,
    allocations: Mutex<Vec<Uuid>>,
    observations: Mutex<BTreeMap<Uuid, InstanceObservation>>,
}

#[async_trait]
impl InstanceProvider for Provider {
    async fn plan(&self, _: &RuntimeClaim) -> Result<InstancePlan> {
        self.plans.fetch_add(1, Ordering::SeqCst);
        Ok(plan(if self.migration_needed.load(Ordering::SeqCst) {
            InstancePurpose::Migration
        } else {
            InstancePurpose::Execution
        }))
    }
    async fn allocate(&self, intent: &InstanceIntent) -> Result<Admission> {
        self.store.verify_instance_intent(intent).await?;
        assert_eq!(intent.phase, InstancePhase::Allocating);
        self.allocations.lock().unwrap().push(intent.id);
        if self.allocation_loss.swap(false, Ordering::SeqCst) {
            return Err(Error::Uncertain("allocation response lost".into()));
        }
        if let Some(previous) = &intent.claim.previous {
            self.observations
                .lock()
                .unwrap()
                .insert(previous.id, InstanceObservation::Stopped);
        }
        Ok(admission(intent))
    }
    async fn activate(&self, intent: &InstanceIntent) -> Result<PreparationProgress> {
        self.store.verify_instance_intent(intent).await?;
        assert_eq!(
            self.store.writer(intent.claim.agent.id).await?.id,
            intent.id
        );
        assert!(matches!(
            self.store.runtime(intent.claim.agent.id).await,
            Err(Error::RuntimeNotReady)
        ));
        if self.activation_pending.swap(false, Ordering::SeqCst) {
            return Ok(PreparationProgress::Pending);
        }
        let runtime = intent.runtime.as_ref().unwrap();
        if intent.plan.purpose == InstancePurpose::Migration {
            let anchor = self.store.initialize_anchor(runtime, &[0; 32]).await?;
            self.migration_needed.store(false, Ordering::SeqCst);
            self.observations
                .lock()
                .unwrap()
                .insert(intent.id, InstanceObservation::Stopped);
            Ok(PreparationProgress::MigrationComplete(anchor.head))
        } else {
            self.observations
                .lock()
                .unwrap()
                .insert(intent.id, InstanceObservation::Live(Activity::Waiting));
            Ok(PreparationProgress::ExecutionAvailable)
        }
    }
    async fn observe(&self, runtime: &Runtime) -> Result<InstanceObservation> {
        Ok(*self
            .observations
            .lock()
            .unwrap()
            .get(&runtime.id)
            .unwrap_or(&InstanceObservation::Unknown))
    }
    async fn retain_until(&self, runtime: &Runtime, _: DateTime<Utc>) -> Result<()> {
        self.store.verify_writer(runtime).await
    }
    async fn stop(&self, runtime: &Runtime) -> Result<()> {
        self.observations
            .lock()
            .unwrap()
            .insert(runtime.id, InstanceObservation::Stopped);
        Ok(())
    }
}

struct Engine {
    ready: AtomicBool,
    prepared: AtomicUsize,
}

#[async_trait]
impl EngineBridge for Engine {
    async fn prepare(&self, _: &Runtime) -> Result<EnginePreparation> {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        Ok(if self.ready.load(Ordering::SeqCst) {
            EnginePreparation::Ready
        } else {
            EnginePreparation::Pending
        })
    }
    fn requirements(&self, _: &Value) -> Result<CommandRequirements> {
        Err(Error::Forbidden)
    }
    async fn request(&self, _: &Runtime, _: Uuid) -> Result<Option<Value>> {
        Err(Error::Forbidden)
    }
    async fn execute(&self, _: &Dispatch) -> Result<ExecutionResponse> {
        Err(Error::Forbidden)
    }
    async fn wake(&self, _: &Runtime) -> Result<Wake> {
        Err(Error::Forbidden)
    }
    async fn resume(&self, _: &Runtime) -> Result<()> {
        Err(Error::Forbidden)
    }
    async fn snapshot(&self, _: &Runtime, _: &str) -> Result<Value> {
        Err(Error::Forbidden)
    }
}

fn controller(store: &Store, provider: Arc<Provider>, engine: Arc<Engine>) -> Kernel {
    Kernel::new(
        store.clone(),
        provider,
        engine,
        RuntimePolicy {
            lease: TimeDelta::minutes(1),
            delivery_lease: TimeDelta::seconds(10),
            retry_delay: TimeDelta::seconds(1),
            recovery_interval: TimeDelta::seconds(5),
        },
    )
    .unwrap()
}

fn provider(store: &Store, migration: bool) -> Arc<Provider> {
    Arc::new(Provider {
        store: store.clone(),
        plans: AtomicUsize::new(0),
        allocation_loss: AtomicBool::new(false),
        activation_pending: AtomicBool::new(false),
        migration_needed: AtomicBool::new(migration),
        allocations: Mutex::new(vec![]),
        observations: Mutex::new(BTreeMap::new()),
    })
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn coordinator_reconciles_lost_allocation_and_renews_the_same_paused_instance() {
    let fixture = Fixture::new().await;
    let provider = provider(&fixture.store, false);
    provider.allocation_loss.store(true, Ordering::SeqCst);
    provider.activation_pending.store(true, Ordering::SeqCst);
    let engine = Arc::new(Engine {
        ready: AtomicBool::new(false),
        prepared: AtomicUsize::new(0),
    });
    let kernel = controller(&fixture.store, provider.clone(), engine.clone());
    assert!(matches!(
        kernel.ensure_runtime(fixture.agent_id).await,
        Err(Error::Uncertain(_))
    ));
    assert!(matches!(
        kernel.ensure_runtime(fixture.agent_id).await,
        Err(Error::RuntimeNotReady)
    ));
    assert!(matches!(
        kernel.ensure_runtime(fixture.agent_id).await,
        Err(Error::RuntimeNotReady)
    ));
    let runtime = fixture.store.writer(fixture.agent_id).await.unwrap();
    assert_eq!(
        fixture.store.instance_phase(&runtime).await.unwrap().1,
        InstancePhase::Unlocking
    );
    assert!(matches!(
        kernel.ensure_runtime(fixture.agent_id).await,
        Err(Error::RuntimeNotReady)
    ));
    let allocations = provider.allocations.lock().unwrap().clone();
    assert_eq!(allocations, [runtime.id, runtime.id]);
    assert_eq!(provider.plans.load(Ordering::SeqCst), 1);
    engine.ready.store(true, Ordering::SeqCst);
    assert_eq!(
        kernel.ensure_runtime(fixture.agent_id).await.unwrap(),
        runtime
    );
    let restarted = controller(&fixture.store, provider.clone(), engine);
    assert!(matches!(
        restarted.ensure_runtime(fixture.agent_id).await,
        Err(Error::StaleGeneration)
    ));
    sqlx::query(
        "UPDATE gesang_agents SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(fixture.agent_id)
    .execute(fixture.store.pool())
    .await
    .unwrap();
    let replacement = restarted.ensure_runtime(fixture.agent_id).await.unwrap();
    assert_ne!(replacement.id, runtime.id);
    assert!(replacement.generation > runtime.generation);
    assert!(matches!(
        fixture.store.verify_writer(&runtime).await,
        Err(Error::StaleGeneration)
    ));
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn completed_migration_gets_a_new_generation_before_execution_can_resume() {
    let fixture = Fixture::new().await;
    let provider = provider(&fixture.store, true);
    let engine = Arc::new(Engine {
        ready: AtomicBool::new(true),
        prepared: AtomicUsize::new(0),
    });
    let kernel = controller(&fixture.store, provider.clone(), engine.clone());
    assert!(matches!(
        kernel.ensure_runtime(fixture.agent_id).await,
        Err(Error::RuntimeNotReady)
    ));
    assert_eq!(engine.prepared.load(Ordering::SeqCst), 0);
    let migration_id = provider.allocations.lock().unwrap()[0];
    let runtime = kernel.ensure_runtime(fixture.agent_id).await.unwrap();
    assert_ne!(runtime.id, migration_id);
    assert_eq!(provider.plans.load(Ordering::SeqCst), 2);
    assert_eq!(engine.prepared.load(Ordering::SeqCst), 1);
    let migration: (String, Value) =
        sqlx::query_as("SELECT phase,migration_head FROM gesang_instance_intents WHERE id=$1")
            .bind(migration_id)
            .fetch_one(fixture.store.pool())
            .await
            .unwrap();
    assert_eq!(migration.0, "stopped");
    assert_eq!(migration.1["sequence"], 0);
    assert_eq!(
        fixture.store.instance_phase(&runtime).await.unwrap(),
        (InstancePurpose::Execution, InstancePhase::Ready)
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn schema_upgrade_preserves_an_existing_admission_without_reviving_an_expired_writer() {
    let fixture = Fixture::migrated_to(Some(7)).await;
    let controller_id = Uuid::new_v4();
    let expires_at = sqlx::query_scalar("UPDATE gesang_agents SET generation=1,controller_id=$2,lease_until=clock_timestamp()+interval '1 minute' WHERE id=$1 RETURNING lease_until")
        .bind(fixture.agent_id).bind(controller_id).fetch_one(fixture.store.pool()).await.unwrap();
    let claim = RuntimeClaim {
        agent: fixture
            .store
            .agent(&fixture.owner, fixture.agent_id)
            .await
            .unwrap(),
        engine_owner: "local".into(),
        placement: None,
        workspace_keys: vec![],
        controller_id,
        generation: 1,
        expires_at,
        previous: None,
    };
    let runtime = Runtime {
        id: Uuid::new_v4(),
        agent_id: fixture.agent_id,
        generation: claim.generation,
        locator: "existing-service".into(),
        engine_owner: "local".into(),
    };
    sqlx::query("UPDATE gesang_agents SET runtime_id=$2,runtime_generation=$3,runtime_locator=$4 WHERE id=$1")
        .bind(fixture.agent_id).bind(runtime.id).bind(runtime.generation).bind(&runtime.locator)
        .execute(fixture.store.pool()).await.unwrap();
    fixture.store.migrate().await.unwrap();
    fixture.store.migrate().await.unwrap();
    assert_eq!(
        fixture.store.runtime(fixture.agent_id).await.unwrap(),
        runtime
    );
    let intent = fixture
        .store
        .instance_intent(&claim)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(intent.id, runtime.id);
    assert_eq!(intent.phase, InstancePhase::Ready);
    assert_eq!(intent.plan.provider, "existing");
    assert_eq!(intent.plan.purpose, InstancePurpose::Execution);
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM gesang_instance_events WHERE instance_id=$1")
            .bind(runtime.id)
            .fetch_one(fixture.store.pool())
            .await
            .unwrap();
    assert_eq!(events, 1);
    sqlx::query(
        "UPDATE gesang_agents SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(fixture.agent_id)
    .execute(fixture.store.pool())
    .await
    .unwrap();
    assert!(matches!(
        fixture.store.verify_execution(&runtime).await,
        Err(Error::StaleGeneration)
    ));
    let next = fixture.claim(Uuid::new_v4()).await;
    assert_eq!(next.previous, Some(runtime));
    assert!(
        fixture
            .store
            .instance_intent(&next)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn a_dead_unlocking_instance_is_replaced_only_after_independent_termination() {
    let fixture = Fixture::new().await;
    let provider = provider(&fixture.store, false);
    let engine = Arc::new(Engine {
        ready: AtomicBool::new(false),
        prepared: AtomicUsize::new(0),
    });
    let kernel = controller(&fixture.store, provider.clone(), engine.clone());
    assert!(matches!(
        kernel.ensure_runtime(fixture.agent_id).await,
        Err(Error::RuntimeNotReady)
    ));
    let first = fixture.store.writer(fixture.agent_id).await.unwrap();
    provider
        .observations
        .lock()
        .unwrap()
        .insert(first.id, InstanceObservation::Unknown);
    assert!(matches!(
        kernel.ensure_runtime(fixture.agent_id).await,
        Err(Error::Uncertain(_))
    ));
    assert_eq!(provider.plans.load(Ordering::SeqCst), 1);
    assert_eq!(engine.prepared.load(Ordering::SeqCst), 1);
    provider
        .observations
        .lock()
        .unwrap()
        .insert(first.id, InstanceObservation::Stopped);
    assert!(matches!(
        kernel.ensure_runtime(fixture.agent_id).await,
        Err(Error::RuntimeNotReady)
    ));
    assert!(matches!(
        fixture.store.verify_writer(&first).await,
        Err(Error::StaleGeneration)
    ));
    engine.ready.store(true, Ordering::SeqCst);
    let second = kernel.ensure_runtime(fixture.agent_id).await.unwrap();
    assert_ne!(first.id, second.id);
    assert!(second.generation > first.generation);
    assert_eq!(provider.plans.load(Ordering::SeqCst), 2);
}
