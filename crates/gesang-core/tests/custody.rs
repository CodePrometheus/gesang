//! Real custody admission, revocation, isolation, and durable retirement checks.
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "../test-support.rs"]
mod support;
use support::ReadyFixture;

use std::{
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use gesang_core::{
    Admission, AgentBinding, CommandRequirements, Custody, CustodyAction, CustodyEnrollment,
    CustodyKey, Dispatch, EngineBridge, Error, ExecutionResponse, InstanceObservation,
    InstanceProvider, Kernel, Owner, PayloadKey, PreviousWriter, Result, Runtime, RuntimePolicy,
    RuntimeRetirement, ServiceKey, ServiceKeyVerifier, Store, VerifiedIdentity, Wake,
};
use jiff::{SignedDuration, Timestamp};
use serde_json::Value;
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

struct Fixture {
    store: Store,
    custody: Custody,
    owner: Owner,
    agent_id: Uuid,
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
            .max_connections(1)
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
        Self::from_store(store).await
    }

    async fn from_store(store: Store) -> Self {
        let owner = store
            .bind_owner(&VerifiedIdentity {
                issuer: "test".into(),
                subject: "person".into(),
            })
            .await
            .unwrap();
        let agent_id = store
            .ensure_agent(&owner, "execution-v1", "local")
            .await
            .unwrap()
            .id;
        store
            .bind_placement(
                &owner,
                agent_id,
                &[format!("vault:{agent_id}")],
                "test-volume",
            )
            .await
            .unwrap();
        let custody = Custody::new(store.clone(), CustodyKey::new([7; 32]).unwrap());
        Self {
            store,
            custody,
            owner,
            agent_id,
        }
    }

    async fn runtime(&self) -> Runtime {
        let claim = self
            .store
            .claim_runtime(self.agent_id, Uuid::new_v4(), SignedDuration::from_mins(1))
            .await
            .unwrap();
        self.store
            .admit_ready_fixture(
                &claim,
                &Admission {
                    runtime_id: Uuid::new_v4(),
                    locator: "protected-service".into(),
                    engine_owner: "local".into(),
                    previous_writer: claim
                        .previous
                        .as_ref()
                        .map_or(PreviousWriter::Absent, |old| {
                            PreviousWriter::Terminated(old.id)
                        }),
                },
            )
            .await
            .unwrap()
    }
}

struct Verifier(AtomicUsize);

#[async_trait]
impl ServiceKeyVerifier for Verifier {
    async fn verify(&self, binding: &AgentBinding, key: &ServiceKey) -> Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        if binding.engine_owner != "local" || key.expose() != &[31; 32] {
            return Err(Error::InvalidCiphertext);
        }
        Ok(())
    }
}

fn enrollment(expected_revision: i64) -> CustodyEnrollment {
    CustodyEnrollment {
        operation_id: Uuid::new_v4(),
        expected_revision,
        service_key: ServiceKey::new([31; 32]),
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn receipts_are_owner_scoped_read_only_and_preserve_original_results_after_revocation() {
    let fixture = Fixture::new().await;
    let verifier = Verifier(AtomicUsize::new(0));
    let request = enrollment(0);
    assert_eq!(
        fixture
            .custody
            .receipt(&fixture.owner, fixture.agent_id, request.operation_id)
            .await
            .unwrap(),
        None
    );
    let original = fixture
        .custody
        .enroll(&fixture.owner, fixture.agent_id, &request, &verifier)
        .await
        .unwrap();
    let reopened = Custody::new(fixture.store.clone(), CustodyKey::new([7; 32]).unwrap());
    assert_eq!(
        reopened
            .receipt(&fixture.owner, fixture.agent_id, request.operation_id)
            .await
            .unwrap(),
        Some(original.clone())
    );
    let foreign = fixture
        .store
        .bind_owner(&VerifiedIdentity {
            issuer: "test".into(),
            subject: "foreign-person".into(),
        })
        .await
        .unwrap();
    let other_agent = fixture
        .store
        .ensure_agent(&foreign, "execution-v1", "local")
        .await
        .unwrap();
    assert!(matches!(
        reopened
            .receipt(&foreign, fixture.agent_id, request.operation_id)
            .await,
        Err(Error::NotFound)
    ));
    assert_eq!(
        reopened
            .receipt(&foreign, other_agent.id, request.operation_id)
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        reopened
            .receipt(&fixture.owner, Uuid::new_v4(), request.operation_id)
            .await,
        Err(Error::NotFound)
    ));
    let revoke_id = Uuid::new_v4();
    let revoked = reopened
        .revoke(
            &fixture.owner,
            fixture.agent_id,
            revoke_id,
            original.revision,
        )
        .await
        .unwrap();
    let status = reopened
        .status(&fixture.owner, fixture.agent_id)
        .await
        .unwrap();
    assert_eq!(status.active_version, None);
    assert!(status.retirement_pending);
    assert_eq!(
        reopened
            .receipt(&fixture.owner, fixture.agent_id, request.operation_id)
            .await
            .unwrap(),
        Some(original)
    );
    assert_eq!(
        reopened
            .receipt(&fixture.owner, fixture.agent_id, revoke_id)
            .await
            .unwrap(),
        Some(revoked)
    );
    assert_eq!(
        reopened
            .receipt(&fixture.owner, fixture.agent_id, Uuid::new_v4())
            .await
            .unwrap(),
        None
    );
    assert_eq!(verifier.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gesang_custody_operations")
            .fetch_one(fixture.store.pool())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gesang_custody_releases")
            .fetch_one(fixture.store.pool())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        reopened
            .status(&fixture.owner, fixture.agent_id)
            .await
            .unwrap()
            .revision,
        status.revision
    );
    fixture.store.pool().close().await;
    assert!(matches!(
        reopened
            .receipt(&fixture.owner, fixture.agent_id, request.operation_id)
            .await,
        Err(Error::Store(_))
    ));
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn enrollment_is_verified_scoped_encrypted_and_idempotent_after_reopening() {
    let fixture = Fixture::new().await;
    let verifier = Verifier(AtomicUsize::new(0));
    let wrong_key = CustodyEnrollment {
        service_key: ServiceKey::new([1; 32]),
        ..enrollment(0)
    };
    assert!(matches!(
        fixture
            .custody
            .enroll(&fixture.owner, fixture.agent_id, &wrong_key, &verifier)
            .await,
        Err(Error::InvalidCiphertext)
    ));
    assert_eq!(
        fixture
            .custody
            .status(&fixture.owner, fixture.agent_id)
            .await
            .unwrap()
            .revision,
        0
    );
    let request = enrollment(0);
    assert!(matches!(
        fixture
            .custody
            .enroll(
                &Owner { id: Uuid::new_v4() },
                fixture.agent_id,
                &request,
                &verifier
            )
            .await,
        Err(Error::NotFound)
    ));
    assert_eq!(verifier.0.load(Ordering::SeqCst), 1);
    let first = fixture
        .custody
        .enroll(&fixture.owner, fixture.agent_id, &request, &verifier)
        .await
        .unwrap();
    assert_eq!(first.revision, 1);
    assert_eq!(first.action, CustodyAction::Enrolled);
    let reopened = Custody::new(fixture.store.clone(), CustodyKey::new([7; 32]).unwrap());
    assert_eq!(
        reopened
            .enroll(&fixture.owner, fixture.agent_id, &request, &verifier)
            .await
            .unwrap(),
        first
    );
    assert_eq!(verifier.0.load(Ordering::SeqCst), 2);
    let conflict = CustodyEnrollment {
        operation_id: request.operation_id,
        service_key: ServiceKey::new([2; 32]),
        expected_revision: 0,
    };
    assert!(matches!(
        reopened
            .enroll(&fixture.owner, fixture.agent_id, &conflict, &verifier)
            .await,
        Err(Error::RequestConflict)
    ));
    let runtime = fixture.runtime().await;
    assert!(matches!(
        reopened.release(Uuid::new_v4(), &runtime).await,
        Err(Error::Forbidden)
    ));
    let key = reopened.release(fixture.owner.id, &runtime).await.unwrap();
    assert_eq!(key.key.expose(), &[31; 32]);
    assert_eq!(key.revision, 1);
    assert!(!format!("{key:?}").contains("31, 31"));
    let sealed: Vec<u8> = sqlx::query_scalar(
        "SELECT sealed_key FROM gesang_custody_operations WHERE kind='enrolled'",
    )
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert!(!sealed.windows(32).any(|bytes| bytes == [31; 32]));
    let wrong_host = Custody::new(fixture.store.clone(), CustodyKey::new([8; 32]).unwrap());
    assert!(matches!(
        wrong_host.release(fixture.owner.id, &runtime).await,
        Err(Error::InvalidCiphertext)
    ));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM gesang_custody_releases")
        .fetch_one(fixture.store.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[derive(Default)]
struct Provider {
    unavailable: AtomicBool,
    attempts: Mutex<Vec<RuntimeRetirement>>,
}

#[async_trait]
impl InstanceProvider for Provider {
    async fn observe(&self, _: &Runtime) -> Result<InstanceObservation> {
        Ok(InstanceObservation::Unknown)
    }
    async fn retain_until(&self, _: &Runtime, _: Timestamp) -> Result<()> {
        Err(Error::FencingRequired)
    }
    async fn stop(&self, _: &Runtime) -> Result<()> {
        Err(Error::FencingRequired)
    }
    async fn retire(&self, retirement: &RuntimeRetirement) -> Result<()> {
        self.attempts.lock().unwrap().push(retirement.clone());
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(Error::Uncertain("physical observations unavailable".into()));
        }
        Ok(())
    }
}

struct NoExecution;

#[async_trait]
impl EngineBridge for NoExecution {
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

fn kernel(store: &Store, provider: Arc<Provider>) -> Kernel {
    Kernel::new(
        store.clone(),
        provider,
        Arc::new(NoExecution),
        RuntimePolicy {
            lease: SignedDuration::from_mins(1),
            delivery_lease: SignedDuration::from_secs(10),
            retry_delay: SignedDuration::from_secs(1),
            recovery_interval: SignedDuration::from_secs(5),
        },
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn revocation_invalidates_cached_runtime_and_recovers_retirement_before_reenrollment() {
    let fixture = Fixture::new().await;
    let verifier = Verifier(AtomicUsize::new(0));
    let original = enrollment(0);
    fixture
        .custody
        .enroll(&fixture.owner, fixture.agent_id, &original, &verifier)
        .await
        .unwrap();
    let runtime = fixture.runtime().await;
    let operation_id = Uuid::new_v4();
    let receipt = fixture
        .custody
        .revoke(&fixture.owner, fixture.agent_id, operation_id, 1)
        .await
        .unwrap();
    let status = fixture
        .custody
        .status(&fixture.owner, fixture.agent_id)
        .await
        .unwrap();
    assert_eq!(status.revision, 2);
    assert_eq!(status.active_version, None);
    assert!(status.retirement_pending);
    assert!(matches!(
        fixture.custody.release(fixture.owner.id, &runtime).await,
        Err(Error::StaleGeneration)
    ));
    assert!(matches!(
        fixture.store.verify_generation(&runtime).await,
        Err(Error::StaleGeneration)
    ));
    assert!(matches!(
        fixture
            .store
            .claim_runtime(
                fixture.agent_id,
                Uuid::new_v4(),
                SignedDuration::from_mins(1)
            )
            .await,
        Err(Error::Forbidden)
    ));
    assert_eq!(
        fixture
            .custody
            .revoke(&fixture.owner, fixture.agent_id, operation_id, 1)
            .await
            .unwrap(),
        receipt
    );
    let next = enrollment(2);
    assert!(matches!(
        fixture
            .custody
            .enroll(&fixture.owner, fixture.agent_id, &next, &verifier)
            .await,
        Err(Error::FencingRequired)
    ));
    let provider = Arc::new(Provider::default());
    provider.unavailable.store(true, Ordering::SeqCst);
    assert!(matches!(
        kernel(&fixture.store, provider.clone()).retire_one().await,
        Err(Error::Uncertain(_))
    ));
    let obsolete = provider.attempts.lock().unwrap()[0].clone();
    fixture.store.verify_retirement(&obsolete).await.unwrap();
    assert_eq!(obsolete.previous.as_ref(), Some(&runtime));
    assert_eq!(obsolete.generation, runtime.generation);
    assert!(
        fixture
            .custody
            .status(&fixture.owner, fixture.agent_id)
            .await
            .unwrap()
            .retirement_pending
    );
    sqlx::query("UPDATE gesang_runtime_retirements SET claimed_until=clock_timestamp()-interval '1 second' WHERE id=$1").bind(obsolete.id).execute(fixture.store.pool()).await.unwrap();
    provider.unavailable.store(false, Ordering::SeqCst);
    assert!(
        kernel(&fixture.store, provider.clone())
            .retire_one()
            .await
            .unwrap()
    );
    assert!(matches!(
        fixture.store.verify_retirement(&obsolete).await,
        Err(Error::StaleGeneration)
    ));
    assert!(
        !kernel(&fixture.store, provider.clone())
            .retire_one()
            .await
            .unwrap()
    );
    assert!(
        !fixture
            .custody
            .status(&fixture.owner, fixture.agent_id)
            .await
            .unwrap()
            .retirement_pending
    );
    let reenrolled = fixture
        .custody
        .enroll(&fixture.owner, fixture.agent_id, &next, &verifier)
        .await
        .unwrap();
    assert_eq!(reenrolled.revision, 3);
    assert_eq!(
        fixture
            .custody
            .revoke(&fixture.owner, fixture.agent_id, operation_id, 1)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        fixture
            .custody
            .enroll(&fixture.owner, fixture.agent_id, &original, &verifier)
            .await
            .unwrap()
            .revision,
        1
    );
    assert_eq!(
        fixture
            .custody
            .status(&fixture.owner, fixture.agent_id)
            .await
            .unwrap()
            .active_version,
        Some(3)
    );
    let replacement = fixture.runtime().await;
    assert!(replacement.generation > runtime.generation);
    assert!(matches!(
        fixture.custody.release(fixture.owner.id, &runtime).await,
        Err(Error::StaleGeneration)
    ));
    assert_eq!(
        fixture
            .custody
            .release(fixture.owner.id, &replacement)
            .await
            .unwrap()
            .revision,
        3
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn concurrent_enrollment_has_one_winner_and_unadmitted_launches_require_retirement() {
    assert_concurrent_enrollment_and_retirement(Fixture::new().await).await;
}

#[tokio::test]
async fn sqlite_concurrent_enrollment_and_retirement() {
    let path = std::env::temp_dir().join(format!("gesang-custody-{}.sqlite", Uuid::new_v4()));
    let store = Store::open_sqlite(&path, PayloadKey::new([42; 32]))
        .await
        .unwrap();
    assert_concurrent_enrollment_and_retirement(Fixture::from_store(store).await).await;
}

async fn assert_concurrent_enrollment_and_retirement(fixture: Fixture) {
    let verifier = Verifier(AtomicUsize::new(0));
    let a = enrollment(0);
    let b = enrollment(0);
    let (a, b) = tokio::join!(
        fixture
            .custody
            .enroll(&fixture.owner, fixture.agent_id, &a, &verifier),
        fixture
            .custody
            .enroll(&fixture.owner, fixture.agent_id, &b, &verifier)
    );
    assert!(matches!(
        (a, b),
        (Ok(_), Err(Error::VersionConflict)) | (Err(Error::VersionConflict), Ok(_))
    ));
    let inflight = fixture
        .store
        .claim_runtime(
            fixture.agent_id,
            Uuid::new_v4(),
            SignedDuration::from_mins(1),
        )
        .await
        .unwrap();
    let mut changed_binding = inflight.clone();
    changed_binding.workspace_keys.push("another-vault".into());
    assert!(matches!(
        fixture
            .store
            .advance_fenced_generation(&changed_binding)
            .await,
        Err(Error::StaleGeneration)
    ));
    fixture.store.verify_runtime_claim(&inflight).await.unwrap();
    let stopped_launch = fixture
        .store
        .advance_fenced_generation(&inflight)
        .await
        .unwrap();
    assert_eq!(stopped_launch.generation, inflight.generation + 1);
    assert!(matches!(
        fixture.store.advance_fenced_generation(&inflight).await,
        Err(Error::StaleGeneration)
    ));
    fixture
        .custody
        .revoke(&fixture.owner, fixture.agent_id, Uuid::new_v4(), 1)
        .await
        .unwrap();
    assert!(matches!(
        fixture.store.verify_runtime_claim(&inflight).await,
        Err(Error::StaleGeneration)
    ));
    let provider = Arc::new(Provider::default());
    kernel(&fixture.store, provider.clone())
        .retire_one()
        .await
        .unwrap();
    let attempts = provider.attempts.lock().unwrap();
    assert_eq!(attempts.len(), 1);
    assert!(attempts[0].previous.is_none());
    assert_eq!(attempts[0].generation, stopped_launch.generation);
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn wrapped_service_keys_cannot_be_substituted_between_owners_or_vaults() {
    let fixture = Fixture::new().await;
    let verifier = Verifier(AtomicUsize::new(0));
    fixture
        .custody
        .enroll(&fixture.owner, fixture.agent_id, &enrollment(0), &verifier)
        .await
        .unwrap();
    let owner = fixture
        .store
        .bind_owner(&VerifiedIdentity {
            issuer: "test".into(),
            subject: "another-person".into(),
        })
        .await
        .unwrap();
    let agent = fixture
        .store
        .ensure_agent(&owner, "execution-v1", "local")
        .await
        .unwrap();
    fixture
        .store
        .bind_placement(
            &owner,
            agent.id,
            &[format!("vault:{}", agent.id)],
            "other-test-volume",
        )
        .await
        .unwrap();
    fixture
        .custody
        .enroll(&owner, agent.id, &enrollment(0), &verifier)
        .await
        .unwrap();
    let claim = fixture
        .store
        .claim_runtime(agent.id, Uuid::new_v4(), SignedDuration::from_mins(1))
        .await
        .unwrap();
    let runtime = fixture
        .store
        .admit_ready_fixture(
            &claim,
            &Admission {
                runtime_id: Uuid::new_v4(),
                locator: "another-protected-service".into(),
                engine_owner: "local".into(),
                previous_writer: PreviousWriter::Absent,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .custody
            .release(owner.id, &runtime)
            .await
            .unwrap()
            .key
            .expose(),
        &[31; 32]
    );
    sqlx::query("UPDATE gesang_custody_operations SET sealed_key=(SELECT sealed_key FROM gesang_custody_operations WHERE agent_id=$1 AND revision=1) WHERE agent_id=$2 AND revision=1")
        .bind(fixture.agent_id).bind(agent.id).execute(fixture.store.pool()).await.unwrap();
    assert!(matches!(
        fixture.custody.release(owner.id, &runtime).await,
        Err(Error::InvalidCiphertext)
    ));
}
