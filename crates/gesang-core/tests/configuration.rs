//! Real configuration publication, custody, and adoption checks.
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "configuration/host-coordination.rs"]
mod host_coordination;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use gesang_core::{
    Activity, Admission, CommandRequirements, Dispatch, EngineBridge, ExecutionResponse,
    InstanceObservation, InstanceProvider, Kernel, PreviousWriter, Runtime, RuntimeClaim,
    RuntimePolicy, Wake,
};
use std::sync::{Arc, Mutex};
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use gesang_core::{
    AdoptionBoundary, Configuration, ConfigurationAccess, ConfigurationCenter, ConfigurationChange,
    ConfigurationKey, ConfigurationScope, Error, PayloadKey, Result, SecretInput, Store,
    VerifiedIdentity,
};
use serde::{Deserialize, Serialize};
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;
use zeroize::Zeroizing;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Settings {
    slots: u32,
}

impl Configuration for Settings {
    const COMPONENT: &'static str = "test.execution";
    const SCHEMA_VERSION: u32 = 1;
    const ADOPTION_BOUNDARY: AdoptionBoundary = AdoptionBoundary::Checkpoint;
    fn validate(&self, _: &ConfigurationScope, secrets: &BTreeSet<String>) -> Result<()> {
        if self.slots == 0
            || !secrets.contains("api")
            || secrets
                .iter()
                .any(|name| !matches!(name.as_str(), "api" | "backup"))
        {
            return Err(Error::InvalidCommand("invalid settings".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
struct LiveSettings(Settings);

impl Configuration for LiveSettings {
    const COMPONENT: &'static str = "test.admission";
    const SCHEMA_VERSION: u32 = 1;
    const ADOPTION_BOUNDARY: AdoptionBoundary = AdoptionBoundary::Live;
    fn validate(&self, scope: &ConfigurationScope, secrets: &BTreeSet<String>) -> Result<()> {
        self.0.validate(scope, secrets)
    }
}

struct Fixture {
    store: Store,
    center: ConfigurationCenter,
    access: ConfigurationAccess,
    scope: ConfigurationScope,
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
        let center =
            ConfigurationCenter::new(store.clone(), ConfigurationKey::new([7; 32]).unwrap());
        Self {
            store,
            center,
            scope: ConfigurationScope::Owner(owner.id),
            access: ConfigurationAccess::Owner(owner),
        }
    }

    async fn counts(&self) -> (i64, i64, i64) {
        let revisions = sqlx::query_scalar("SELECT count(*) FROM gesang_configuration_revisions")
            .fetch_one(self.store.pool())
            .await
            .unwrap();
        let secrets = sqlx::query_scalar("SELECT count(*) FROM gesang_configuration_secrets")
            .fetch_one(self.store.pool())
            .await
            .unwrap();
        let events = sqlx::query_scalar("SELECT count(*) FROM gesang_configuration_events")
            .fetch_one(self.store.pool())
            .await
            .unwrap();
        (revisions, secrets, events)
    }
}

fn change(
    slots: u32,
    expected_revision: Option<i64>,
    secret: &str,
) -> ConfigurationChange<Settings> {
    ConfigurationChange {
        operation_id: Uuid::new_v4(),
        expected_revision,
        values: Settings { slots },
        secrets: BTreeMap::from([(
            "api".into(),
            SecretInput::Set(Zeroizing::new(secret.as_bytes().to_vec())),
        )]),
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn immutable_preparation_atomic_publication_and_rollback_survive_reopening() {
    let fixture = Fixture::new().await;
    let (access, scope) = (&fixture.access, &fixture.scope);
    let first = change(2, None, "private-api-key");
    let prepared = fixture.center.prepare(access, scope, &first).await.unwrap();
    assert_eq!(prepared.revision, 1);
    assert!(prepared.published_at.is_none());
    assert!(matches!(
        fixture.center.current::<Settings>(access, scope).await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        fixture
            .center
            .resolve_secret::<Settings>(access, scope, 1, "api")
            .await,
        Err(Error::NotFound)
    ));
    let reopened = ConfigurationCenter::new(
        fixture.store.clone(),
        ConfigurationKey::new([7; 32]).unwrap(),
    );
    assert_eq!(
        reopened
            .prepare(access, scope, &first)
            .await
            .unwrap()
            .secrets,
        prepared.secrets
    );
    let mut altered = change(2, None, "different-key");
    altered.operation_id = first.operation_id;
    assert!(matches!(
        reopened.prepare(access, scope, &altered).await,
        Err(Error::RequestConflict)
    ));
    assert_eq!(fixture.counts().await, (1, 1, 1));
    let sealed: Vec<u8> =
        sqlx::query_scalar("SELECT sealed_value FROM gesang_configuration_secrets")
            .fetch_one(fixture.store.pool())
            .await
            .unwrap();
    assert!(
        !sealed
            .windows("private-api-key".len())
            .any(|window| window == b"private-api-key")
    );
    let initial = reopened
        .publish::<Settings>(access, scope, first.operation_id)
        .await
        .unwrap();
    assert!(initial.published_at.is_some());
    assert_eq!(
        &*reopened
            .resolve_secret::<Settings>(access, scope, 1, "api")
            .await
            .unwrap(),
        b"private-api-key"
    );
    let consumer = Uuid::new_v4();
    reopened
        .adopt::<Settings>(access, scope, consumer, 1, None)
        .await
        .unwrap();
    let second = change(4, Some(1), "next-key");
    let third = change(8, Some(1), "another-key");
    let (a, b) = tokio::join!(
        reopened.prepare(access, scope, &second),
        reopened.prepare(access, scope, &third)
    );
    assert_ne!(a.unwrap().revision, b.unwrap().revision);
    let (a, b) = tokio::join!(
        reopened.publish::<Settings>(access, scope, second.operation_id),
        reopened.publish::<Settings>(access, scope, third.operation_id)
    );
    let winner = match (a, b) {
        (Ok(winner), Err(Error::VersionConflict)) | (Err(Error::VersionConflict), Ok(winner)) => {
            winner
        }
        result => panic!("expected exactly one publication: {result:?}"),
    };
    assert_eq!(
        reopened
            .current::<Settings>(access, scope)
            .await
            .unwrap()
            .revision,
        winner.revision
    );
    assert_eq!(
        reopened
            .publish::<Settings>(access, scope, first.operation_id)
            .await
            .unwrap()
            .revision,
        1
    );
    assert_eq!(
        reopened
            .adopted::<Settings>(access, scope, consumer)
            .await
            .unwrap()
            .revision,
        1
    );
    let rollback_id = Uuid::new_v4();
    let rollback = reopened
        .prepare_rollback::<Settings>(access, scope, rollback_id, winner.revision, 1)
        .await
        .unwrap();
    assert!(rollback.revision > winner.revision);
    assert_eq!(rollback.rollback_of, Some(1));
    assert_eq!(rollback.values, first.values);
    assert_eq!(rollback.secrets, initial.secrets);
    reopened
        .publish::<Settings>(access, scope, rollback_id)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .current::<Settings>(access, scope)
            .await
            .unwrap()
            .revision,
        rollback.revision
    );
    reopened
        .adopt::<Settings>(access, scope, consumer, rollback.revision, Some(1))
        .await
        .unwrap();
    assert!(matches!(
        reopened
            .adopt::<Settings>(access, scope, consumer, 1, Some(1))
            .await,
        Err(Error::VersionConflict)
    ));
    assert_eq!(
        reopened
            .history::<Settings>(access, scope, 1)
            .await
            .unwrap()
            .values
            .slots,
        2
    );
    assert_eq!(fixture.counts().await, (4, 3, 9));
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn secret_staging_is_atomic_owner_scoped_and_revocation_invalidates_pins() {
    let fixture = Fixture::new().await;
    let (access, scope) = (&fixture.access, &fixture.scope);
    let first = change(2, None, "owner-a-secret");
    let prepared = fixture.center.prepare(access, scope, &first).await.unwrap();
    fixture
        .center
        .publish::<Settings>(access, scope, first.operation_id)
        .await
        .unwrap();
    let consumer = Uuid::new_v4();
    fixture
        .center
        .adopt::<Settings>(access, scope, consumer, 1, None)
        .await
        .unwrap();
    let other = fixture
        .store
        .bind_owner(&VerifiedIdentity {
            issuer: "other".into(),
            subject: "person".into(),
        })
        .await
        .unwrap();
    let other_scope = ConfigurationScope::Owner(other.id);
    let other_access = ConfigurationAccess::Owner(other);
    assert!(matches!(
        fixture
            .center
            .current::<Settings>(&other_access, scope)
            .await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        fixture
            .center
            .prepare(access, &ConfigurationScope::Deployment, &first)
            .await,
        Err(Error::Forbidden)
    ));
    let mut other_change = change(1, None, "owner-b-api");
    other_change.secrets.insert(
        "backup".into(),
        SecretInput::Set(Zeroizing::new(b"owner-b-backup".to_vec())),
    );
    let other_revision = fixture
        .center
        .prepare(&other_access, &other_scope, &other_change)
        .await
        .unwrap();
    let before = fixture.counts().await;
    let mut invalid = change(3, Some(1), "staged-but-not-committed");
    invalid.secrets.insert(
        "backup".into(),
        SecretInput::Keep(other_revision.secrets["backup"]),
    );
    assert!(matches!(
        fixture.center.prepare(access, scope, &invalid).await,
        Err(Error::Forbidden)
    ));
    assert_eq!(fixture.counts().await, before);
    let next = ConfigurationChange {
        operation_id: Uuid::new_v4(),
        expected_revision: Some(1),
        values: Settings { slots: 5 },
        secrets: BTreeMap::from([("api".into(), SecretInput::Keep(prepared.secrets["api"]))]),
    };
    fixture.center.prepare(access, scope, &next).await.unwrap();
    fixture
        .center
        .revoke_secret::<Settings>(access, scope, "api", prepared.secrets["api"])
        .await
        .unwrap();
    let revoked = fixture.counts().await;
    fixture
        .center
        .revoke_secret::<Settings>(access, scope, "api", prepared.secrets["api"])
        .await
        .unwrap();
    assert_eq!(fixture.counts().await, revoked);
    assert!(matches!(
        fixture
            .center
            .publish::<Settings>(access, scope, next.operation_id)
            .await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        fixture
            .center
            .adopted::<Settings>(access, scope, consumer)
            .await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        fixture
            .center
            .resolve_secret::<Settings>(access, scope, 1, "api")
            .await,
        Err(Error::Forbidden)
    ));
    assert_eq!(
        fixture
            .center
            .current::<Settings>(access, scope)
            .await
            .unwrap()
            .revision,
        1
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn live_policy_rejects_superseded_adoptions_and_ciphertext_substitution() {
    let fixture = Fixture::new().await;
    let (access, scope) = (&fixture.access, &fixture.scope);
    let first = change(2, None, "first-secret");
    let first = ConfigurationChange {
        operation_id: first.operation_id,
        expected_revision: first.expected_revision,
        values: LiveSettings(first.values),
        secrets: first.secrets,
    };
    let initial = fixture.center.prepare(access, scope, &first).await.unwrap();
    fixture
        .center
        .publish::<LiveSettings>(access, scope, first.operation_id)
        .await
        .unwrap();
    let consumer = Uuid::new_v4();
    fixture
        .center
        .adopt::<LiveSettings>(access, scope, consumer, 1, None)
        .await
        .unwrap();
    let second = change(3, Some(1), "second-secret");
    let second = ConfigurationChange {
        operation_id: second.operation_id,
        expected_revision: second.expected_revision,
        values: LiveSettings(second.values),
        secrets: second.secrets,
    };
    let next = fixture
        .center
        .prepare(access, scope, &second)
        .await
        .unwrap();
    fixture
        .center
        .publish::<LiveSettings>(access, scope, second.operation_id)
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .center
            .adopted::<LiveSettings>(access, scope, consumer)
            .await,
        Err(Error::VersionConflict)
    ));
    assert!(matches!(
        fixture
            .center
            .resolve_secret::<LiveSettings>(access, scope, 1, "api")
            .await,
        Err(Error::VersionConflict)
    ));
    assert!(matches!(
        fixture
            .center
            .adopt::<LiveSettings>(access, scope, consumer, next.revision, None)
            .await,
        Err(Error::VersionConflict)
    ));
    fixture
        .center
        .adopt::<LiveSettings>(access, scope, consumer, next.revision, Some(1))
        .await
        .unwrap();
    sqlx::query("UPDATE gesang_configuration_secrets SET sealed_value=(SELECT sealed_value FROM gesang_configuration_secrets WHERE id=$1) WHERE id=$2")
        .bind(initial.secrets["api"].id).bind(next.secrets["api"].id).execute(fixture.store.pool()).await.unwrap();
    assert!(matches!(
        fixture
            .center
            .resolve_secret::<LiveSettings>(access, scope, next.revision, "api")
            .await,
        Err(Error::InvalidCiphertext)
    ));
}

#[derive(Default)]
struct Provider {
    retained: Mutex<Vec<DateTime<Utc>>>,
}

#[async_trait]
impl InstanceProvider for Provider {
    async fn plan(&self, _: &RuntimeClaim) -> Result<gesang_core::InstancePlan> {
        Ok(gesang_core::InstancePlan {
            purpose: gesang_core::InstancePurpose::Execution,
            provider: "policy-fixture".into(),
            schema_version: 1,
            definition: serde_json::json!({}),
        })
    }
    async fn allocate(&self, intent: &gesang_core::InstanceIntent) -> Result<Admission> {
        Ok(Admission {
            runtime_id: intent.id,
            locator: "policy-fixture".into(),
            engine_owner: "local".into(),
            previous_writer: PreviousWriter::Absent,
        })
    }
    async fn activate(
        &self,
        _: &gesang_core::InstanceIntent,
    ) -> Result<gesang_core::PreparationProgress> {
        Ok(gesang_core::PreparationProgress::ExecutionAvailable)
    }
    async fn observe(&self, _: &Runtime) -> Result<InstanceObservation> {
        Ok(InstanceObservation::Live(Activity::Waiting))
    }
    async fn retain_until(&self, _: &Runtime, until: DateTime<Utc>) -> Result<()> {
        self.retained.lock().unwrap().push(until);
        Ok(())
    }
    async fn stop(&self, _: &Runtime) -> Result<()> {
        Ok(())
    }
}

struct NoExecution;

#[async_trait]
impl EngineBridge for NoExecution {
    async fn prepare(&self, _: &Runtime) -> Result<gesang_core::EnginePreparation> {
        Ok(gesang_core::EnginePreparation::Ready)
    }
    fn requirements(&self, _: &serde_json::Value) -> Result<CommandRequirements> {
        Err(Error::Forbidden)
    }
    async fn request(&self, _: &Runtime, _: Uuid) -> Result<Option<serde_json::Value>> {
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
    async fn snapshot(&self, _: &Runtime, _: &str) -> Result<serde_json::Value> {
        Err(Error::Forbidden)
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn kernel_adopts_new_runtime_policy_without_reconstruction_or_duplicate_adoption_events() {
    assert_kernel_adopts_new_runtime_policy(Fixture::new().await).await;
}

#[tokio::test]
async fn sqlite_kernel_adopts_new_runtime_policy() {
    let path = std::env::temp_dir().join(format!("gesang-configuration-{}.sqlite", Uuid::new_v4()));
    let store = Store::open_sqlite(&path, PayloadKey::new([42; 32]))
        .await
        .unwrap();
    assert_kernel_adopts_new_runtime_policy(Fixture::from_store(store).await).await;
}

async fn adopted_event_count(store: &Store) -> i64 {
    const COUNT: &str = "SELECT count(*) FROM gesang_configuration_events WHERE kind='adopted'";
    if let Some(pool) = store.sqlite_pool() {
        sqlx::query_scalar(COUNT).fetch_one(pool).await.unwrap()
    } else {
        sqlx::query_scalar(COUNT)
            .fetch_one(store.pool())
            .await
            .unwrap()
    }
}

async fn assert_kernel_adopts_new_runtime_policy(fixture: Fixture) {
    let access = ConfigurationAccess::Administrator(VerifiedIdentity {
        issuer: "trusted-administrator".into(),
        subject: "operator".into(),
    });
    let scope = ConfigurationScope::Deployment;
    let policy = RuntimePolicy {
        lease: TimeDelta::seconds(30),
        delivery_lease: TimeDelta::seconds(10),
        retry_delay: TimeDelta::seconds(1),
        recovery_interval: TimeDelta::seconds(5),
    };
    let mut first = ConfigurationChange {
        operation_id: Uuid::new_v4(),
        expected_revision: None,
        values: policy.clone(),
        secrets: BTreeMap::new(),
    };
    fixture
        .center
        .prepare(&access, &scope, &first)
        .await
        .unwrap();
    fixture
        .center
        .publish::<RuntimePolicy>(&access, &scope, first.operation_id)
        .await
        .unwrap();
    let provider = Arc::new(Provider::default());
    let host = Arc::new(host_coordination::HostCoordination {
        store: fixture.store.clone(),
        verified: Mutex::new(None),
    });
    let storage: Arc<dyn gesang_core::CoordinationStore> = host.clone();
    let kernel = Kernel::with_coordination_store_and_configuration(
        storage,
        provider.clone(),
        Arc::new(NoExecution),
        fixture.center.clone(),
        access.clone(),
    )
    .await
    .unwrap();
    let ConfigurationAccess::Owner(owner) = &fixture.access else {
        panic!("owner fixture required");
    };
    let agent = fixture
        .store
        .ensure_agent(owner, "execution-v1", "local")
        .await
        .unwrap();
    let runtime = kernel.ensure_runtime(agent.id).await.unwrap();
    assert_eq!(host.verified.lock().unwrap().as_ref(), Some(&runtime));
    let before = provider.retained.lock().unwrap()[0];
    first.operation_id = Uuid::new_v4();
    first.expected_revision = Some(1);
    first.values.lease = TimeDelta::seconds(90);
    fixture
        .center
        .prepare(&access, &scope, &first)
        .await
        .unwrap();
    fixture
        .center
        .publish::<RuntimePolicy>(&access, &scope, first.operation_id)
        .await
        .unwrap();
    assert_eq!(kernel.ensure_runtime(agent.id).await.unwrap(), runtime);
    assert!(*provider.retained.lock().unwrap().last().unwrap() > before);
    const ADOPTIONS: &str = "SELECT a.revision FROM gesang_configuration_adoptions a
        JOIN gesang_configuration_streams s ON s.id=a.stream_id WHERE s.component=$1";
    let adoptions: Vec<i64> = if let Some(pool) = fixture.store.sqlite_pool() {
        sqlx::query_scalar(ADOPTIONS)
            .bind(RuntimePolicy::COMPONENT)
            .fetch_all(pool)
            .await
            .unwrap()
    } else {
        sqlx::query_scalar(ADOPTIONS)
            .bind(RuntimePolicy::COMPONENT)
            .fetch_all(fixture.store.pool())
            .await
            .unwrap()
    };
    assert_eq!(adoptions, vec![2]);
    assert_eq!(adopted_event_count(&fixture.store).await, 2);
    let consumer = Uuid::new_v4();
    let (a, b) = tokio::join!(
        fixture
            .center
            .current_for_consumer::<RuntimePolicy>(&access, &scope, consumer),
        fixture
            .center
            .current_for_consumer::<RuntimePolicy>(&access, &scope, consumer)
    );
    assert_eq!(a.unwrap().revision, 2);
    assert_eq!(b.unwrap().revision, 2);
    assert_eq!(
        fixture
            .center
            .adoption_revision::<RuntimePolicy>(&access, &scope, consumer)
            .await
            .unwrap(),
        Some(2)
    );
    assert_eq!(adopted_event_count(&fixture.store).await, 3);
    let mut invalid = first;
    invalid.operation_id = Uuid::new_v4();
    invalid.expected_revision = Some(2);
    invalid.values.recovery_interval = invalid.values.lease;
    assert!(matches!(
        fixture.center.prepare(&access, &scope, &invalid).await,
        Err(Error::InvalidCommand(_))
    ));
    assert_eq!(
        fixture
            .center
            .current::<RuntimePolicy>(&access, &scope)
            .await
            .unwrap()
            .values
            .lease,
        TimeDelta::seconds(90)
    );
}
