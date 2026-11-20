//! Real initialization transactions and HTTP; provider identities and storage heads are explicit fixtures.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    num::NonZeroUsize,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use gesang_api::{CapabilityScope, GrantPurpose, GrantSelection, PermissionMode};
use gesang_core::{
    Admission, AgentBinding, AnchorHead, Custody, CustodyKey, Error, Initialization,
    InitializationRequest, InitializationState, InstanceIntent, InstancePhase, InstancePlan,
    InstancePurpose, Owner, PayloadKey, PreviousWriter, Result, Runtime, Store, VerifiedIdentity,
};
use gesang_host::{ControlKey, ControlPlane, ControlPolicy, ExecutionAction};
use jiff::SignedDuration;
use serde_json::{Value, json};
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tower::ServiceExt as _;
use uuid::Uuid;

#[derive(Default)]
struct Policy {
    denied: AtomicBool,
}

#[async_trait]
impl ControlPolicy for Policy {
    async fn verify_initialization(
        &self,
        _: &AgentBinding,
        _: &Runtime,
        initialization: &Initialization,
    ) -> Result<()> {
        if self.denied.load(Ordering::SeqCst) || initialization.plan_digest != [19; 32] {
            return Err(Error::FencingRequired);
        }
        Ok(())
    }

    async fn requirements(
        &self,
        _: &AgentBinding,
        _: &ExecutionAction,
    ) -> Result<Vec<CapabilityScope>> {
        Ok(vec![])
    }
}

struct Fixture {
    store: Store,
    custody: Arc<Custody>,
    owner: Owner,
    agent_id: Uuid,
    request: InitializationRequest,
    control: Arc<ControlKey>,
    policy: Arc<Policy>,
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
        let owner = store
            .bind_owner(&VerifiedIdentity {
                issuer: "initialization".into(),
                subject: "owner".into(),
            })
            .await
            .unwrap();
        let agent_id = store
            .ensure_agent(&owner, "engine", "local")
            .await
            .unwrap()
            .id;
        let custody = Arc::new(Custody::new(
            store.clone(),
            CustodyKey::new([23; 32]).unwrap(),
        ));
        Self {
            store,
            custody,
            owner,
            agent_id,
            request: InitializationRequest {
                operation_id: Uuid::new_v4(),
                plan_digest: [19; 32],
            },
            control: Arc::new(ControlKey::new([17; 32])),
            policy: Arc::new(Policy::default()),
        }
    }

    async fn bind(&self) {
        self.custody
            .bind_initialization_placement(
                &self.owner,
                self.agent_id,
                &self.request,
                &[format!("fixture:{}", self.agent_id)],
                "fresh-fixture",
            )
            .await
            .unwrap();
    }

    async fn allocate(&self, purpose: InstancePurpose) -> InstanceIntent {
        let claim = self
            .store
            .claim_runtime(self.agent_id, Uuid::new_v4(), SignedDuration::from_mins(1))
            .await
            .unwrap();
        let intent = self
            .store
            .begin_instance(
                &claim,
                &InstancePlan {
                    purpose,
                    provider: "fixture".into(),
                    schema_version: 1,
                    definition: json!({}),
                },
            )
            .await
            .unwrap();
        self.store
            .admit_instance(
                &intent,
                &Admission {
                    runtime_id: intent.id,
                    locator: format!("fixture:{}", intent.id),
                    engine_owner: claim.engine_owner.clone(),
                    previous_writer: claim
                        .previous
                        .map_or(PreviousWriter::Absent, |r| PreviousWriter::Terminated(r.id)),
                },
            )
            .await
            .unwrap()
    }

    fn router(&self) -> Router {
        ControlPlane::new(
            self.store.clone(),
            self.control.clone(),
            self.policy.clone(),
            SignedDuration::from_millis(7),
        )
        .unwrap()
        .with_initialization_custody(self.custody.clone())
        .with_migration_custody(self.custody.clone())
        .router(NonZeroUsize::new(4096).unwrap())
    }

    async fn post(
        &self,
        runtime: &Runtime,
        operation_id: Uuid,
        endpoint: &str,
        head: Option<&AnchorHead>,
    ) -> (StatusCode, Value) {
        let mut body = json!({"agentId":self.agent_id,"epoch":runtime.generation,"owner":runtime.engine_owner,"operationId":operation_id});
        if let Some(head) = head {
            body["head"] = json!({"sequence":head.sequence,"digest":BASE64_URL_SAFE_NO_PAD.encode(&head.digest)});
        }
        let response = self
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/internal/v1/initialization/{endpoint}"))
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", self.control.issue(runtime).unwrap().expose()),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let status = response.status();
        let body =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        (status, body)
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn first_store_key_is_durable_scoped_and_never_replaces_an_existing_binding() {
    let f = Fixture::new().await;
    let original = f
        .custody
        .begin_initialization(&f.owner, f.agent_id, &f.request)
        .await
        .unwrap();
    assert_eq!(original.state, InitializationState::Pending);
    let reopened = Custody::new(f.store.clone(), CustodyKey::new([23; 32]).unwrap());
    assert_eq!(
        reopened
            .begin_initialization(&f.owner, f.agent_id, &f.request)
            .await
            .unwrap(),
        original
    );
    for changed in [
        InitializationRequest {
            operation_id: Uuid::new_v4(),
            ..f.request.clone()
        },
        InitializationRequest {
            plan_digest: [20; 32],
            ..f.request.clone()
        },
    ] {
        assert!(matches!(
            reopened
                .begin_initialization(&f.owner, f.agent_id, &changed)
                .await,
            Err(Error::RequestConflict)
        ));
    }
    let foreign = f
        .store
        .bind_owner(&VerifiedIdentity {
            issuer: "initialization".into(),
            subject: "foreign".into(),
        })
        .await
        .unwrap();
    assert!(matches!(
        reopened.initialization(&foreign, f.agent_id).await,
        Err(Error::NotFound)
    ));
    let keys = [format!("fixture:{}", f.agent_id)];
    assert!(matches!(
        f.store
            .bind_placement(&f.owner, f.agent_id, &keys, "fresh-fixture")
            .await,
        Err(Error::Forbidden)
    ));
    let mut changed = f.request.clone();
    changed.plan_digest[0] ^= 1;
    assert!(matches!(
        f.custody
            .bind_initialization_placement(&f.owner, f.agent_id, &changed, &keys, "fresh-fixture")
            .await,
        Err(Error::RequestConflict)
    ));
    f.bind().await;
    f.bind().await;
    let intent = f.allocate(InstancePurpose::Initialization).await;
    assert_eq!(intent.phase, InstancePhase::Initializing);
    assert!(
        f.store
            .transition_instance(&intent, InstancePhase::Ready)
            .await
            .is_err()
    );
    let runtime = intent.runtime.as_ref().unwrap();
    let key = reopened
        .release_initialization_key(&f.owner, runtime, f.request.operation_id)
        .await
        .unwrap();
    assert_eq!(
        key.key.expose(),
        f.custody
            .release_initialization_key(&f.owner, runtime, f.request.operation_id)
            .await
            .unwrap()
            .key
            .expose()
    );
    let sealed: Vec<u8> =
        sqlx::query_scalar("SELECT sealed_key FROM gesang_initializations WHERE agent_id=$1")
            .bind(f.agent_id)
            .fetch_one(f.store.pool())
            .await
            .unwrap();
    assert!(!sealed.windows(32).any(|part| part == key.key.expose()));
    assert!(f.store.runtime(f.agent_id).await.is_err());
    assert_eq!(
        f.custody
            .status(&f.owner, f.agent_id)
            .await
            .unwrap()
            .active_version,
        None
    );
    assert!(
        f.store
            .bind_placement(
                &f.owner,
                f.agent_id,
                &["replacement".into()],
                "empty-replacement"
            )
            .await
            .is_err()
    );
    let g = Fixture::new().await;
    g.store
        .bind_placement(
            &g.owner,
            g.agent_id,
            &["existing-fixture".into()],
            "existing",
        )
        .await
        .unwrap();
    assert!(matches!(
        g.custody
            .begin_initialization(&g.owner, g.agent_id, &g.request)
            .await,
        Err(Error::Forbidden)
    ));
    g.store.pool().close().await;
    f.store.pool().close().await;
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn revoked_initialization_cannot_bind_resources() {
    let f = Fixture::new().await;
    f.custody
        .begin_initialization(&f.owner, f.agent_id, &f.request)
        .await
        .unwrap();
    f.custody
        .revoke(&f.owner, f.agent_id, Uuid::new_v4(), 0)
        .await
        .unwrap();
    assert!(matches!(
        f.custody
            .bind_initialization_placement(
                &f.owner,
                f.agent_id,
                &f.request,
                &["test:volume".into()],
                "fresh-fixture"
            )
            .await,
        Err(Error::Forbidden)
    ));
    let binding = f.store.agent_binding(f.agent_id).await.unwrap();
    assert!(binding.placement.is_none() && binding.workspace_keys.is_empty());
    f.store.pool().close().await;
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn pending_revocation_invalidates_initialization_without_ever_publishing_custody() {
    let f = Fixture::new().await;
    f.custody
        .begin_initialization(&f.owner, f.agent_id, &f.request)
        .await
        .unwrap();
    f.bind().await;
    let intent = f.allocate(InstancePurpose::Initialization).await;
    let runtime = intent.runtime.as_ref().unwrap();
    f.custody
        .release_initialization_key(&f.owner, runtime, f.request.operation_id)
        .await
        .unwrap();
    let revoked = f
        .custody
        .revoke(&f.owner, f.agent_id, Uuid::new_v4(), 0)
        .await
        .unwrap();
    assert_eq!(revoked.revision, 1);
    assert_eq!(
        f.custody
            .initialization(&f.owner, f.agent_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InitializationState::Revoked
    );
    assert!(
        f.custody
            .release_initialization_key(&f.owner, runtime, f.request.operation_id)
            .await
            .is_err()
    );
    assert!(
        f.custody
            .finish_initialization(
                &f.owner,
                runtime,
                f.request.operation_id,
                &AnchorHead {
                    sequence: 0,
                    digest: vec![31; 32]
                }
            )
            .await
            .is_err()
    );
    let status = f.custody.status(&f.owner, f.agent_id).await.unwrap();
    assert_eq!(status.active_version, None);
    assert!(status.retirement_pending);
    assert_eq!(
        f.custody
            .begin_initialization(&f.owner, f.agent_id, &f.request)
            .await
            .unwrap()
            .state,
        InitializationState::Revoked
    );
    f.store.pool().close().await;
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn pending_initialization_admits_only_its_initializer_without_execution_authority() {
    let f = Fixture::new().await;
    f.custody
        .begin_initialization(&f.owner, f.agent_id, &f.request)
        .await
        .unwrap();
    f.bind().await;
    let claim = f
        .store
        .claim_runtime(f.agent_id, Uuid::new_v4(), SignedDuration::from_mins(1))
        .await
        .unwrap();
    let mut plan = InstancePlan {
        purpose: InstancePurpose::Execution,
        provider: "fixture".into(),
        schema_version: 1,
        definition: json!({}),
    };
    for purpose in [InstancePurpose::Execution, InstancePurpose::Migration] {
        plan.purpose = purpose;
        assert!(matches!(
            f.store.begin_instance(&claim, &plan).await,
            Err(Error::Forbidden)
        ));
    }
    assert!(f.store.instance_intent(&claim).await.unwrap().is_none());
    plan.purpose = InstancePurpose::Initialization;
    let intent = f.store.begin_instance(&claim, &plan).await.unwrap();
    let runtime = Runtime {
        id: intent.id,
        agent_id: f.agent_id,
        generation: claim.generation,
        locator: "fixture".into(),
        engine_owner: claim.engine_owner.clone(),
    };
    assert!(
        f.custody
            .release_initialization_key(&f.owner, &runtime, f.request.operation_id)
            .await
            .is_err()
    );
    let admitted = f
        .store
        .admit_instance(
            &intent,
            &Admission {
                runtime_id: runtime.id,
                locator: runtime.locator.clone(),
                engine_owner: runtime.engine_owner.clone(),
                previous_writer: PreviousWriter::Absent,
            },
        )
        .await
        .unwrap();
    let runtime = admitted.runtime.as_ref().unwrap();
    let grant = f
        .store
        .select_grant(
            &f.owner,
            f.agent_id,
            &GrantSelection {
                mode: PermissionMode::FullAccess,
                scopes: vec![],
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        f.store
            .authorize_runtime_for(runtime, grant.id, grant.version, GrantPurpose::Task, &[])
            .await,
        Err(Error::Forbidden)
    ));
    f.custody
        .release_initialization_key(&f.owner, runtime, f.request.operation_id)
        .await
        .unwrap();
    f.store.pool().close().await;
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn http_completion_reconciles_the_original_receipt_without_readmitting_creation() {
    let f = Fixture::new().await;
    f.custody
        .begin_initialization(&f.owner, f.agent_id, &f.request)
        .await
        .unwrap();
    f.bind().await;
    let intent = f.allocate(InstancePurpose::Initialization).await;
    let runtime = intent.runtime.as_ref().unwrap();
    let operation = f.request.operation_id;
    assert_eq!(
        f.post(runtime, operation, "verify", None).await,
        (StatusCode::OK, json!({"allowed":true}))
    );
    assert_eq!(
        f.post(runtime, Uuid::new_v4(), "key", None).await.0,
        StatusCode::FORBIDDEN
    );
    f.policy.denied.store(true, Ordering::SeqCst);
    assert_ne!(
        f.post(runtime, operation, "key", None).await.0,
        StatusCode::OK
    );
    f.policy.denied.store(false, Ordering::SeqCst);
    let (status, key) = f.post(runtime, operation, "key", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(key["operationId"], operation.to_string());
    assert_eq!(
        BASE64_URL_SAFE_NO_PAD
            .decode(key["serviceKey"].as_str().unwrap())
            .unwrap()
            .len(),
        32
    );
    let head = AnchorHead {
        sequence: 0,
        digest: vec![31; 32],
    };
    assert_ne!(
        f.post(runtime, operation, "complete", Some(&head)).await.0,
        StatusCode::OK
    );
    f.store
        .initialize_anchor(runtime, &head.digest)
        .await
        .unwrap();
    let (status, receipt) = f.post(runtime, operation, "complete", Some(&head)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(receipt["operationId"], operation.to_string());
    assert_eq!(receipt["action"], "enrolled");
    assert_eq!(
        f.post(runtime, operation, "complete", Some(&head)).await,
        (StatusCode::OK, receipt.clone())
    );
    assert_eq!(
        f.post(runtime, operation, "key", None).await,
        (StatusCode::OK, key)
    );
    assert_eq!(
        f.post(runtime, operation, "verify", None).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        f.custody
            .status(&f.owner, f.agent_id)
            .await
            .unwrap()
            .active_version,
        Some(1)
    );
    assert!(f.store.runtime(f.agent_id).await.is_err());
    f.store
        .finish_instance(
            &intent,
            &PreviousWriter::Terminated(intent.id),
            Some(&head),
            None,
        )
        .await
        .unwrap();
    sqlx::query("UPDATE gesang_agents SET lease_until=clock_timestamp() WHERE id=$1")
        .bind(f.agent_id)
        .execute(f.store.pool())
        .await
        .unwrap();
    let another = f.allocate(InstancePurpose::Execution).await;
    assert_eq!(
        f.post(another.runtime.as_ref().unwrap(), operation, "key", None)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert!(
        f.custody
            .release_initialization_key(&f.owner, runtime, operation)
            .await
            .is_err()
    );
    f.custody
        .revoke(&f.owner, f.agent_id, Uuid::new_v4(), 1)
        .await
        .unwrap();
    assert_eq!(
        f.custody
            .receipt(&f.owner, f.agent_id, operation)
            .await
            .unwrap()
            .unwrap()
            .operation_id,
        operation
    );
    f.store.pool().close().await;
}
