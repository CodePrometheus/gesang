//! Internal transport behavior against an actual transactional store.
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "../../gesang-core/test-support.rs"]
mod support;
use support::ReadyFixture;

use std::{
    num::NonZeroUsize,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use chopin_protocol::{ChordClient, GoalCommand, GoalGrant};
use gesang_api::{
    CapabilityScope, CommandAuthorization, CommandSubmission, GrantPurpose, GrantSelection,
    PermissionMode,
};
use gesang_core::{
    Admission, AgentBinding, Custody, CustodyEnrollment, CustodyKey, EngineBridge,
    EnginePreparation, Error, InstanceIntent, InstancePhase, InstancePlan, InstancePurpose, Owner,
    PayloadKey, PreviousWriter, Result, Runtime, ServiceKey, ServiceKeyVerifier, Store,
    VerifiedIdentity,
};
use gesang_host::{
    ControlKey, ControlPlane, ControlPolicy, EngineAdapter, EngineConnectionPolicy,
    EngineConnector, ExecutionAction,
};
use jiff::{SignedDuration, Timestamp};
use serde_json::{Value, json};
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tower::ServiceExt;
use uuid::Uuid;

struct Policy;

struct ExecutionPolicy(AtomicBool);

#[async_trait]
impl ControlPolicy for ExecutionPolicy {
    async fn verify_execution(&self, binding: &AgentBinding, runtime: &Runtime) -> Result<()> {
        assert_eq!(binding.agent.id, runtime.agent_id);
        assert_eq!(binding.engine_owner, runtime.engine_owner);
        if !self.0.load(Ordering::SeqCst) {
            return Err(Error::FencingRequired);
        }
        Ok(())
    }

    async fn requirements(
        &self,
        _: &AgentBinding,
        _: &ExecutionAction,
    ) -> Result<Vec<CapabilityScope>> {
        Err(Error::Forbidden)
    }
}

#[async_trait]
impl ControlPolicy for Policy {
    async fn requirements(
        &self,
        binding: &AgentBinding,
        action: &ExecutionAction,
    ) -> Result<Vec<CapabilityScope>> {
        if let ExecutionAction::Tool { name } = action
            && name != "read_file"
        {
            return Err(Error::Forbidden);
        }
        Ok(vec![CapabilityScope {
            capability: "execute".into(),
            resource: binding.agent.id.to_string(),
        }])
    }
}

struct Fixture {
    store: Store,
    owner: Owner,
    runtime: Runtime,
    plane: ControlPlane,
    key: Arc<ControlKey>,
}

impl Fixture {
    async fn new() -> Self {
        let (fixture, intent) = Self::admitted(InstancePurpose::Execution).await;
        let unlocking = fixture
            .store
            .transition_instance(&intent, InstancePhase::Unlocking)
            .await
            .unwrap();
        fixture
            .store
            .transition_instance(&unlocking, InstancePhase::Ready)
            .await
            .unwrap();
        fixture
    }

    async fn admitted(purpose: InstancePurpose) -> (Self, InstanceIntent) {
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
        let store = Store::new(pool, PayloadKey::new([5; 32]));
        store.migrate().await.unwrap();
        let owner = store
            .bind_owner(&VerifiedIdentity {
                issuer: "test".into(),
                subject: "person".into(),
            })
            .await
            .unwrap();
        let agent = store
            .ensure_agent(&owner, "execution-v1", "local")
            .await
            .unwrap();
        let resources = [format!("fixture:{schema}")];
        if purpose == InstancePurpose::Initialization {
            let custody = Custody::new(store.clone(), CustodyKey::new([4; 32]).unwrap());
            let request = gesang_core::InitializationRequest {
                operation_id: Uuid::new_v4(),
                plan_digest: [19; 32],
            };
            custody
                .begin_initialization(&owner, agent.id, &request)
                .await
                .unwrap();
            custody
                .bind_initialization_placement(
                    &owner,
                    agent.id,
                    &request,
                    &resources,
                    "fixture-volume",
                )
                .await
                .unwrap();
        } else {
            store
                .bind_placement(&owner, agent.id, &resources, "fixture-volume")
                .await
                .unwrap();
        }
        let claim = store
            .claim_runtime(agent.id, Uuid::new_v4(), SignedDuration::from_mins(1))
            .await
            .unwrap();
        let planned = store
            .begin_instance(
                &claim,
                &InstancePlan {
                    purpose,
                    provider: "transport-fixture".into(),
                    schema_version: 1,
                    definition: json!({}),
                },
            )
            .await
            .unwrap();
        let intent = store
            .admit_instance(
                &planned,
                &Admission {
                    runtime_id: planned.id,
                    locator: "protected-service".into(),
                    engine_owner: "local".into(),
                    previous_writer: PreviousWriter::Absent,
                },
            )
            .await
            .unwrap();
        let runtime = intent.runtime.clone().unwrap();
        let key = Arc::new(ControlKey::new([8; 32]));
        let plane = ControlPlane::new(
            store.clone(),
            key.clone(),
            Arc::new(Policy),
            SignedDuration::from_secs(10),
        )
        .unwrap();
        (
            Self {
                store,
                owner,
                runtime,
                plane,
                key,
            },
            intent,
        )
    }

    async fn post(&self, path: &str, payload: Value) -> (StatusCode, Value) {
        let token = self.key.issue(&self.runtime).unwrap();
        self.post_as(path, payload, &self.runtime, token.expose())
            .await
    }

    async fn post_as(
        &self,
        path: &str,
        mut payload: Value,
        identity: &Runtime,
        credential: &str,
    ) -> (StatusCode, Value) {
        let object = payload.as_object_mut().unwrap();
        object.insert("agentId".into(), json!(identity.agent_id));
        object.insert("epoch".into(), json!(identity.generation));
        object.insert("owner".into(), json!(identity.engine_owner));
        let body = serde_json::to_vec(&payload).unwrap();
        let app = self
            .plane
            .clone()
            .router(NonZeroUsize::new(body.len()).unwrap());
        let response = app
            .oneshot(
                Request::post(path)
                    .header("authorization", format!("Bearer {credential}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
        let status = response.status();
        if status == StatusCode::SERVICE_UNAVAILABLE {
            assert_eq!(response.headers().get("retry-after").unwrap(), "10");
        }
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)));
        (status, value)
    }
}

fn head(sequence: i64) -> Value {
    json!({"sequence": sequence, "digest": BASE64_URL_SAFE_NO_PAD.encode([sequence as u8; 32])})
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn replacement_writer_reconciles_original_pending_anchor_and_rejects_old_credentials() {
    let mut fixture = Fixture::new().await;
    assert_eq!(
        fixture.post("/internal/v1/anchor/read", json!({})).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        fixture
            .post("/internal/v1/anchor/init", json!({"head":head(0)}))
            .await
            .0,
        StatusCode::OK
    );
    let pending = json!({"operationId":Uuid::new_v4(),"generation":fixture.runtime.generation,"next":head(1)});
    let (status, prepared) = fixture
        .post(
            "/internal/v1/anchor/prepare",
            json!({"expected":head(0),"pending":pending}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(prepared["pending"], pending);
    let old_runtime = fixture.runtime.clone();
    let old_token = fixture.key.issue(&old_runtime).unwrap();
    sqlx::query(
        "UPDATE gesang_agents SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(fixture.runtime.agent_id)
    .execute(fixture.store.pool())
    .await
    .unwrap();
    let claim = fixture
        .store
        .claim_runtime(
            old_runtime.agent_id,
            Uuid::new_v4(),
            SignedDuration::from_mins(1),
        )
        .await
        .unwrap();
    fixture.runtime = fixture
        .store
        .admit_ready_fixture(
            &claim,
            &Admission {
                runtime_id: Uuid::new_v4(),
                locator: "new-protected-service".into(),
                engine_owner: "local".into(),
                previous_writer: PreviousWriter::Terminated(old_runtime.id),
            },
        )
        .await
        .unwrap();
    let (status, _) = fixture
        .post_as(
            "/internal/v1/anchor/read",
            json!({}),
            &fixture.runtime,
            old_token.expose(),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, recovered) = fixture.post("/internal/v1/anchor/read", json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(recovered["pending"], pending);
    let mut rewritten = pending.clone();
    rewritten["generation"] = json!(fixture.runtime.generation);
    assert_eq!(
        fixture
            .post(
                "/internal/v1/anchor/confirm",
                json!({"pending":rewritten,"local":head(1)})
            )
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        fixture
            .post(
                "/internal/v1/anchor/confirm",
                json!({"pending":pending,"local":head(0)})
            )
            .await
            .0,
        StatusCode::CONFLICT
    );
    let (status, confirmed) = fixture
        .post(
            "/internal/v1/anchor/confirm",
            json!({"pending":pending,"local":head(1)}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(confirmed, json!({"head":head(1),"pending":null}));
    assert_eq!(
        fixture
            .post(
                "/internal/v1/anchor/confirm",
                json!({"pending":pending,"local":head(1)})
            )
            .await
            .1,
        confirmed
    );
    let next = json!({"operationId":Uuid::new_v4(),"generation":fixture.runtime.generation,"next":head(2)});
    assert_eq!(
        fixture
            .post(
                "/internal/v1/anchor/prepare",
                json!({"expected":head(1),"pending":next})
            )
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .post(
                "/internal/v1/anchor/abort",
                json!({"pending":next,"verifiedLocal":head(0)})
            )
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        fixture
            .post(
                "/internal/v1/anchor/abort",
                json!({"pending":next,"verifiedLocal":head(1)})
            )
            .await
            .1,
        confirmed
    );
    assert_eq!(
        fixture
            .post(
                "/internal/v1/anchor/init",
                json!({"head":{"sequence":0,"digest":"invalid"}})
            )
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        fixture
            .post("/internal/v1/anchor/read", json!({"unexpected":true}))
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn owner_controls_survive_revocation_but_execution_never_bypasses_host_policy() {
    let fixture = Fixture::new().await;
    let grant = fixture
        .store
        .select_grant(
            &fixture.owner,
            fixture.runtime.agent_id,
            &GrantSelection {
                mode: PermissionMode::FullAccess,
                scopes: vec![],
            },
        )
        .await
        .unwrap();
    let authority = |action, tool| json!({"grant":{"id":grant.id,"version":grant.version},"action":action,"tool":tool});
    assert_eq!(
        fixture
            .post("/internal/v1/authority", authority("submit", None::<&str>))
            .await
            .1["allowed"],
        true
    );
    assert_eq!(
        fixture
            .post(
                "/internal/v1/authority",
                authority("tool", Some("read_file"))
            )
            .await
            .1["allowed"],
        true
    );
    assert_eq!(
        fixture
            .post(
                "/internal/v1/authority",
                authority("tool", Some("platform_admin"))
            )
            .await
            .1["allowed"],
        false
    );
    let token = fixture.key.issue(&fixture.runtime).unwrap();
    let mut other = fixture.runtime.clone();
    other.engine_owner = "another".into();
    assert_eq!(
        fixture
            .post_as(
                "/internal/v1/authority",
                authority("stop", None),
                &other,
                token.expose()
            )
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    other = fixture.runtime.clone();
    other.agent_id = Uuid::new_v4();
    assert_ne!(
        fixture
            .post_as(
                "/internal/v1/authority",
                authority("stop", None),
                &other,
                token.expose()
            )
            .await
            .0,
        StatusCode::OK
    );
    fixture
        .store
        .revoke_grant(&fixture.owner, fixture.runtime.agent_id, grant.id)
        .await
        .unwrap();
    for action in ["submit", "model"] {
        let (status, decision) = fixture
            .post("/internal/v1/authority", authority(action, None))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(decision["allowed"], false);
    }
    for action in ["input", "stop"] {
        let (status, decision) = fixture
            .post("/internal/v1/authority", authority(action, None))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(decision["allowed"], true);
    }
    let scoped = fixture
        .store
        .select_grant(
            &fixture.owner,
            fixture.runtime.agent_id,
            &GrantSelection {
                mode: PermissionMode::Scoped,
                scopes: vec![],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .post(
                "/internal/v1/authority",
                json!({"action":"submit","grant":{"id":scoped.id,"version":scoped.version}})
            )
            .await
            .1["allowed"],
        false
    );
    sqlx::query(
        "UPDATE gesang_agents SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(fixture.runtime.agent_id)
    .execute(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        fixture
            .post("/internal/v1/authority", authority("stop", None))
            .await
            .0,
        StatusCode::CONFLICT
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn wake_callbacks_preserve_earlier_work_and_fail_closed_on_database_outage() {
    let fixture = Fixture::new().await;
    let due_ms = Timestamp::now().as_millisecond() - 1_000;
    assert_eq!(
        fixture
            .post(
                "/internal/v1/wake",
                json!({"wake":{"active":false,"at":due_ms}})
            )
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        fixture
            .post("/internal/v1/wake", json!({"wake":{"active":false}}))
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        fixture
            .post("/internal/v1/wake", json!({"wake":{"active":true}}))
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    let scheduled: gesang_core::PostgresTimestamp =
        sqlx::query_scalar("SELECT due_at FROM gesang_wakes WHERE agent_id=$1")
            .bind(fixture.runtime.agent_id)
            .fetch_one(fixture.store.pool())
            .await
            .unwrap();
    assert_eq!(scheduled.0, Timestamp::from_millisecond(due_ms).unwrap());
    assert_eq!(
        fixture
            .post(
                "/internal/v1/wake",
                json!({"wake":{"active":false,"at":1.5}})
            )
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    fixture.store.pool().close().await;
    let (status, problem) = fixture.post("/internal/v1/anchor/read", json!({})).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(problem["code"], "control_unavailable");
    assert!(!problem.to_string().contains("pool"));
}

struct NoConnection;

#[async_trait]
impl EngineConnector for NoConnection {
    async fn connect(&self, _: &Runtime) -> Result<ChordClient> {
        Err(Error::Uncertain(
            "no connection expected during admission".into(),
        ))
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn replacement_grants_require_matching_delegation_before_command_persistence() {
    let fixture = Fixture::new().await;
    let adapter = EngineAdapter::new(
        fixture.store.clone(),
        Arc::new(NoConnection),
        EngineConnectionPolicy {
            operation_timeout: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(1),
        },
    )
    .unwrap();
    let grant = fixture
        .store
        .select_grant(
            &fixture.owner,
            fixture.runtime.agent_id,
            &GrantSelection {
                mode: PermissionMode::FullAccess,
                scopes: vec![],
            },
        )
        .await
        .unwrap();
    let make_input = |replacement| GoalCommand::Input {
        goal_id: "1".into(),
        expected_version: 1,
        text: "Continue with this permission".into(),
        question_id: None,
        grant: replacement,
    };
    let plain = serde_json::to_value(make_input(None)).unwrap();
    let replacement = serde_json::to_value(make_input(Some(GoalGrant {
        id: grant.id.to_string(),
        version: grant.version as u64,
    })))
    .unwrap();
    let delegated = CommandAuthorization::Grant {
        id: grant.id,
        version: grant.version,
    };
    for authorization in [
        CommandAuthorization::Owner,
        CommandAuthorization::Grant {
            id: Uuid::new_v4(),
            version: grant.version,
        },
        CommandAuthorization::Grant {
            id: grant.id,
            version: grant.version + 1,
        },
    ] {
        assert!(matches!(
            adapter.authorize_command(&replacement, &authorization),
            Err(Error::Forbidden)
        ));
    }
    let submission = CommandSubmission {
        request_id: Uuid::new_v4(),
        authorization: delegated,
        command: replacement,
    };
    let requirements = adapter
        .authorize_command(&submission.command, &submission.authorization)
        .unwrap();
    fixture
        .store
        .enqueue(
            &fixture.owner,
            fixture.runtime.agent_id,
            &submission,
            &requirements,
        )
        .await
        .unwrap();
    fixture
        .store
        .revoke_grant(&fixture.owner, fixture.runtime.agent_id, grant.id)
        .await
        .unwrap();
    let revoked = CommandSubmission {
        request_id: Uuid::new_v4(),
        ..submission
    };
    assert!(matches!(
        fixture
            .store
            .enqueue(
                &fixture.owner,
                fixture.runtime.agent_id,
                &revoked,
                &requirements
            )
            .await,
        Err(Error::Forbidden)
    ));
    let owner_input = CommandSubmission {
        request_id: Uuid::new_v4(),
        authorization: CommandAuthorization::Owner,
        command: plain,
    };
    let requirements = adapter
        .authorize_command(&owner_input.command, &owner_input.authorization)
        .unwrap();
    fixture
        .store
        .enqueue(
            &fixture.owner,
            fixture.runtime.agent_id,
            &owner_input,
            &requirements,
        )
        .await
        .unwrap();
    let persisted: i64 = sqlx::query_scalar("SELECT count(*) FROM gesang_commands")
        .fetch_one(fixture.store.pool())
        .await
        .unwrap();
    assert_eq!(persisted, 2);
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn improvement_and_task_delegations_are_independent_and_cannot_substitute() {
    let fixture = Fixture::new().await;
    let selection = GrantSelection {
        mode: PermissionMode::FullAccess,
        scopes: vec![],
    };
    let task = fixture
        .store
        .select_grant(&fixture.owner, fixture.runtime.agent_id, &selection)
        .await
        .unwrap();
    let improvement = fixture
        .store
        .select_grant_for(
            &fixture.owner,
            fixture.runtime.agent_id,
            GrantPurpose::Improvement,
            &selection,
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .store
            .session(&fixture.owner)
            .await
            .unwrap()
            .grant
            .unwrap()
            .id,
        task.id
    );
    let request = |grant: &gesang_api::Grant, action: &str| json!({"grant":{"id":grant.id,"version":grant.version},"action":action});
    for (grant, action, allowed) in [
        (&task, "submit", true),
        (&task, "model", true),
        (&task, "rsi", false),
        (&improvement, "rsi", true),
        (&improvement, "submit", false),
        (&improvement, "model", false),
    ] {
        let (status, result) = fixture
            .post("/internal/v1/authority", request(grant, action))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result["allowed"], allowed);
    }
    assert!(matches!(
        fixture
            .store
            .authorize(
                fixture.runtime.agent_id,
                improvement.id,
                improvement.version,
                &[]
            )
            .await,
        Err(Error::Forbidden)
    ));
    fixture
        .store
        .revoke_grant(&fixture.owner, fixture.runtime.agent_id, task.id)
        .await
        .unwrap();
    assert!(
        fixture
            .store
            .session(&fixture.owner)
            .await
            .unwrap()
            .grant
            .is_none()
    );
    assert_eq!(
        fixture
            .post("/internal/v1/authority", request(&improvement, "rsi"))
            .await
            .1["allowed"],
        true
    );
    assert_eq!(
        fixture
            .post("/internal/v1/authority", json!({"action":"owner"}))
            .await
            .1["allowed"],
        true
    );
    assert_eq!(
        fixture
            .post("/internal/v1/authority", json!({"action":"rsi"}))
            .await
            .1["allowed"],
        false
    );
    let next_task = fixture
        .store
        .select_grant(&fixture.owner, fixture.runtime.agent_id, &selection)
        .await
        .unwrap();
    let replacement = fixture
        .store
        .select_grant_for(
            &fixture.owner,
            fixture.runtime.agent_id,
            GrantPurpose::Improvement,
            &selection,
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .post("/internal/v1/authority", request(&improvement, "rsi"))
            .await
            .1["allowed"],
        false
    );
    assert_eq!(
        fixture
            .post("/internal/v1/authority", request(&replacement, "rsi"))
            .await
            .1["allowed"],
        true
    );
    assert_eq!(
        fixture
            .post("/internal/v1/authority", request(&next_task, "model"))
            .await
            .1["allowed"],
        true
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn startup_can_reconcile_storage_but_cannot_resume_or_execute_before_ready() {
    let (fixture, intent) = Fixture::admitted(InstancePurpose::Execution).await;
    assert_eq!(
        fixture
            .post("/internal/v1/anchor/init", json!({"head":head(0)}))
            .await
            .0,
        StatusCode::OK
    );
    for action in [
        "resume", "owner", "model", "tool", "submit", "input", "stop", "rsi",
    ] {
        assert_eq!(
            fixture
                .post("/internal/v1/authority", json!({"action":action}))
                .await
                .0,
            StatusCode::SERVICE_UNAVAILABLE,
            "{action}"
        );
    }
    let unlocking = fixture
        .store
        .transition_instance(&intent, InstancePhase::Unlocking)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .post("/internal/v1/authority", json!({"action":"resume"}))
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        fixture.post("/internal/v1/anchor/read", json!({})).await.0,
        StatusCode::OK
    );
    let ready = fixture
        .store
        .transition_instance(&unlocking, InstancePhase::Ready)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .post("/internal/v1/authority", json!({"action":"resume"}))
            .await,
        (StatusCode::OK, json!({"allowed":true}))
    );
    assert_eq!(
        fixture
            .post("/internal/v1/authority", json!({"action":"migrate"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    fixture
        .store
        .transition_instance(&ready, InstancePhase::Draining)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .post("/internal/v1/authority", json!({"action":"resume"}))
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        fixture.post("/internal/v1/anchor/read", json!({})).await.0,
        StatusCode::OK
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn execution_key_requires_current_custody_writer_and_explicit_host_verification() {
    let (mut f, starting) = Fixture::admitted(InstancePurpose::Execution).await;
    let custody = Arc::new(Custody::new(
        f.store.clone(),
        CustodyKey::new([4; 32]).unwrap(),
    ));
    custody
        .enroll(
            &f.owner,
            f.runtime.agent_id,
            &CustodyEnrollment {
                operation_id: Uuid::new_v4(),
                expected_revision: 0,
                service_key: ServiceKey::new([3; 32]),
            },
            &FixtureKeyVerifier,
        )
        .await
        .unwrap();
    assert_eq!(
        f.post("/internal/v1/execution/key", json!({})).await.0,
        StatusCode::FORBIDDEN
    );
    f.plane = f.plane.clone().with_execution_custody(custody.clone());
    assert_eq!(
        f.post("/internal/v1/execution/key", json!({})).await.0,
        StatusCode::CONFLICT
    );
    let policy = Arc::new(ExecutionPolicy(AtomicBool::new(true)));
    f.plane = ControlPlane::new(
        f.store.clone(),
        f.key.clone(),
        policy.clone(),
        SignedDuration::from_secs(10),
    )
    .unwrap()
    .with_execution_custody(custody.clone());
    let response = json!({"revision":1,"serviceKey":BASE64_URL_SAFE_NO_PAD.encode([3;32])});
    assert_eq!(
        f.post("/internal/v1/execution/key", json!({})).await,
        (StatusCode::OK, response.clone())
    );
    let unlocking = f
        .store
        .transition_instance(&starting, InstancePhase::Unlocking)
        .await
        .unwrap();
    assert_eq!(
        f.post("/internal/v1/execution/key", json!({})).await,
        (StatusCode::OK, response.clone())
    );
    let ready = f
        .store
        .transition_instance(&unlocking, InstancePhase::Ready)
        .await
        .unwrap();
    assert_eq!(
        f.post("/internal/v1/execution/key", json!({})).await,
        (StatusCode::OK, response)
    );
    assert_eq!(
        f.post_as("/internal/v1/execution/key", json!({}), &f.runtime, "wrong")
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let mut foreign = f.runtime.clone();
    foreign.engine_owner = "foreign-owner".into();
    let token = f.key.issue(&f.runtime).unwrap();
    assert_eq!(
        f.post_as(
            "/internal/v1/execution/key",
            json!({}),
            &foreign,
            token.expose()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    policy.0.store(false, Ordering::SeqCst);
    assert_eq!(
        f.post("/internal/v1/execution/key", json!({})).await.0,
        StatusCode::CONFLICT
    );
    let releases: i64 = sqlx::query_scalar("SELECT count(*) FROM gesang_custody_releases")
        .fetch_one(f.store.pool())
        .await
        .unwrap();
    assert_eq!(releases, 3);
    policy.0.store(true, Ordering::SeqCst);
    f.store
        .transition_instance(&ready, InstancePhase::Draining)
        .await
        .unwrap();
    assert_eq!(
        f.post("/internal/v1/execution/key", json!({})).await.0,
        StatusCode::FORBIDDEN
    );
    custody
        .revoke(&f.owner, f.runtime.agent_id, Uuid::new_v4(), 1)
        .await
        .unwrap();
    assert_eq!(
        f.post("/internal/v1/execution/key", json!({})).await.0,
        StatusCode::CONFLICT
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn nonexecution_writers_never_receive_execution_keys() {
    for purpose in [InstancePurpose::Migration, InstancePurpose::Initialization] {
        let (mut f, _) = Fixture::admitted(purpose).await;
        let custody = Arc::new(Custody::new(
            f.store.clone(),
            CustodyKey::new([4; 32]).unwrap(),
        ));
        f.plane = ControlPlane::new(
            f.store.clone(),
            f.key.clone(),
            Arc::new(ExecutionPolicy(AtomicBool::new(true))),
            SignedDuration::from_secs(10),
        )
        .unwrap()
        .with_execution_custody(custody);
        assert_eq!(
            f.post("/internal/v1/execution/key", json!({})).await.0,
            StatusCode::FORBIDDEN
        );
        let releases: i64 = sqlx::query_scalar("SELECT count(*) FROM gesang_custody_releases")
            .fetch_one(f.store.pool())
            .await
            .unwrap();
        assert_eq!(releases, 0);
    }
}

struct MigrationPolicy {
    fenced: AtomicBool,
    verifications: AtomicUsize,
}

#[async_trait]
impl ControlPolicy for MigrationPolicy {
    async fn requirements(
        &self,
        _: &AgentBinding,
        _: &ExecutionAction,
    ) -> Result<Vec<CapabilityScope>> {
        Err(Error::Forbidden)
    }

    async fn verify_migration(&self, binding: &AgentBinding, runtime: &Runtime) -> Result<()> {
        assert_eq!(binding.agent.id, runtime.agent_id);
        self.verifications.fetch_add(1, Ordering::SeqCst);
        if self.fenced.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(Error::FencingRequired)
        }
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn migration_checks_host_fencing_on_every_action_and_never_receives_execution_authority() {
    let (mut fixture, intent) = Fixture::admitted(InstancePurpose::Migration).await;
    assert_eq!(
        fixture
            .post("/internal/v1/authority", json!({"action":"migrate"}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let policy = Arc::new(MigrationPolicy {
        fenced: AtomicBool::new(true),
        verifications: AtomicUsize::new(0),
    });
    fixture.plane = ControlPlane::new(
        fixture.store.clone(),
        fixture.key.clone(),
        policy.clone(),
        SignedDuration::from_secs(10),
    )
    .unwrap();
    assert_eq!(
        fixture
            .post("/internal/v1/authority", json!({"action":"migrate"}))
            .await,
        (StatusCode::OK, json!({"allowed":true}))
    );
    assert_eq!(
        fixture
            .post("/internal/v1/anchor/init", json!({"head":head(0)}))
            .await
            .0,
        StatusCode::OK
    );
    for action in [
        "resume", "owner", "model", "tool", "submit", "input", "stop", "rsi",
    ] {
        assert_eq!(
            fixture
                .post("/internal/v1/authority", json!({"action":action}))
                .await
                .0,
            StatusCode::FORBIDDEN,
            "{action}"
        );
    }
    assert_eq!(
        fixture
            .post(
                "/internal/v1/authority",
                json!({"action":"migrate","grant":{"id":Uuid::new_v4(),"version":1}})
            )
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    policy.fenced.store(false, Ordering::SeqCst);
    assert_eq!(
        fixture
            .post("/internal/v1/authority", json!({"action":"migrate"}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(policy.verifications.load(Ordering::SeqCst), 2);
    fixture
        .store
        .finish_instance(&intent, &PreviousWriter::Terminated(intent.id), None, None)
        .await
        .unwrap();
    policy.fenced.store(true, Ordering::SeqCst);
    assert_eq!(
        fixture
            .post("/internal/v1/authority", json!({"action":"migrate"}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(policy.verifications.load(Ordering::SeqCst), 2);
}

struct FixtureKeyVerifier;

#[async_trait]
impl ServiceKeyVerifier for FixtureKeyVerifier {
    async fn verify(&self, binding: &AgentBinding, key: &ServiceKey) -> Result<()> {
        assert_eq!(binding.engine_owner, "local");
        assert_eq!(key.expose(), &[3; 32]);
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn migration_key_handoff_requires_active_custody_current_writer_and_physical_fencing() {
    let (mut fixture, intent) = Fixture::admitted(InstancePurpose::Migration).await;
    let policy = Arc::new(MigrationPolicy {
        fenced: AtomicBool::new(true),
        verifications: AtomicUsize::new(0),
    });
    fixture.plane = ControlPlane::new(
        fixture.store.clone(),
        fixture.key.clone(),
        policy.clone(),
        SignedDuration::from_secs(10),
    )
    .unwrap();
    assert_eq!(
        fixture
            .post("/internal/v1/migration/key", json!({}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let custody = Arc::new(Custody::new(
        fixture.store.clone(),
        CustodyKey::new([4; 32]).unwrap(),
    ));
    fixture.plane = fixture
        .plane
        .clone()
        .with_migration_custody(custody.clone());
    assert_eq!(
        fixture
            .post("/internal/v1/migration/key", json!({}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    custody
        .enroll(
            &fixture.owner,
            fixture.runtime.agent_id,
            &CustodyEnrollment {
                operation_id: Uuid::new_v4(),
                expected_revision: 0,
                service_key: ServiceKey::new([3; 32]),
            },
            &FixtureKeyVerifier,
        )
        .await
        .unwrap();
    let mut foreign = fixture.runtime.clone();
    foreign.engine_owner = "another-owner".into();
    let token = fixture.key.issue(&fixture.runtime).unwrap();
    assert_eq!(
        fixture
            .post_as(
                "/internal/v1/migration/key",
                json!({}),
                &foreign,
                token.expose()
            )
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        fixture
            .post_as(
                "/internal/v1/migration/key",
                json!({}),
                &fixture.runtime,
                "wrong"
            )
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        fixture.post("/internal/v1/migration/key", json!({})).await,
        (
            StatusCode::OK,
            json!({"revision":1, "serviceKey":BASE64_URL_SAFE_NO_PAD.encode([3;32])})
        )
    );
    policy.fenced.store(false, Ordering::SeqCst);
    assert_eq!(
        fixture
            .post("/internal/v1/migration/key", json!({}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let releases: i64 = sqlx::query_scalar("SELECT count(*) FROM gesang_custody_releases")
        .fetch_one(fixture.store.pool())
        .await
        .unwrap();
    assert_eq!(releases, 1);
    policy.fenced.store(true, Ordering::SeqCst);
    fixture
        .store
        .transition_instance(&intent, InstancePhase::Draining)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .post("/internal/v1/migration/key", json!({}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    custody
        .revoke(&fixture.owner, fixture.runtime.agent_id, Uuid::new_v4(), 1)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .post("/internal/v1/migration/key", json!({}))
            .await
            .0,
        StatusCode::CONFLICT
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn execution_and_unadmitted_migration_credentials_never_receive_migration_keys() {
    let (mut fixture, starting) = Fixture::admitted(InstancePurpose::Execution).await;
    let policy = Arc::new(MigrationPolicy {
        fenced: AtomicBool::new(true),
        verifications: AtomicUsize::new(0),
    });
    let custody = Arc::new(Custody::new(
        fixture.store.clone(),
        CustodyKey::new([4; 32]).unwrap(),
    ));
    fixture.plane = ControlPlane::new(
        fixture.store.clone(),
        fixture.key.clone(),
        policy.clone(),
        SignedDuration::from_secs(10),
    )
    .unwrap()
    .with_migration_custody(custody.clone());
    custody
        .enroll(
            &fixture.owner,
            fixture.runtime.agent_id,
            &CustodyEnrollment {
                operation_id: Uuid::new_v4(),
                expected_revision: 0,
                service_key: ServiceKey::new([3; 32]),
            },
            &FixtureKeyVerifier,
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .post("/internal/v1/migration/key", json!({}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let unlocking = fixture
        .store
        .transition_instance(&starting, InstancePhase::Unlocking)
        .await
        .unwrap();
    let ready = fixture
        .store
        .transition_instance(&unlocking, InstancePhase::Ready)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .post("/internal/v1/migration/key", json!({}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    fixture
        .store
        .finish_instance(&ready, &PreviousWriter::Terminated(ready.id), None, None)
        .await
        .unwrap();
    let claim = fixture
        .store
        .claim_runtime(
            fixture.runtime.agent_id,
            starting.claim.controller_id,
            SignedDuration::from_mins(1),
        )
        .await
        .unwrap();
    let next = fixture
        .store
        .advance_fenced_generation(&claim)
        .await
        .unwrap();
    let planned = fixture
        .store
        .begin_instance(
            &next,
            &InstancePlan {
                purpose: InstancePurpose::Migration,
                provider: "transport-fixture".into(),
                schema_version: 1,
                definition: json!({}),
            },
        )
        .await
        .unwrap();
    let unadmitted = Runtime {
        id: planned.id,
        agent_id: next.agent.id,
        generation: next.generation,
        locator: "unadmitted".into(),
        engine_owner: next.engine_owner.clone(),
    };
    let token = fixture.key.issue(&unadmitted).unwrap();
    assert_eq!(
        fixture
            .post_as(
                "/internal/v1/migration/key",
                json!({}),
                &unadmitted,
                token.expose()
            )
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(policy.verifications.load(Ordering::SeqCst), 0);
    let releases: i64 = sqlx::query_scalar("SELECT count(*) FROM gesang_custody_releases")
        .fetch_one(fixture.store.pool())
        .await
        .unwrap();
    assert_eq!(releases, 0);
}

struct ConnectingGuard(Arc<AtomicUsize>);

impl Drop for ConnectingGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct SlowConnector {
    active: Arc<AtomicUsize>,
    started: tokio::sync::Notify,
}

#[async_trait]
impl EngineConnector for SlowConnector {
    async fn connect(&self, _: &Runtime) -> Result<ChordClient> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _guard = ConnectingGuard(self.active.clone());
        self.started.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn slow_unlock_does_not_block_lease_renewal_and_retirement_cancels_its_owned_task() {
    let (fixture, intent) = Fixture::admitted(InstancePurpose::Execution).await;
    fixture
        .store
        .transition_instance(&intent, InstancePhase::Unlocking)
        .await
        .unwrap();
    let connector = Arc::new(SlowConnector {
        active: Arc::new(AtomicUsize::new(0)),
        started: tokio::sync::Notify::new(),
    });
    let adapter = EngineAdapter::new(
        fixture.store.clone(),
        connector.clone(),
        EngineConnectionPolicy {
            operation_timeout: Duration::from_secs(60),
            idle_timeout: Duration::from_secs(60),
        },
    )
    .unwrap();
    assert_eq!(
        adapter.prepare(&fixture.runtime).await.unwrap(),
        EnginePreparation::Pending
    );
    connector.started.notified().await;
    assert_eq!(connector.active.load(Ordering::SeqCst), 1);
    assert_eq!(
        adapter.prepare(&fixture.runtime).await.unwrap(),
        EnginePreparation::Pending
    );
    let renewed = fixture
        .store
        .claim_runtime(
            intent.claim.agent.id,
            intent.claim.controller_id,
            SignedDuration::from_mins(1),
        )
        .await
        .unwrap();
    assert_eq!(renewed.generation, intent.claim.generation);
    assert_eq!(connector.active.load(Ordering::SeqCst), 1);
    adapter.retire(&fixture.runtime).await.unwrap();
    assert_eq!(connector.active.load(Ordering::SeqCst), 0);
    assert_eq!(
        adapter.prepare(&fixture.runtime).await.unwrap(),
        EnginePreparation::Pending
    );
    connector.started.notified().await;
    adapter.shutdown().await.unwrap();
    assert_eq!(connector.active.load(Ordering::SeqCst), 0);
    assert!(matches!(
        adapter.prepare(&fixture.runtime).await,
        Err(Error::Uncertain(_))
    ));
}
