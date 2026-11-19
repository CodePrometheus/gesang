//! Actual native initialization and reconciliation through shared HTTP custody and transactional storage.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{num::NonZeroUsize, str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use chrono::TimeDelta;
use gesang_api::CapabilityScope;
use gesang_core::{
    Admission, AgentBinding, Custody, CustodyKey, Error, Initialization, InitializationRequest,
    InitializationState, InstancePlan, InstancePurpose, PayloadKey, PreviousWriter, Result,
    Runtime, Store, VerifiedIdentity,
};
use gesang_host::{ControlKey, ControlPlane, ControlPolicy, ExecutionAction};
use serde_json::{Value, json};
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{net::TcpListener, process::Command, sync::oneshot, time::timeout};
use uuid::Uuid;

struct FixturePolicy {
    operation: Uuid,
}

#[async_trait]
impl ControlPolicy for FixturePolicy {
    async fn verify_initialization(
        &self,
        _: &AgentBinding,
        _: &Runtime,
        initialization: &Initialization,
    ) -> Result<()> {
        if initialization.operation_id != self.operation {
            return Err(Error::Forbidden);
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

#[tokio::test]
#[ignore = "requires isolated database, compiled initializer and actual native storage module"]
async fn actual_initializer_and_existing_store_reconciliation_share_one_key_marker_and_receipt() {
    verify_initialization("reconcile").await;
}

#[tokio::test]
#[ignore = "requires isolated database, compiled initializer and actual native storage module"]
async fn key_response_loss() {
    verify_initialization("key-response-lost").await;
}

#[tokio::test]
#[ignore = "requires isolated database, compiled initializer and actual native storage module"]
async fn genesis_interruption() {
    verify_initialization("genesis-interrupted").await;
}

#[tokio::test]
#[ignore = "requires isolated database, compiled initializer and actual native storage module"]
async fn completion_response_loss() {
    verify_initialization("completion-response-lost").await;
}

#[tokio::test]
#[ignore = "requires isolated database, compiled initializer and actual native storage module"]
async fn closed_store_interruption() {
    verify_initialization("completion-interrupted").await;
}

async fn verify_initialization(scenario: &str) {
    let url =
        std::env::var("GESANG_TEST_DATABASE_URL").expect("isolated integration database required");
    let probe = std::env::var("GESANG_TEST_INITIALIZATION_PROBE")
        .expect("built initializer probe required");
    let native = std::env::var("CHOPIN_STORAGE_NATIVE").expect("compiled native module required");
    let milliseconds: u64 = std::env::var("GESANG_TEST_STORAGE_TIMEOUT_MS")
        .expect("explicit integration deadline required")
        .parse()
        .unwrap();
    assert!(milliseconds > 0);
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
            issuer: "native-initialization".into(),
            subject: "owner".into(),
        })
        .await
        .unwrap();
    let agent = store.ensure_agent(&owner, "engine", "local").await.unwrap();
    let custody = Arc::new(Custody::new(
        store.clone(),
        CustodyKey::new([23; 32]).unwrap(),
    ));
    let operation = Uuid::new_v4();
    let request = InitializationRequest {
        operation_id: operation,
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
            &[format!("test-native:{operation}")],
            "native-fixture",
        )
        .await
        .unwrap();
    let claim = store
        .claim_runtime(
            agent.id,
            Uuid::new_v4(),
            TimeDelta::milliseconds(milliseconds.try_into().unwrap()),
        )
        .await
        .unwrap();
    let intent = store
        .begin_instance(
            &claim,
            &InstancePlan {
                purpose: InstancePurpose::Initialization,
                provider: "native-fixture".into(),
                schema_version: 1,
                definition: json!({}),
            },
        )
        .await
        .unwrap();
    let intent = store
        .admit_instance(
            &intent,
            &Admission {
                runtime_id: intent.id,
                locator: "native-fixture".into(),
                engine_owner: claim.engine_owner.clone(),
                previous_writer: PreviousWriter::Absent,
            },
        )
        .await
        .unwrap();
    let runtime = intent.runtime.as_ref().unwrap();
    let key = Arc::new(ControlKey::new([17; 32]));
    let token = key.issue(runtime).unwrap();
    let router = ControlPlane::new(
        store.clone(),
        key,
        Arc::new(FixturePolicy { operation }),
        TimeDelta::milliseconds(7),
    )
    .unwrap()
    .with_initialization_custody(custody.clone())
    .router(NonZeroUsize::new(4096).unwrap());
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (shutdown, stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let destination = std::env::temp_dir().join(format!("gesang-initialization-{operation}"));
    let child = Command::new("node")
        .arg(&probe)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap())
        .env("CHOPIN_STORAGE_NATIVE", native)
        .env("CHOPIN_CONTROL_URL", base)
        .env("CHOPIN_CONTROL_TOKEN", token.expose())
        .env("CHOPIN_AGENT_ID", agent.id.to_string())
        .env("CHOPIN_RUNTIME_EPOCH", runtime.generation.to_string())
        .env("CHOPIN_TEST_STORAGE_DIR", &destination)
        .env("CHOPIN_TEST_INITIALIZATION_SCENARIO", scenario)
        .env(
            "CHOPIN_TEST_INITIALIZATION_OPERATION_ID",
            operation.to_string(),
        )
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let output = timeout(
        Duration::from_millis(milliseconds),
        child.wait_with_output(),
    )
    .await;
    shutdown.send(()).unwrap();
    server.await.unwrap();
    let output = output.unwrap().unwrap();
    assert!(
        output.status.success(),
        "initializer probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let observed: Value = serde_json::from_slice(&output.stdout).unwrap();
    for property in [
        "initialized",
        "reconciled",
        "encrypted",
        "noProjectStarted",
        "keyConsistent",
    ] {
        assert_eq!(observed[property], true);
    }
    assert_eq!(observed["faultObserved"], scenario != "reconcile");
    let record = custody
        .initialization(&owner, agent.id)
        .await
        .unwrap()
        .unwrap();
    let InitializationState::Completed { head, receipt, .. } = record.state else {
        panic!("native initialization did not complete");
    };
    assert_eq!(receipt.operation_id, operation);
    assert_eq!(
        serde_json::to_value(&receipt).unwrap(),
        observed["receipt"]["custody"]
    );
    assert_eq!(
        head.sequence,
        observed["receipt"]["confirmedHead"]["sequence"]
            .as_i64()
            .unwrap()
    );
    assert_eq!(
        BASE64_URL_SAFE_NO_PAD.encode(&head.digest),
        observed["receipt"]["confirmedHead"]["digest"]
    );
    assert_eq!(store.anchor(runtime).await.unwrap().head, head);
    assert!(store.runtime(agent.id).await.is_err());
    assert_eq!(
        custody
            .status(&owner, agent.id)
            .await
            .unwrap()
            .active_version,
        Some(receipt.revision)
    );
    println!(
        "verified native initialization artifacts: {}",
        destination.display()
    );
    store.pool().close().await;
}
