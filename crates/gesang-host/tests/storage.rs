//! Native encrypted storage against the real TCP control plane and transactional freshness anchors.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    num::NonZeroUsize, path::PathBuf, process::Stdio, str::FromStr, sync::Arc, time::Duration,
};

use async_trait::async_trait;
use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use gesang_api::CapabilityScope;
use gesang_core::{
    Admission, AgentBinding, Custody, CustodyEnrollment, CustodyKey, Error, InstancePhase,
    InstancePlan, InstancePurpose, PayloadKey, PreviousWriter, Result, Runtime, RuntimeClaim,
    ServiceKey, ServiceKeyVerifier, Store, VerifiedIdentity,
};
use gesang_host::{ControlKey, ControlPlane, ControlPolicy, ExecutionAction};
use jiff::SignedDuration;
use serde_json::{Value, json};
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    task::JoinHandle,
    time::timeout,
};
use uuid::Uuid;

struct Policy {
    migration_checks: Arc<AtomicUsize>,
}

struct FixtureKeyVerifier;

#[async_trait]
impl ServiceKeyVerifier for FixtureKeyVerifier {
    async fn verify(&self, _: &AgentBinding, key: &ServiceKey) -> Result<()> {
        if key.expose() == &[11; 32] {
            Ok(())
        } else {
            Err(Error::Forbidden)
        }
    }
}

#[async_trait]
impl ControlPolicy for Policy {
    async fn verify_migration(&self, _: &AgentBinding, _: &Runtime) -> Result<()> {
        self.migration_checks.fetch_add(1, Ordering::SeqCst);
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

struct Fixture {
    store: Store,
    runtime: Runtime,
    key: Arc<ControlKey>,
    url: String,
    dir: PathBuf,
    script: String,
    module: String,
    deadline: Duration,
    server: JoinHandle<()>,
    migration_checks: Arc<AtomicUsize>,
}

async fn migration_writer(store: &Store, claim: &RuntimeClaim) -> Runtime {
    let intent = store
        .begin_instance(
            claim,
            &InstancePlan {
                purpose: InstancePurpose::Migration,
                provider: "native-storage-fixture".into(),
                schema_version: 1,
                definition: json!({}),
            },
        )
        .await
        .unwrap();
    let admitted = store
        .admit_instance(
            &intent,
            &Admission {
                runtime_id: intent.id,
                locator: format!("protected-native-storage-fixture:{}", intent.id),
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
    assert_eq!(admitted.phase, InstancePhase::Migrating);
    let runtime = admitted.runtime.unwrap();
    assert!(matches!(
        store.verify_execution(&runtime).await,
        Err(Error::Forbidden)
    ));
    runtime
}

impl Fixture {
    async fn new() -> Self {
        Self::with_purpose(InstancePurpose::Migration).await
    }

    async fn with_purpose(purpose: InstancePurpose) -> Self {
        let database = std::env::var("GESANG_TEST_DATABASE_URL")
            .expect("the isolated integration database is required");
        let script = std::env::var("GESANG_TEST_STORAGE_PROBE")
            .expect("the built upstream storage probe is required");
        let module = std::env::var("CHOPIN_STORAGE_NATIVE")
            .expect("the built native storage module is required");
        let milliseconds = std::env::var("GESANG_TEST_STORAGE_TIMEOUT_MS")
            .expect("the storage test deadline is required")
            .parse::<u64>()
            .unwrap();
        assert!(milliseconds > 0);
        let deadline = Duration::from_millis(milliseconds);
        let schema = format!("test_{}", Uuid::new_v4().simple());
        let bootstrap = PgPoolOptions::new().connect(&database).await.unwrap();
        bootstrap
            .execute(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .await
            .unwrap();
        let pool = PgPoolOptions::new()
            .connect_with(
                PgConnectOptions::from_str(&database)
                    .unwrap()
                    .options([("search_path", schema.as_str())]),
            )
            .await
            .unwrap();
        bootstrap.close().await;
        let store = Store::new(pool, PayloadKey::new([49; 32]));
        store.migrate().await.unwrap();
        let owner = store
            .bind_owner(&VerifiedIdentity {
                issuer: "native-storage-fixture".into(),
                subject: "person".into(),
            })
            .await
            .unwrap();
        let agent = store
            .ensure_agent(&owner, "execution-v1", "local")
            .await
            .unwrap();
        store
            .bind_placement(
                &owner,
                agent.id,
                &[format!("native-storage:{schema}")],
                "isolated-storage-fixture",
            )
            .await
            .unwrap();
        let claim = store
            .claim_runtime(
                agent.id,
                Uuid::new_v4(),
                SignedDuration::try_from(deadline).unwrap(),
            )
            .await
            .unwrap();
        let runtime = if purpose == InstancePurpose::Migration {
            migration_writer(&store, &claim).await
        } else {
            let intent = store
                .begin_instance(
                    &claim,
                    &InstancePlan {
                        purpose,
                        provider: "native-storage-fixture".into(),
                        schema_version: 1,
                        definition: json!({}),
                    },
                )
                .await
                .unwrap();
            let admitted = store
                .admit_instance(
                    &intent,
                    &Admission {
                        runtime_id: intent.id,
                        locator: format!("protected-legacy-home-fixture:{}", intent.id),
                        engine_owner: claim.engine_owner.clone(),
                        previous_writer: PreviousWriter::Absent,
                    },
                )
                .await
                .unwrap();
            let unlocking = store
                .transition_instance(&admitted, InstancePhase::Unlocking)
                .await
                .unwrap();
            let runtime = unlocking.runtime.unwrap();
            assert!(store.verify_execution(&runtime).await.is_err());
            runtime
        };
        let key = Arc::new(ControlKey::new([73; 32]));
        let migration_checks = Arc::new(AtomicUsize::new(0));
        let custody = Arc::new(Custody::new(
            store.clone(),
            CustodyKey::new([43; 32]).unwrap(),
        ));
        custody
            .enroll(
                &owner,
                agent.id,
                &CustodyEnrollment {
                    operation_id: Uuid::new_v4(),
                    expected_revision: 0,
                    service_key: ServiceKey::new([11; 32]),
                },
                &FixtureKeyVerifier,
            )
            .await
            .unwrap();
        let plane = ControlPlane::new(
            store.clone(),
            key.clone(),
            Arc::new(Policy {
                migration_checks: migration_checks.clone(),
            }),
            SignedDuration::from_secs(10),
        )
        .unwrap()
        .with_migration_custody(custody);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, plane.router(NonZeroUsize::new(4096).unwrap()))
                .await
                .unwrap();
        });
        let dir = std::env::temp_dir().join(format!("gesang-native-store-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        Self {
            store,
            runtime,
            key,
            url,
            dir,
            script,
            module,
            deadline,
            server,
            migration_checks,
        }
    }

    fn command(
        &self,
        action: &str,
        entry: Option<u64>,
        pause: Option<&str>,
        fault: Option<&str>,
        dir: &std::path::Path,
    ) -> Command {
        let mut command = Command::new("node");
        command
            .arg(&self.script)
            .env("CHOPIN_CONTROL_URL", &self.url)
            .env(
                "CHOPIN_CONTROL_TOKEN",
                self.key.issue(&self.runtime).unwrap().expose(),
            )
            .env("CHOPIN_AGENT_ID", self.runtime.agent_id.to_string())
            .env("CHOPIN_RUNTIME_EPOCH", self.runtime.generation.to_string())
            .env("CHOPIN_STORAGE_NATIVE", &self.module)
            .env("CHOPIN_TEST_STORAGE_DIR", dir)
            .env(
                "CHOPIN_TEST_STORAGE_KEY",
                BASE64_URL_SAFE_NO_PAD.encode([31; 32]),
            )
            .env("CHOPIN_TEST_STORAGE_ACTION", action)
            .env_remove("CHOPIN_TEST_STORAGE_PAUSE")
            .env_remove("CHOPIN_TEST_STORAGE_FAULT")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(entry) = entry {
            command.env("CHOPIN_TEST_STORAGE_ENTRY", entry.to_string());
        }
        if let Some(pause) = pause {
            command.env("CHOPIN_TEST_STORAGE_PAUSE", pause);
        }
        if let Some(fault) = fault {
            command.env("CHOPIN_TEST_STORAGE_FAULT", fault);
        }
        command
    }

    async fn run(&self, action: &str, entry: Option<u64>, fault: Option<&str>) -> Value {
        let output = timeout(
            self.deadline,
            self.command(action, entry, None, fault, &self.dir)
                .spawn()
                .unwrap()
                .wait_with_output(),
        )
        .await
        .expect("the storage probe exceeded its configured deadline")
        .unwrap();
        assert!(
            output.status.success(),
            "the storage probe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(
            output
                .stdout
                .split(|byte| *byte == b'\n')
                .rfind(|line| !line.is_empty())
                .unwrap(),
        )
        .unwrap()
    }

    async fn pause(&self, entry: u64, stage: &str) -> Child {
        let mut child = self
            .command("write", Some(entry), Some(stage), None, &self.dir)
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut lines = BufReader::new(stdout).lines();
        let line = timeout(self.deadline, lines.next_line())
            .await
            .expect("the storage probe did not reach its configured phase")
            .unwrap()
            .expect("the storage probe stopped before its configured phase");
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap(),
            json!({"stage":stage})
        );
        child
    }

    async fn replace(&mut self) {
        let old = self.runtime.clone();
        sqlx::query("UPDATE gesang_agents SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1").bind(old.agent_id).execute(self.store.pool()).await.unwrap();
        let claim = self
            .store
            .claim_runtime(
                old.agent_id,
                Uuid::new_v4(),
                SignedDuration::try_from(self.deadline).unwrap(),
            )
            .await
            .unwrap();
        self.runtime = migration_writer(&self.store, &claim).await;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test]
#[ignore = "requires the actual SQLCipher artifact, upstream CLI build, and an isolated transactional database"]
async fn admitted_migration_cli_preserves_real_history_and_confirms_before_receipt() {
    let fixture = Fixture::new().await;
    let result = fixture.run("migrate", None, None).await;
    assert_eq!(result["migrated"], true);
    assert_eq!(result["repeated"], true);
    assert_eq!(result["preserved"], true);
    assert_eq!(result["hostCustody"], true);
    assert!(fixture.migration_checks.load(Ordering::SeqCst) >= 10);
    let anchor = fixture.store.anchor(&fixture.runtime).await.unwrap();
    assert!(anchor.head.sequence > 0);
    assert!(anchor.pending.is_none());
    assert_eq!(
        result["receipt"]["confirmedHead"],
        json!({
            "sequence": anchor.head.sequence,
            "digest": BASE64_URL_SAFE_NO_PAD.encode(anchor.head.digest)
        })
    );
}

#[tokio::test]
#[ignore = "requires the actual SQLCipher artifact, upstream sealed service build, and an isolated transactional database"]
async fn legacy_sealed_home_restores_its_real_provider_catalog_while_execution_is_paused() {
    let fixture = Fixture::with_purpose(InstancePurpose::Execution).await;
    let result = fixture.run("legacy-home", None, None).await;
    assert_eq!(
        result,
        json!({"unlocked":true,"catalogAvailable":true,"currentPreserved":true,"credentialsPreserved":true,"mcpPreserved":true})
    );
    assert!(
        fixture
            .store
            .verify_execution(&fixture.runtime)
            .await
            .is_err()
    );
    assert!(
        fixture
            .store
            .anchor(&fixture.runtime)
            .await
            .unwrap()
            .pending
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires the actual SQLCipher artifact, upstream Node build, and an isolated transactional database"]
async fn killed_writers_reconcile_each_durability_boundary_without_replaying_deliveries() {
    let mut fixture = Fixture::new().await;
    assert_eq!(fixture.run("init", None, None).await["acknowledged"], true);
    for (id, stage, durable) in [
        (1, "prepared", false),
        (2, "durable", true),
        (3, "confirmed", true),
    ] {
        let mut child = fixture.pause(id, stage).await;
        let pending = fixture
            .store
            .anchor(&fixture.runtime)
            .await
            .unwrap()
            .pending;
        if stage != "confirmed" {
            assert_eq!(
                pending.as_ref().unwrap().generation,
                fixture.runtime.generation
            );
        }
        child.kill().await.unwrap();
        fixture.replace().await;
        let resumed = fixture.run("read", None, None).await;
        let rows = resumed["rows"].as_array().unwrap();
        assert_eq!(rows.iter().any(|row| row["id"] == id), durable);
        let reconciled = fixture.store.anchor(&fixture.runtime).await.unwrap();
        assert!(reconciled.pending.is_none());
        if stage == "durable" {
            let pending = pending.unwrap();
            assert_eq!(reconciled.head, pending.next);
            assert!(pending.generation < fixture.runtime.generation);
        }
    }
    assert_eq!(
        fixture.run("read", None, None).await["rows"],
        json!([{"id":2},{"id":3}])
    );
}

#[tokio::test]
#[ignore = "requires the actual SQLCipher artifact, upstream Node build, and an isolated transactional database"]
async fn lost_acknowledgements_and_older_backups_preserve_the_trusted_confirmed_history() {
    let mut fixture = Fixture::new().await;
    fixture.run("init", None, None).await;
    assert_eq!(
        fixture.run("write", Some(1), Some("after_prepare")).await,
        json!({"acknowledged":false,"uncertain":true})
    );
    assert!(
        fixture
            .store
            .anchor(&fixture.runtime)
            .await
            .unwrap()
            .pending
            .is_some()
    );
    fixture.replace().await;
    assert_eq!(fixture.run("read", None, None).await["rows"], json!([]));
    assert_eq!(
        fixture.run("write", Some(2), Some("before_confirm")).await,
        json!({"acknowledged":false,"uncertain":true})
    );
    let pending = fixture
        .store
        .anchor(&fixture.runtime)
        .await
        .unwrap()
        .pending
        .unwrap();
    fixture.replace().await;
    assert_eq!(
        fixture.run("read", None, None).await["rows"],
        json!([{"id":2}])
    );
    assert_eq!(
        fixture.store.anchor(&fixture.runtime).await.unwrap().head,
        pending.next
    );
    let older = std::env::temp_dir().join(format!("gesang-native-backup-{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&older).await.unwrap();
    tokio::fs::copy(
        fixture.dir.join("execution.sqlite"),
        older.join("execution.sqlite"),
    )
    .await
    .unwrap();
    fixture.run("write", Some(3), None).await;
    let output = timeout(
        fixture.deadline,
        fixture
            .command("read", None, None, None, &older)
            .spawn()
            .unwrap()
            .wait_with_output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("\"acknowledged\":true"));
    assert_eq!(
        fixture.run("read", None, None).await["rows"],
        json!([{"id":2},{"id":3}])
    );
}
