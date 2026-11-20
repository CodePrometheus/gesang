//! Real execution interoperability probe launched by the upstream service fixture.
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "../../gesang-core/test-support.rs"]
mod support;
use support::ReadyFixture;

use std::{
    env,
    error::Error,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chopin_protocol::{ChordClient, GoalCommand, GoalGrant};
use gesang_api::{CommandAuthorization, GrantSelection, PermissionMode};
use gesang_core::{
    Admission, Dispatch, EngineBridge, ExecutionResponse, PayloadKey, PreviousWriter, Runtime,
    Store, VerifiedIdentity,
};
use gesang_host::{EngineAdapter, EngineConnectionPolicy, EngineConnector};
use jiff::SignedDuration;
use serde_json::json;
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;
use zeroize::Zeroizing;

struct Connector {
    url: String,
    credential: Zeroizing<String>,
    connections: AtomicUsize,
}

#[async_trait]
impl EngineConnector for Connector {
    async fn connect(&self, _: &Runtime) -> gesang_core::Result<ChordClient> {
        self.connections.fetch_add(1, Ordering::Relaxed);
        ChordClient::connect(&self.url, &self.credential)
            .await
            .map_err(|_| gesang_core::Error::Uncertain("test execution connection failed".into()))
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn Error>> {
    let database = env::var("GESANG_TEST_DATABASE_URL")?;
    let bootstrap = PgPoolOptions::new().connect(&database).await?;
    let schema = format!("test_{}", Uuid::new_v4().simple());
    bootstrap
        .execute(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .await?;
    let pool = PgPoolOptions::new()
        .connect_with(
            PgConnectOptions::from_str(&database)?.options([("search_path", schema.as_str())]),
        )
        .await?;
    bootstrap.close().await;
    let store = Store::new(pool, PayloadKey::new([42; 32]));
    store.migrate().await?;
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "interop-fixture".into(),
            subject: "person".into(),
        })
        .await?;
    let agent = store.ensure_agent(&owner, "execution-v1", "local").await?;
    let timeout_millis: u64 = env::var("GESANG_TEST_OPERATION_TIMEOUT_MS")?.parse()?;
    let claim = store
        .claim_runtime(
            agent.id,
            Uuid::new_v4(),
            SignedDuration::from_millis(timeout_millis.try_into()?),
        )
        .await?;
    let runtime = store
        .admit_ready_fixture(
            &claim,
            &Admission {
                runtime_id: Uuid::new_v4(),
                locator: "isolated-test-service".into(),
                engine_owner: "local".into(),
                previous_writer: PreviousWriter::Absent,
            },
        )
        .await?;
    let grant = store
        .select_grant(
            &owner,
            agent.id,
            &GrantSelection {
                mode: PermissionMode::FullAccess,
                scopes: vec![],
            },
        )
        .await?;
    let connector = Arc::new(Connector {
        url: env::var("CHOPIN_TEST_URL")?,
        credential: Zeroizing::new(env::var("CHOPIN_TEST_CREDENTIAL")?),
        connections: AtomicUsize::new(0),
    });
    let timeout = Duration::from_millis(timeout_millis);
    let adapter = EngineAdapter::new(
        store.clone(),
        connector.clone(),
        EngineConnectionPolicy {
            operation_timeout: timeout,
            idle_timeout: timeout,
        },
    )?;
    let request_id = Uuid::new_v4();
    eprintln!("stage: query missing receipt");
    assert!(adapter.request(&runtime, request_id).await?.is_none());
    let command = GoalCommand::submit(
        "Wait for an explicit result format".into(),
        vec![],
        GoalGrant {
            id: grant.id.to_string(),
            version: grant.version.try_into()?,
        },
    );
    let dispatch = Dispatch {
        request_id,
        runtime: runtime.clone(),
        authorization: CommandAuthorization::Grant {
            id: grant.id,
            version: grant.version,
        },
        command: serde_json::to_value(command)?,
    };
    eprintln!("stage: accept command");
    let ExecutionResponse::Accepted(receipt) = adapter.execute(&dispatch).await? else {
        panic!("expected acceptance");
    };
    eprintln!("stage: reconcile and repeat");
    let ExecutionResponse::Accepted(repeated) = adapter.execute(&dispatch).await? else {
        panic!("expected repeated acceptance");
    };
    assert_eq!(receipt, repeated);
    assert_eq!(
        adapter.request(&runtime, request_id).await?,
        Some(receipt.clone())
    );
    let goal_id = receipt["goalId"].as_str().unwrap();
    eprintln!("stage: snapshot and scheduler");
    assert_eq!(
        adapter.snapshot(&runtime, goal_id).await?["goalId"],
        goal_id
    );
    adapter.wake(&runtime).await?;
    adapter.resume(&runtime).await?;
    let goal = adapter.goal(&runtime, goal_id).await?;
    eprintln!("stage: query goal pages");
    let mut cursor = None;
    let mut found = false;
    loop {
        let page = adapter
            .goals(&runtime, std::num::NonZeroU64::MIN, cursor)
            .await?;
        found |= page.goals.iter().any(|item| item.goal_id == goal_id);
        cursor = page.cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert!(found);
    eprintln!("stage: query conversation");
    let conversation = adapter.conversation(&runtime, goal.conversation_id).await?;
    assert_eq!(conversation.view.conversation.id, goal.conversation_id);
    let associated = adapter
        .goal_for_conversation(&runtime, goal.conversation_id)
        .await?;
    assert_eq!(
        associated.as_ref().map(|item| item.goal_id.as_str()),
        Some(goal_id)
    );
    eprintln!("stage: query history");
    let entries = adapter
        .entries(
            &runtime,
            goal.conversation_id,
            std::num::NonZeroU64::MIN,
            None,
            None,
        )
        .await?;
    assert!(entries.entries.len() <= 1);
    eprintln!("stage: query legacy summaries and model catalog");
    assert!(
        adapter
            .summaries(&runtime)
            .await?
            .iter()
            .any(|item| item.id == goal.conversation_id)
    );
    assert!(!adapter.models(&runtime).await?.models.is_empty());
    assert_eq!(connector.connections.load(Ordering::Relaxed), 1);
    let stale = Dispatch {
        request_id: Uuid::new_v4(),
        runtime: runtime.clone(),
        authorization: CommandAuthorization::Owner,
        command: serde_json::to_value(GoalCommand::Stop {
            goal_id: goal_id.into(),
            expected_version: (1u64 << 53) - 1,
        })?,
    };
    eprintln!("stage: definitive rejection");
    let ExecutionResponse::Rejected(problem) = adapter.execute(&stale).await? else {
        panic!("expected a definitive stale-goal rejection");
    };
    assert_eq!(problem.code, "goal_conflict");
    eprintln!("stage: observe actual goal changes");
    let mut goal_watch = adapter.observe_goal(&runtime, goal_id).await?;
    let conversation_watch = adapter
        .observe_conversation(&runtime, goal.conversation_id)
        .await?;
    assert_eq!(
        conversation_watch.state().await?["view"]["conversation"]["id"],
        goal.conversation_id
    );
    tokio::time::timeout(timeout, async {
        while goal_watch.state().await?["snapshot"]["status"] != "waiting_user" {
            goal_watch.next().await?;
        }
        gesang_core::Result::Ok(())
    })
    .await??;
    let stopping = Dispatch {
        request_id: Uuid::new_v4(),
        runtime: runtime.clone(),
        authorization: CommandAuthorization::Owner,
        command: serde_json::to_value(GoalCommand::Stop {
            goal_id: goal_id.into(),
            expected_version: goal_watch.state().await?["snapshot"]["version"]
                .as_u64()
                .unwrap(),
        })?,
    };
    assert!(matches!(
        adapter.execute(&stopping).await?,
        ExecutionResponse::Accepted(_)
    ));
    tokio::time::timeout(timeout, async {
        while goal_watch.state().await?["snapshot"]["status"] != "stopped" {
            goal_watch.next().await?;
        }
        gesang_core::Result::Ok(())
    })
    .await??;
    eprintln!("stage: retire and fence");
    let mut idle_watch = adapter.observe_goal(&runtime, goal_id).await?;
    let (interrupted, retired) = tokio::join!(idle_watch.next(), adapter.retire(&runtime));
    assert!(matches!(
        interrupted,
        Err(gesang_core::Error::StaleGeneration)
    ));
    retired?;
    idle_watch.close().await?;
    assert!(matches!(
        goal_watch.state().await,
        Err(gesang_core::Error::StaleGeneration)
    ));
    assert!(matches!(
        conversation_watch.state().await,
        Err(gesang_core::Error::StaleGeneration)
    ));
    goal_watch.close().await?;
    conversation_watch.close().await?;
    let before = connector.connections.load(Ordering::Relaxed);
    assert_eq!(adapter.request(&runtime, request_id).await?, Some(receipt));
    assert_eq!(connector.connections.load(Ordering::Relaxed), before + 1);
    sqlx::query(
        "UPDATE gesang_agents SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(agent.id)
    .execute(store.pool())
    .await?;
    assert!(matches!(
        adapter.wake(&runtime).await,
        Err(gesang_core::Error::StaleGeneration)
    ));
    adapter.shutdown().await?;
    assert!(adapter.request(&runtime, request_id).await.is_err());
    println!(
        "{}",
        json!({"reusedConnection":true,"reconciled":true,"rejected":true,"retired":true,"staleRejected":true})
    );
    Ok(())
}
