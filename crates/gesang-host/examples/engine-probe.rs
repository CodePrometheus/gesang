//! Real execution interoperability probe launched by the upstream service fixture.
#![allow(clippy::unwrap_used, clippy::expect_used)]

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
use gesang_api::{
    CommandAuthorization, CommandReceipt, CommandSubmission, DeliveryState, GrantSelection,
    PermissionMode,
};
use gesang_core::{
    Activity, Admission, Dispatch, EngineBridge, ExecutionResponse, InstanceIntent,
    InstanceObservation, InstancePlan, InstanceProvider, InstancePurpose, Kernel, Owner,
    PayloadKey, PreparationProgress, PreviousWriter, Runtime, RuntimeClaim, RuntimePolicy, Store,
    VerifiedIdentity,
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

#[path = "../tests/support/chat-gateway.rs"]
mod chat_gateway;

#[path = "../tests/support/gateway.rs"]
mod gateway_fixture;

#[path = "engine-probe/goals.rs"]
mod goals;

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

struct FixtureProvider;

#[async_trait]
impl InstanceProvider for FixtureProvider {
    async fn plan(&self, _: &RuntimeClaim) -> gesang_core::Result<InstancePlan> {
        Ok(InstancePlan {
            purpose: InstancePurpose::Execution,
            provider: "interop-fixture".into(),
            schema_version: 1,
            definition: json!({}),
        })
    }

    async fn allocate(&self, intent: &InstanceIntent) -> gesang_core::Result<Admission> {
        if intent.claim.previous.is_some() {
            return Err(gesang_core::Error::FencingRequired);
        }
        Ok(Admission {
            runtime_id: intent.id,
            locator: "isolated-test-service".into(),
            engine_owner: intent.claim.engine_owner.clone(),
            previous_writer: PreviousWriter::Absent,
        })
    }

    async fn activate(&self, _: &InstanceIntent) -> gesang_core::Result<PreparationProgress> {
        Ok(PreparationProgress::ExecutionAvailable)
    }

    async fn observe(&self, _: &Runtime) -> gesang_core::Result<InstanceObservation> {
        Ok(InstanceObservation::Live(Activity::Waiting))
    }

    async fn retain_until(&self, _: &Runtime, _: jiff::Timestamp) -> gesang_core::Result<()> {
        Ok(())
    }

    async fn stop(&self, _: &Runtime) -> gesang_core::Result<()> {
        Err(gesang_core::Error::FencingRequired)
    }
}

async fn wait_for_receipt(
    store: &Store,
    owner: &Owner,
    request_id: Uuid,
    timeout: Duration,
) -> Result<CommandReceipt, Box<dyn Error>> {
    let receipt = tokio::time::timeout(timeout, async {
        loop {
            let delivery = store.receipt(owner, request_id).await?;
            if delivery.state != DeliveryState::Pending {
                return gesang_core::Result::Ok(delivery);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    Ok(receipt)
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
    let adapter = Arc::new(EngineAdapter::new(
        store.clone(),
        connector.clone(),
        EngineConnectionPolicy {
            operation_timeout: timeout,
            idle_timeout: timeout,
        },
    )?);
    let kernel = Arc::new(Kernel::new(
        store.clone(),
        Arc::new(FixtureProvider),
        adapter.clone(),
        RuntimePolicy {
            lease: SignedDuration::from_millis(timeout_millis.try_into()?),
            delivery_lease: SignedDuration::from_millis(timeout_millis.try_into()?),
            retry_delay: SignedDuration::from_millis(20),
            recovery_interval: SignedDuration::from_millis(100),
        },
    )?);
    if let Some(ready) = env::var_os("GESANG_TEST_CHAT_HOST_READY") {
        return chat_gateway::serve(
            &store,
            &owner,
            kernel,
            adapter,
            &grant,
            std::path::Path::new(&ready),
            async {
                let _ = tokio::signal::ctrl_c().await;
            },
        )
        .await;
    }
    let runtime = tokio::time::timeout(timeout, async {
        loop {
            match kernel.ensure_runtime(agent.id).await {
                Err(gesang_core::Error::RuntimeNotReady) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                result => break result,
            }
        }
    })
    .await??;
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
    let lost = CommandSubmission {
        request_id,
        authorization: dispatch.authorization.clone(),
        command: dispatch.command.clone(),
    };
    assert_eq!(
        kernel.submit(&owner, agent.id, &lost).await?.state,
        DeliveryState::Pending
    );
    eprintln!("stage: accept command without recording its delivery acknowledgement");
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
    assert_eq!(
        store.receipt(&owner, request_id).await?.state,
        DeliveryState::Pending
    );
    let (shutdown, stop_receiver) = tokio::sync::watch::channel(false);
    let recovery_kernel = kernel.clone();
    let recovery = tokio::spawn(async move {
        recovery_kernel
            .run_recovery(stop_receiver, |lane, error| eprintln!("{lane:?}: {error}"))
            .await
    });
    let recovered = wait_for_receipt(&store, &owner, request_id, timeout).await?;
    assert_eq!(recovered.state, DeliveryState::Accepted);
    assert_eq!(recovered.engine_receipt.as_ref(), Some(&receipt));
    eprintln!("stage: background recovery matched the original upstream acknowledgement");
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
    goals::verify(&store, &owner, kernel.clone(), adapter.clone(), &goal).await?;
    assert_eq!(connector.connections.load(Ordering::Relaxed), 2);
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
    let stopping = CommandSubmission {
        request_id: Uuid::new_v4(),
        authorization: CommandAuthorization::Owner,
        command: serde_json::to_value(GoalCommand::Stop {
            goal_id: goal_id.into(),
            expected_version: goal_watch.state().await?["snapshot"]["version"]
                .as_u64()
                .unwrap(),
        })?,
    };
    assert_eq!(
        kernel.submit(&owner, agent.id, &stopping).await?.state,
        DeliveryState::Pending
    );
    let stopped = wait_for_receipt(&store, &owner, stopping.request_id, timeout).await?;
    assert_eq!(stopped.state, DeliveryState::Accepted);
    eprintln!("stage: background recovery delivered a new owner control");
    tokio::time::timeout(timeout, async {
        while goal_watch.state().await?["snapshot"]["status"] != "stopped" {
            goal_watch.next().await?;
        }
        gesang_core::Result::Ok(())
    })
    .await??;
    shutdown.send(true)?;
    tokio::time::timeout(timeout, recovery).await???;
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
        json!({"reusedConnection":true,"reconciled":true,"backgroundDelivery":true,"backgroundRecovery":true,"goalRoutes":true,"rejected":true,"retired":true,"staleRejected":true})
    );
    Ok(())
}
