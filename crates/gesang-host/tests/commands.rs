//! Command gateway admission with real credential and coordination transactions.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{collections::BTreeSet, num::NonZeroUsize, str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
    response::Response,
};
use chopin_protocol::ChordClient;
use gesang_api::{CapabilityScope, CommandReceipt, DeliveryState, GrantSelection, PermissionMode};
use gesang_core::{
    CoordinationTransaction, Error, InstanceObservation, InstanceProvider, Kernel, PayloadKey,
    Runtime, RuntimePolicy, Store, VerifiedIdentity,
};
use gesang_host::{
    CommandGateway, CommandGatewayPolicy, EngineAdapter, EngineConnectionPolicy, EngineConnector,
    observe_command_scope, submit_command_scope,
};
use gesang_plugins::PrincipalId;
use gesang_services::{Caller, Credential, CredentialGrant, ServiceAccess};
use jiff::{SignedDuration, Timestamp};
use serde_json::{Value, json};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tower::ServiceExt as _;
use uuid::Uuid;

#[path = "support/gateway.rs"]
mod gateway_fixture;

struct Offline;

#[async_trait]
impl EngineConnector for Offline {
    async fn connect(&self, _: &Runtime) -> gesang_core::Result<ChordClient> {
        Err(Error::RuntimeNotReady)
    }
}

#[async_trait]
impl InstanceProvider for Offline {
    async fn observe(&self, _: &Runtime) -> gesang_core::Result<InstanceObservation> {
        Ok(InstanceObservation::Unknown)
    }

    async fn retain_until(&self, _: &Runtime, _: Timestamp) -> gesang_core::Result<()> {
        Err(Error::RuntimeNotReady)
    }

    async fn stop(&self, _: &Runtime) -> gesang_core::Result<()> {
        Err(Error::RuntimeNotReady)
    }
}

#[derive(Debug)]
struct RevokingPolicy(ServiceAccess);

#[async_trait]
impl CommandGatewayPolicy for RevokingPolicy {
    async fn authorize(
        &self,
        tx: &mut CoordinationTransaction<'_, '_>,
        caller: &Caller,
        scope: &CapabilityScope,
    ) -> gesang_core::Result<()> {
        self.0.revoke(caller.authorization().id).await.unwrap();
        CommandGatewayPolicy::authorize(&self.0, tx, caller, scope).await
    }
}

fn kernel(store: Store) -> Arc<Kernel> {
    let engine = EngineAdapter::new(
        store.clone(),
        Arc::new(Offline),
        EngineConnectionPolicy {
            operation_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(30),
        },
    )
    .unwrap();
    Arc::new(
        Kernel::new(
            store,
            Arc::new(Offline),
            Arc::new(engine),
            RuntimePolicy {
                lease: SignedDuration::from_secs(60),
                delivery_lease: SignedDuration::from_secs(10),
                retry_delay: SignedDuration::from_secs(1),
                recovery_interval: SignedDuration::from_secs(5),
            },
        )
        .unwrap(),
    )
}

async fn call(
    router: &Router,
    path: &str,
    bearer: Option<&Credential>,
    input: Option<&Value>,
) -> Response {
    let mut request = Request::builder().uri(path);
    if let Some(bearer) = bearer {
        request = request.header(header::AUTHORIZATION, format!("Bearer {}", bearer.expose()));
    }
    let body = if let Some(input) = input {
        request = request
            .method("POST")
            .header(header::CONTENT_TYPE, "application/json");
        Body::from(input.to_string())
    } else {
        Body::empty()
    };
    router
        .clone()
        .oneshot(request.body(body).unwrap())
        .await
        .unwrap()
}

async fn read_receipt(response: Response) -> CommandReceipt {
    let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
#[ignore = "requires a dedicated PostgreSQL database with schema creation permission"]
async fn command_route_preserves_the_original_receipt() {
    use sqlx::Executor as _;

    let url = std::env::var("GESANG_TEST_DATABASE_URL").expect("dedicated database required");
    let schema = format!("test_{}", Uuid::new_v4().simple());
    let bootstrap = PgPoolOptions::new().connect(&url).await.unwrap();
    bootstrap
        .execute(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .await
        .unwrap();
    bootstrap.close().await;
    let db_options = PgConnectOptions::from_str(&url)
        .unwrap()
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .connect_with(db_options.clone())
        .await
        .unwrap();
    let store = Store::new(pool, PayloadKey::new([42; 32]));
    store.migrate().await.unwrap();
    let fixture = gateway_fixture::configure(&store).await;
    let credentials = fixture.credentials.clone();
    let binding = fixture.binding;
    let identity = |subject: &str| VerifiedIdentity {
        issuer: "command-gateway-test".into(),
        subject: subject.into(),
    };
    let owner = store.bind_owner(&identity("owner")).await.unwrap();
    let foreign = store.bind_owner(&identity("foreign")).await.unwrap();
    let agent = store.ensure_agent(&owner, "chopin", "owner").await.unwrap();
    let expires_at = Timestamp::now() + SignedDuration::from_mins(5);
    let credential = |owner_id, scopes: BTreeSet<CapabilityScope>| CredentialGrant {
        binding,
        principal: PrincipalId::Owner { owner_id },
        scopes,
        expires_at,
    };
    let scopes = BTreeSet::from([
        submit_command_scope(agent.id),
        observe_command_scope(agent.id),
    ]);
    let (_, bearer) = credentials
        .issue(&credential(owner.id, scopes.clone()))
        .await
        .unwrap();
    let (_, foreign_bearer) = credentials
        .issue(&credential(foreign.id, scopes.clone()))
        .await
        .unwrap();
    let (_, submit_only) = credentials
        .issue(&credential(
            owner.id,
            BTreeSet::from([submit_command_scope(agent.id)]),
        ))
        .await
        .unwrap();
    let (_, revoked) = credentials
        .issue(&credential(owner.id, scopes))
        .await
        .unwrap();
    let router = |store: Store, policy: Arc<dyn CommandGatewayPolicy>| {
        let gateway = CommandGateway::new(
            kernel(store),
            fixture.catalog.clone(),
            fixture.graph.clone(),
            fixture.administrator.clone(),
            fixture.host.clone(),
            policy,
        )
        .unwrap();
        Router::new().nest("/api", gateway.router(NonZeroUsize::new(4096).unwrap()))
    };
    let primary = router(store.clone(), Arc::new(credentials.clone()));
    let request_id = Uuid::new_v4();
    let request_path = format!("/api/v1/requests/{request_id}");
    let submit_path = format!("/api/v1/agents/{}/commands", agent.id);
    let grant = store
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
    let request = json!({
        "requestId": request_id,
        "authorization": { "kind":"grant", "id":grant.id, "version":grant.version },
        "command": { "kind":"goal.submit", "objective":"Run the example job.", "completionCriteria":[],
                     "grant":{"id":grant.id,"version":grant.version} },
    });

    assert_eq!(
        call(&primary, &submit_path, None, Some(&request))
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &primary,
            &submit_path,
            Some(&foreign_bearer),
            Some(&request)
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    let mut owner_only = request.clone();
    owner_only["authorization"] = json!({"kind": "owner"});
    assert_eq!(
        call(&primary, &submit_path, Some(&bearer), Some(&owner_only))
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    let mut malformed = request.clone();
    malformed["command"]["grant"] = Value::Null;
    assert_eq!(
        call(&primary, &submit_path, Some(&bearer), Some(&malformed))
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    let revoked_router = router(store.clone(), Arc::new(RevokingPolicy(credentials.clone())));
    assert_eq!(
        call(
            &revoked_router,
            &submit_path,
            Some(&revoked),
            Some(&request)
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&primary, &request_path, Some(&bearer), None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );

    let accepted = call(&primary, &submit_path, Some(&bearer), Some(&request)).await;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    assert_eq!(accepted.headers()[header::LOCATION], request_path);
    assert_eq!(accepted.headers()[header::CACHE_CONTROL], "no-store");
    let original = read_receipt(accepted).await;
    assert_eq!(original.state, DeliveryState::Pending);
    assert_eq!(original.request_id, request_id);
    assert_eq!(
        original,
        read_receipt(call(&primary, &submit_path, Some(&bearer), Some(&request)).await).await
    );
    let mut changed = request.clone();
    changed["command"]["objective"] = json!("Altered content.");
    assert_eq!(
        call(&primary, &submit_path, Some(&bearer), Some(&changed))
            .await
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(&primary, &request_path, Some(&foreign_bearer), None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&primary, &request_path, Some(&submit_only), None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );

    let reopened = PgPoolOptions::new().connect_with(db_options).await.unwrap();
    let reopened = router(
        Store::new(reopened, PayloadKey::new([42; 32])),
        Arc::new(credentials),
    );
    let found = call(&reopened, &request_path, Some(&bearer), None).await;
    assert_eq!(found.status(), StatusCode::OK);
    assert_eq!(read_receipt(found).await, original);
}
