//! Exercise the public owner routes against the probe's real execution service.

use std::{collections::BTreeSet, num::NonZeroUsize, sync::Arc};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
    response::Response,
};
use chopin_protocol::GoalSnapshot;
use gesang_core::{Kernel, Owner, Store, VerifiedIdentity};
use gesang_host::{ArtifactDownloadPolicy, EngineAdapter, GoalGateway, observe_goal_scope};
use gesang_plugins::PrincipalId;
use gesang_services::{Credential, CredentialGrant};
use jiff::{SignedDuration, Timestamp};
use serde_json::Value;
use tower::ServiceExt as _;
use uuid::Uuid;

use crate::gateway_fixture;

pub(super) async fn verify(
    store: &Store,
    owner: &Owner,
    kernel: Arc<Kernel>,
    engine: Arc<EngineAdapter>,
    goal: &GoalSnapshot,
) -> Result<(), Box<dyn std::error::Error>> {
    let fixture = gateway_fixture::configure(store).await;
    let agent_id = store.session(owner).await?.agent.id;
    let other = store
        .bind_owner(&VerifiedIdentity {
            issuer: "interop-fixture".into(),
            subject: "other".into(),
        })
        .await?;
    let grant = |owner_id, scopes| CredentialGrant {
        binding: fixture.binding,
        principal: PrincipalId::Owner { owner_id },
        scopes,
        expires_at: Timestamp::now() + SignedDuration::from_mins(5),
    };
    let scopes = BTreeSet::from([observe_goal_scope(agent_id)]);
    let (authorization, bearer) = fixture
        .credentials
        .issue(&grant(owner.id, scopes.clone()))
        .await?;
    let (_, foreign) = fixture.credentials.issue(&grant(other.id, scopes)).await?;
    let (_, unscoped) = fixture
        .credentials
        .issue(&grant(
            owner.id,
            BTreeSet::from([gesang_host::observe_command_scope(agent_id)]),
        ))
        .await?;
    let router = GoalGateway::new(
        kernel,
        engine,
        fixture.catalog,
        fixture.graph,
        fixture.administrator,
        fixture.host,
        ArtifactDownloadPolicy {
            max_file_bytes: NonZeroUsize::new(4096).unwrap(),
            concurrent_downloads: NonZeroUsize::MIN,
        },
    )?
    .router();
    let goals = format!("/v1/agents/{agent_id}/goals");
    let original = format!("{goals}/{}", goal.goal_id);
    assert_eq!(
        get(&router, &original, None).await.status(),
        StatusCode::UNAUTHORIZED
    );
    for denied in [&foreign, &unscoped] {
        assert_eq!(
            get(&router, &original, Some(denied)).await.status(),
            StatusCode::NOT_FOUND
        );
    }
    let missing = format!("{goals}/{}", Uuid::new_v4());
    assert_eq!(
        get(&router, &missing, Some(&bearer)).await.status(),
        StatusCode::NOT_FOUND
    );
    let snapshot = json(get(&router, &original, Some(&bearer)).await).await;
    assert_eq!(snapshot["goalId"], goal.goal_id);
    assert_eq!(snapshot["conversationId"], goal.conversation_id);
    let conversation =
        json(get(&router, &format!("{original}/conversation"), Some(&bearer)).await).await;
    assert_eq!(
        conversation["view"]["conversation"]["id"],
        goal.conversation_id
    );
    assert!(conversation["view"]["entries"].is_array());
    let artifacts = format!("{original}/artifacts");
    let publications = json(get(&router, &artifacts, Some(&bearer)).await).await;
    assert_eq!(publications["artifacts"], serde_json::json!([]));
    assert_eq!(
        get(
            &router,
            &format!("{artifacts}/{}", Uuid::new_v4()),
            Some(&bearer)
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    fixture.credentials.revoke(authorization.id).await?;
    assert_eq!(
        get(&router, &original, Some(&bearer)).await.status(),
        StatusCode::UNAUTHORIZED
    );
    eprintln!(
        "stage: authenticated goal and conversation routes returned the original execution identities"
    );
    Ok(())
}

async fn get(router: &Router, path: &str, credential: Option<&Credential>) -> Response {
    let mut request = Request::builder().uri(path);
    if let Some(credential) = credential {
        request = request.header(
            header::AUTHORIZATION,
            format!("Bearer {}", credential.expose()),
        );
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    response
}

async fn json(response: Response) -> Value {
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
