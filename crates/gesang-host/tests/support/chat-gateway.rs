//! Start the real owner gateways and custody-independent recovery under test control.

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
use std::{
    collections::BTreeSet, future::Future, io::Write as _, num::NonZeroUsize, path::Path, sync::Arc,
};

use gesang_api::Grant;
use gesang_core::{Kernel, Owner, Store};
use gesang_host::{
    ArtifactDownloadPolicy, CommandGateway, EngineAdapter, GoalGateway, observe_command_scope,
    observe_goal_scope, submit_command_scope,
};
use gesang_plugins::PrincipalId;
use gesang_services::CredentialGrant;
use jiff::{SignedDuration, Timestamp};

use crate::gateway_fixture;

pub(super) async fn serve(
    store: &Store,
    owner: &Owner,
    kernel: Arc<Kernel>,
    engine: Arc<EngineAdapter>,
    grant: &Grant,
    ready: &Path,
    stop: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    let fixture = gateway_fixture::configure(store).await;
    let agent = store.session(owner).await?.agent;
    let (_, bearer) = fixture
        .credentials
        .issue(&CredentialGrant {
            binding: fixture.binding,
            principal: PrincipalId::Owner { owner_id: owner.id },
            scopes: BTreeSet::from([
                submit_command_scope(agent.id),
                observe_command_scope(agent.id),
                observe_goal_scope(agent.id),
            ]),
            expires_at: Timestamp::now() + SignedDuration::from_mins(15),
        })
        .await?;
    let commands = CommandGateway::new(
        kernel.clone(),
        fixture.catalog.clone(),
        fixture.graph.clone(),
        fixture.administrator.clone(),
        fixture.host.clone(),
        Arc::new(fixture.credentials),
    )?
    .router(NonZeroUsize::new(32 * 1024).unwrap());
    let goals = GoalGateway::new(
        kernel.clone(),
        engine.clone(),
        fixture.catalog,
        fixture.graph,
        fixture.administrator,
        fixture.host,
        ArtifactDownloadPolicy {
            max_file_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            concurrent_downloads: NonZeroUsize::MIN,
        },
    )?
    .router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let token_path = ready.with_extension("token");
    let mut token = std::fs::OpenOptions::new();
    token.write(true).create_new(true);
    #[cfg(unix)]
    token.mode(0o600);
    token
        .open(&token_path)?
        .write_all(bearer.expose().as_bytes())?;
    std::fs::write(
        ready,
        serde_json::to_vec(&serde_json::json!({
            "url": format!("http://{}/", listener.local_addr()?),
            "agent": agent.id,
            "grant": grant.id,
            "version": grant.version,
            "tokenFile": token_path,
        }))?,
    )?;

    let (shutdown, stopped) = tokio::sync::watch::channel(false);
    let recovery = tokio::spawn(async move {
        kernel
            .run_recovery(stopped, |lane, error| eprintln!("{lane:?}: {error}"))
            .await
    });
    let result = axum::serve(listener, commands.merge(goals))
        .with_graceful_shutdown(async move {
            stop.await;
            let _ = shutdown.send(true);
        })
        .await;
    recovery.await??;
    engine.shutdown().await?;
    result?;
    Ok(())
}
