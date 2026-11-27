//! Authenticated access to execution-owned goals, conversations, and artifacts.

use std::{
    fmt::Write as _,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderName, HeaderValue, header},
    middleware,
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chopin_protocol::{ConversationState, GoalPage, GoalSnapshot, PublishedArtifact};
use gesang_api::CapabilityScope;
use gesang_core::{ConfigurationAccess, Error, Kernel, PluginCatalog, Runtime};
use gesang_plugins::{Authority, Graph};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::{
    ArtifactDownload, ArtifactRead, EngineAdapter,
    routes::{ApiFailure, HostAccess, no_store},
};

/// Permission to read an agent's execution records and original published files.
#[must_use]
pub fn observe_goal_scope(agent_id: Uuid) -> CapabilityScope {
    CapabilityScope {
        capability: "goal.observe".into(),
        resource: agent_id.to_string(),
    }
}

/// Host-supplied bounds for complete, verified file responses.
#[derive(Clone, Copy, Debug)]
pub struct ArtifactDownloadPolicy {
    /// Maximum bytes allowed in an individual file.
    pub max_file_bytes: NonZeroUsize,
    /// Maximum files being verified or held by HTTP response bodies.
    pub concurrent_downloads: NonZeroUsize,
}

/// Owner-authenticated execution reads for standalone and embedded hosts.
#[derive(Clone, Debug)]
pub struct GoalGateway {
    kernel: Arc<Kernel>,
    engine: Arc<EngineAdapter>,
    access: HostAccess,
    max_file_bytes: NonZeroUsize,
    downloads: Arc<Semaphore>,
}

impl GoalGateway {
    /// Uses the same kernel and execution adapter as command delivery.
    ///
    /// The host supplies TLS and owns the kernel recovery loop.
    /// # Errors
    /// Returns an error if the host identity is owner scoped or the download
    /// concurrency exceeds the semaphore implementation's capacity.
    pub fn new(
        kernel: Arc<Kernel>,
        engine: Arc<EngineAdapter>,
        catalog: PluginCatalog,
        graph: Graph,
        configuration: ConfigurationAccess,
        host: Authority,
        policy: ArtifactDownloadPolicy,
    ) -> gesang_services::Result<Self> {
        if policy.concurrent_downloads.get() > Semaphore::MAX_PERMITS {
            return Err(gesang_services::Error::Invalid(
                "download concurrency exceeds the semaphore capacity".into(),
            ));
        }
        Ok(Self {
            kernel,
            engine,
            access: HostAccess::new(catalog, graph, configuration, host)?,
            max_file_bytes: policy.max_file_bytes,
            downloads: Arc::new(Semaphore::new(policy.concurrent_downloads.get())),
        })
    }

    /// Builds the goal reads to mount alongside the command routes.
    pub fn router(self) -> Router {
        Router::new()
            .route("/v1/agents/{agent_id}/goals", get(list))
            .route("/v1/agents/{agent_id}/goals/{goal_id}", get(snapshot))
            .route(
                "/v1/agents/{agent_id}/goals/{goal_id}/conversation",
                get(conversation),
            )
            .route(
                "/v1/agents/{agent_id}/goals/{goal_id}/artifacts",
                get(artifacts),
            )
            .route(
                "/v1/agents/{agent_id}/goals/{goal_id}/artifacts/{artifact_id}",
                get(download),
            )
            .layer(middleware::map_response(no_store))
            .with_state(self)
    }

    async fn runtime(&self, headers: &HeaderMap, agent_id: Uuid) -> Result<Runtime, ApiFailure> {
        let (owner, caller) = self.access.owner(headers).await?;
        if !caller
            .authority()
            .capabilities()
            .contains(&observe_goal_scope(agent_id))
        {
            return Err(ApiFailure::not_found());
        }
        Ok(self.kernel.ensure_owner_runtime(&owner, agent_id).await?)
    }
}

#[derive(Deserialize)]
struct PageQuery {
    limit: NonZeroU64,
    cursor: Option<String>,
}

async fn list(
    State(gateway): State<GoalGateway>,
    headers: HeaderMap,
    Path(agent_id): Path<Uuid>,
    Query(query): Query<PageQuery>,
) -> Result<Json<GoalPage>, ApiFailure> {
    let cursor = query
        .cursor
        .map(|cursor| serde_json::from_str::<serde_json::Map<String, Value>>(&cursor))
        .transpose()
        .map_err(|_| ApiFailure::invalid())?
        .map(Value::Object);
    let runtime = gateway.runtime(&headers, agent_id).await?;
    Ok(Json(
        gateway
            .engine
            .goals(&runtime, query.limit, cursor)
            .await
            .map_err(|error| match error {
                Error::InvalidCommand(_) => ApiFailure::invalid(),
                _ => error.into(),
            })?,
    ))
}

async fn snapshot(
    State(gateway): State<GoalGateway>,
    headers: HeaderMap,
    Path((agent_id, goal_id)): Path<(Uuid, String)>,
) -> Result<Json<GoalSnapshot>, ApiFailure> {
    let runtime = gateway.runtime(&headers, agent_id).await?;
    Ok(Json(gateway.engine.goal(&runtime, &goal_id).await?))
}

async fn conversation(
    State(gateway): State<GoalGateway>,
    headers: HeaderMap,
    Path((agent_id, goal_id)): Path<(Uuid, String)>,
) -> Result<Json<ConversationState>, ApiFailure> {
    let runtime = gateway.runtime(&headers, agent_id).await?;
    let goal = gateway.engine.goal(&runtime, &goal_id).await?;
    Ok(Json(
        gateway
            .engine
            .conversation(&runtime, goal.conversation_id)
            .await?,
    ))
}

#[derive(Serialize)]
struct Artifacts {
    artifacts: Vec<PublishedArtifact>,
}

async fn artifacts(
    State(gateway): State<GoalGateway>,
    headers: HeaderMap,
    Path((agent_id, goal_id)): Path<(Uuid, String)>,
) -> Result<Json<Artifacts>, ApiFailure> {
    let runtime = gateway.runtime(&headers, agent_id).await?;
    Ok(Json(Artifacts {
        artifacts: gateway.engine.goal_artifacts(&runtime, &goal_id).await?,
    }))
}

struct DownloadBody {
    bytes: Vec<u8>,
    _slot: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for DownloadBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

async fn download(
    State(gateway): State<GoalGateway>,
    headers: HeaderMap,
    Path((agent_id, goal_id, artifact_id)): Path<(Uuid, String, Uuid)>,
) -> Result<Response, ApiFailure> {
    let runtime = gateway.runtime(&headers, agent_id).await?;
    let slot = gateway
        .downloads
        .try_acquire_owned()
        .map_err(|_| ApiFailure::unavailable())?;
    let file = match gateway
        .engine
        .goal_artifact(&runtime, &goal_id, artifact_id, gateway.max_file_bytes)
        .await?
    {
        ArtifactRead::Complete(file) => file,
        ArtifactRead::Missing => return Err(ApiFailure::not_found()),
        ArtifactRead::TooLarge => return Err(ApiFailure::artifact_too_large()),
    };
    attachment(file, slot)
}

fn attachment(file: ArtifactDownload, slot: OwnedSemaphorePermit) -> Result<Response, ApiFailure> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        disposition(&file.metadata.name)?,
    );
    headers.insert(
        HeaderName::from_static("content-digest"),
        HeaderValue::from_str(&format!("sha-256=:{}:", STANDARD.encode(file.digest)))
            .map_err(|_| ApiFailure::unavailable())?,
    );
    let body = Bytes::from_owner(DownloadBody {
        bytes: file.bytes,
        _slot: slot,
    });
    Ok((headers, body).into_response())
}

fn disposition(name: &str) -> Result<HeaderValue, ApiFailure> {
    let mut value = String::from("attachment; filename*=UTF-8''");
    for byte in name.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                value.push(char::from(byte));
            }
            _ => write!(&mut value, "%{byte:02X}").map_err(|_| ApiFailure::unavailable())?,
        }
    }
    HeaderValue::from_str(&value).map_err(|_| ApiFailure::unavailable())
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use sha2::{Digest, Sha256};

    use super::*;

    #[tokio::test]
    async fn attachment_keeps_its_download_slot() -> Result<(), &'static str> {
        let downloads = Arc::new(Semaphore::new(1));
        let slot = downloads
            .clone()
            .try_acquire_owned()
            .map_err(|_| "first download should have a slot")?;
        let bytes = vec![0, 128, 255];
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        let file = ArtifactDownload {
            metadata: PublishedArtifact {
                artifact_id: Uuid::new_v4(),
                goal_id: "goal".into(),
                operation_id: Uuid::new_v4(),
                name: "résumé.bin".into(),
                sha256: "5240672d7b51756b829ad0ef8d9468b7a078afa2f410484fd3892dab47becb72".into(),
                size_bytes: 3,
            },
            bytes: bytes.clone(),
            digest,
        };
        let response =
            attachment(file, slot).map_err(|_| "verified file should be downloadable")?;
        assert_eq!(
            response.headers()[header::CONTENT_DISPOSITION],
            "attachment; filename*=UTF-8''r%C3%A9sum%C3%A9.bin"
        );
        assert_eq!(
            response.headers()["content-digest"],
            format!("sha-256=:{}:", STANDARD.encode(digest))
        );
        assert!(downloads.clone().try_acquire_owned().is_err());
        let data = to_bytes(response.into_body(), bytes.len())
            .await
            .map_err(|_| "attachment should be readable")?;
        assert_eq!(data.as_ref(), bytes);
        drop(data);
        assert!(downloads.try_acquire_owned().is_ok());
        Ok(())
    }
}
