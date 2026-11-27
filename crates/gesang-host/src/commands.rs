//! Authenticated command admission and receipt lookup for application hosts.

use std::{num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, OriginalUri, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use gesang_api::{CapabilityScope, CommandReceipt, CommandSubmission, DeliveryState};
use gesang_core::{
    CommandAdmissionCheck, ConfigurationAccess, CoordinationTransaction, Error, Kernel,
    PluginCatalog,
};
use gesang_plugins::{Authority, Graph};
use gesang_services::{Caller, ServiceAccess};
use uuid::Uuid;

use crate::routes::{ApiFailure, HostAccess, no_store};

/// Permission to admit commands for the specified agent, subject to its current delegation.
#[must_use]
pub fn submit_command_scope(agent_id: Uuid) -> CapabilityScope {
    CapabilityScope {
        capability: "command.submit".into(),
        resource: agent_id.to_string(),
    }
}

/// Permission to look up the specified agent's durable command delivery receipts.
#[must_use]
pub fn observe_command_scope(agent_id: Uuid) -> CapabilityScope {
    CapabilityScope {
        capability: "command.observe".into(),
        resource: agent_id.to_string(),
    }
}

/// Joins the host credential check to the command's admission transaction.
#[async_trait]
pub trait CommandGatewayPolicy: std::fmt::Debug + Send + Sync {
    /// Rechecks the caller and exact capability before admission.
    async fn authorize(
        &self,
        tx: &mut CoordinationTransaction<'_, '_>,
        caller: &Caller,
        capability: &CapabilityScope,
    ) -> gesang_core::Result<()>;
}

#[async_trait]
impl CommandGatewayPolicy for ServiceAccess {
    async fn authorize(
        &self,
        tx: &mut CoordinationTransaction<'_, '_>,
        caller: &Caller,
        capability: &CapabilityScope,
    ) -> gesang_core::Result<()> {
        let CoordinationTransaction::Postgres(tx) = tx else {
            return Err(Error::StorageBackend);
        };
        self.authorize_in(tx, caller, capability)
            .await
            .map(|_| ())
            .map_err(|error| match error {
                gesang_services::Error::Forbidden | gesang_services::Error::Unauthorized => {
                    Error::Forbidden
                }
                gesang_services::Error::Platform(error) => error,
                _ => Error::Uncertain("command credential authority is unavailable".into()),
            })
    }
}

struct Admission<'a> {
    policy: &'a dyn CommandGatewayPolicy,
    caller: Caller,
    scope: CapabilityScope,
}

#[async_trait]
impl CommandAdmissionCheck for Admission<'_> {
    async fn authorize(&self, tx: &mut CoordinationTransaction<'_, '_>) -> gesang_core::Result<()> {
        self.policy.authorize(tx, &self.caller, &self.scope).await
    }
}

/// Owner-authenticated routes for an embedded or standalone command gateway.
#[derive(Clone, Debug)]
pub struct CommandGateway {
    kernel: Arc<Kernel>,
    access: HostAccess,
    policy: Arc<dyn CommandGatewayPolicy>,
}

impl CommandGateway {
    /// Binds an existing kernel to a trusted host identity and credential provider.
    ///
    /// The same kernel's delivery loop forwards admitted requests to the execution service.
    /// The host supplies TLS and owns the lifetime of that loop.
    ///
    /// # Errors
    /// Returns an error if the host identity has owner-scoped authority.
    pub fn new(
        kernel: Arc<Kernel>,
        catalog: PluginCatalog,
        graph: Graph,
        configuration: ConfigurationAccess,
        host: Authority,
        policy: Arc<dyn CommandGatewayPolicy>,
    ) -> gesang_services::Result<Self> {
        Ok(Self {
            kernel,
            access: HostAccess::new(catalog, graph, configuration, host)?,
            policy,
        })
    }

    /// Builds bounded admission and receipt routes.
    pub fn router(self, max_body_bytes: NonZeroUsize) -> Router {
        Router::new()
            .route("/v1/agents/{agent_id}/commands", post(submit))
            .route("/v1/requests/{request_id}", get(receipt))
            .layer(DefaultBodyLimit::max(max_body_bytes.get()))
            .layer(middleware::map_response(no_store))
            .with_state(self)
    }
}

async fn submit(
    State(gateway): State<CommandGateway>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Path(agent_id): Path<Uuid>,
    Json(request): Json<CommandSubmission>,
) -> Result<Response, ApiFailure> {
    let scope = submit_command_scope(agent_id);
    let (owner, caller) = gateway.access.owner(&headers).await?;
    if !caller.authority().capabilities().contains(&scope) {
        return Err(ApiFailure::forbidden());
    }
    let admission = Admission {
        policy: &*gateway.policy,
        caller,
        scope,
    };
    let receipt = gateway
        .kernel
        .submit_checked(&owner, agent_id, &request, &admission)
        .await
        .map_err(|error| match error {
            Error::InvalidCommand(_) => ApiFailure::invalid(),
            _ => error.into(),
        })?;
    let (version_root, _) = uri
        .path()
        .rsplit_once("/agents/")
        .ok_or_else(ApiFailure::unavailable)?;
    let location =
        HeaderValue::from_str(&format!("{version_root}/requests/{}", receipt.request_id))
            .map_err(|_| ApiFailure::unavailable())?;
    let status = match receipt.state {
        DeliveryState::Pending => StatusCode::ACCEPTED,
        DeliveryState::Accepted | DeliveryState::Rejected => StatusCode::OK,
    };
    Ok((status, [(header::LOCATION, location)], Json(receipt)).into_response())
}

async fn receipt(
    State(gateway): State<CommandGateway>,
    headers: HeaderMap,
    Path(request_id): Path<Uuid>,
) -> Result<Json<CommandReceipt>, ApiFailure> {
    let (owner, caller) = gateway.access.owner(&headers).await?;
    let receipt = gateway.kernel.store().receipt(&owner, request_id).await?;
    if !caller
        .authority()
        .capabilities()
        .contains(&observe_command_scope(receipt.agent_id))
    {
        return Err(ApiFailure::not_found());
    }
    Ok(Json(receipt))
}
