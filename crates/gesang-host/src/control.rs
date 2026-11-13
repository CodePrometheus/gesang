use std::{num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::post,
};
use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use chrono::{DateTime, TimeDelta};
use gesang_api::{ApiProblem, CapabilityScope};
use gesang_core::{
    AgentBinding, Anchor, AnchorHead, Custody, CustodyReceipt, Error, Initialization,
    InitializationState, InstancePhase, InstancePurpose, Owner, PendingAnchor, Result, Runtime,
    Store, Wake,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::ControlKey;

/// New execution whose host ceiling must be checked independently of task delegation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionAction {
    /// Admission of a new goal or replacement delegation.
    Submit,
    /// One new model request.
    Model,
    /// One independently delegated recursive improvement model request.
    RsiModel,
    /// One named tool; the tool gateway must separately authorize actual resource arguments.
    Tool {
        /// Tool identifier from the trusted execution registry.
        name: String,
    },
}

/// Host-owned resource policy required even when the owner selected full access.
#[async_trait]
pub trait ControlPolicy: Send + Sync {
    /// Verifies the original initialization plan, newly created resources and exact protected writer.
    async fn verify_initialization(
        &self,
        _binding: &AgentBinding,
        _runtime: &Runtime,
        _initialization: &Initialization,
    ) -> Result<()> {
        Err(Error::FencingRequired)
    }
    /// Independently verifies source fencing and the exact migration allocation before copying data.
    async fn verify_migration(&self, _binding: &AgentBinding, _runtime: &Runtime) -> Result<()> {
        Err(Error::FencingRequired)
    }
    /// Rejects actions outside the host ceiling and derives exact delegation requirements.
    async fn requirements(
        &self,
        binding: &AgentBinding,
        action: &ExecutionAction,
    ) -> Result<Vec<CapabilityScope>>;
}

/// Authenticated internal callbacks shared by independent and embedded hosts.
#[derive(Clone)]
pub struct ControlPlane {
    store: Store,
    key: Arc<ControlKey>,
    policy: Arc<dyn ControlPolicy>,
    migration_custody: Option<Arc<Custody>>,
    initialization_custody: Option<Arc<Custody>>,
    recovery_interval: TimeDelta,
}

impl std::fmt::Debug for ControlPlane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlane")
            .field("recovery_interval", &self.recovery_interval)
            .finish_non_exhaustive()
    }
}

impl ControlPlane {
    /// Binds callbacks to deployment secrets, current metadata, and explicit host policy.
    pub fn new(
        store: Store,
        key: Arc<ControlKey>,
        policy: Arc<dyn ControlPolicy>,
        recovery_interval: TimeDelta,
    ) -> Result<Self> {
        if recovery_interval.num_milliseconds() <= 0 {
            return Err(Error::InvalidCommand(
                "recovery interval must be positive".into(),
            ));
        }
        Ok(Self {
            store,
            key,
            policy,
            migration_custody: None,
            initialization_custody: None,
            recovery_interval,
        })
    }

    /// Enables protected key handoff only to an admitted, independently fenced migration writer.
    #[must_use]
    pub fn with_migration_custody(mut self, custody: Arc<Custody>) -> Self {
        self.migration_custody = Some(custody);
        self
    }

    /// Enables first-store key release and completion under independently verified new-resource policy.
    #[must_use]
    pub fn with_initialization_custody(mut self, custody: Arc<Custody>) -> Self {
        self.initialization_custody = Some(custody);
        self
    }

    /// Builds internal routes with a deployment-configured request envelope bound.
    ///
    /// Mount on a protected listener. Only the trusted execution process receives its
    /// credential; tool processes must not share its environment, memory, or network identity.
    pub fn router(self, max_body_bytes: NonZeroUsize) -> Router {
        Router::new()
            .route("/internal/v1/authority", post(authority))
            .route("/internal/v1/migration/key", post(migration_key))
            .route(
                "/internal/v1/initialization/verify",
                post(initialization_verify),
            )
            .route("/internal/v1/initialization/key", post(initialization_key))
            .route(
                "/internal/v1/initialization/complete",
                post(initialization_complete),
            )
            .route("/internal/v1/wake", post(wake))
            .route("/internal/v1/anchor/read", post(read))
            .route("/internal/v1/anchor/init", post(initialize))
            .route("/internal/v1/anchor/prepare", post(prepare))
            .route("/internal/v1/anchor/confirm", post(confirm))
            .route("/internal/v1/anchor/abort", post(abort))
            .layer(DefaultBodyLimit::max(max_body_bytes.get()))
            .layer(middleware::map_response_with_state(self.clone(), no_store))
            .with_state(self)
    }

    async fn runtime(
        &self,
        headers: &HeaderMap,
        agent_id: Uuid,
        epoch: i64,
        owner: &str,
    ) -> Result<Runtime> {
        let mut credentials = headers.get_all(header::AUTHORIZATION).iter();
        let credential = credentials.next().ok_or(Error::Forbidden)?;
        if credentials.next().is_some() {
            return Err(Error::Forbidden);
        }
        let (scheme, token) = credential
            .to_str()
            .map_err(|_| Error::Forbidden)?
            .split_once(' ')
            .ok_or(Error::Forbidden)?;
        if !scheme.eq_ignore_ascii_case("bearer") {
            return Err(Error::Forbidden);
        }
        let runtime = self.store.writer(agent_id).await?;
        self.key.verify(&runtime, token)?;
        if runtime.generation != epoch || runtime.engine_owner != owner {
            return Err(Error::Forbidden);
        }
        Ok(runtime)
    }

    async fn migration(&self, runtime: &Runtime) -> Result<AgentBinding> {
        self.migration_phase(runtime).await?;
        let binding = self.store.agent_binding(runtime.agent_id).await?;
        self.policy.verify_migration(&binding, runtime).await?;
        self.migration_phase(runtime).await?;
        Ok(binding)
    }

    async fn migration_phase(&self, runtime: &Runtime) -> Result<()> {
        if self.store.instance_phase(runtime).await?
            != (InstancePurpose::Migration, InstancePhase::Migrating)
        {
            return Err(Error::Forbidden);
        }
        Ok(())
    }

    async fn initialization(
        &self,
        runtime: &Runtime,
        operation_id: Uuid,
    ) -> Result<(AgentBinding, Initialization)> {
        self.initialization_phase(runtime).await?;
        let binding = self.store.agent_binding(runtime.agent_id).await?;
        let custody = self
            .initialization_custody
            .as_ref()
            .ok_or(Error::Forbidden)?;
        let initialization = custody
            .initialization(
                &Owner {
                    id: binding.agent.owner_id,
                },
                runtime.agent_id,
            )
            .await?
            .ok_or(Error::Forbidden)?;
        if initialization.operation_id != operation_id
            || matches!(initialization.state, InitializationState::Revoked)
        {
            return Err(Error::Forbidden);
        }
        self.policy
            .verify_initialization(&binding, runtime, &initialization)
            .await?;
        self.initialization_phase(runtime).await?;
        Ok((binding, initialization))
    }

    async fn initialization_phase(&self, runtime: &Runtime) -> Result<()> {
        if self.store.instance_phase(runtime).await?
            != (InstancePurpose::Initialization, InstancePhase::Initializing)
        {
            return Err(Error::Forbidden);
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InitializationIdentity {
    agent_id: Uuid,
    epoch: i64,
    owner: String,
    operation_id: Uuid,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InitializationComplete {
    agent_id: Uuid,
    epoch: i64,
    owner: String,
    operation_id: Uuid,
    head: Head,
}

#[derive(Serialize, Zeroize, ZeroizeOnDrop)]
#[serde(rename_all = "camelCase")]
struct InitializationKey {
    #[zeroize(skip)]
    operation_id: Uuid,
    service_key: String,
}

async fn initialization_verify(
    State(state): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<InitializationIdentity>,
) -> std::result::Result<Json<Decision>, Failure> {
    let runtime = state
        .runtime(&headers, request.agent_id, request.epoch, &request.owner)
        .await?;
    let (_, initialization) = state.initialization(&runtime, request.operation_id).await?;
    if initialization.state != InitializationState::Pending {
        return Err(Error::Forbidden.into());
    }
    Ok(Json(Decision {
        allowed: true,
        reason: None,
    }))
}

async fn initialization_key(
    State(state): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<InitializationIdentity>,
) -> std::result::Result<Json<InitializationKey>, Failure> {
    let runtime = state
        .runtime(&headers, request.agent_id, request.epoch, &request.owner)
        .await?;
    let (binding, _) = state.initialization(&runtime, request.operation_id).await?;
    let custody = state
        .initialization_custody
        .as_ref()
        .ok_or(Error::Forbidden)?;
    let lease = custody
        .release_initialization_key(
            &Owner {
                id: binding.agent.owner_id,
            },
            &runtime,
            request.operation_id,
        )
        .await?;
    state.initialization_phase(&runtime).await?;
    Ok(Json(InitializationKey {
        operation_id: lease.operation_id,
        service_key: BASE64_URL_SAFE_NO_PAD.encode(lease.key.expose()),
    }))
}

async fn initialization_complete(
    State(state): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<InitializationComplete>,
) -> std::result::Result<Json<CustodyReceipt>, Failure> {
    let runtime = state
        .runtime(&headers, request.agent_id, request.epoch, &request.owner)
        .await?;
    let (binding, _) = state.initialization(&runtime, request.operation_id).await?;
    let custody = state
        .initialization_custody
        .as_ref()
        .ok_or(Error::Forbidden)?;
    Ok(Json(
        custody
            .finish_initialization(
                &Owner {
                    id: binding.agent.owner_id,
                },
                &runtime,
                request.operation_id,
                &request.head.try_into()?,
            )
            .await?,
    ))
}

#[derive(Serialize, Zeroize, ZeroizeOnDrop)]
#[serde(rename_all = "camelCase")]
struct MigrationKey {
    revision: i64,
    service_key: String,
}

async fn migration_key(
    State(state): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<IdentityRequest>,
) -> std::result::Result<Json<MigrationKey>, Failure> {
    let runtime = state
        .runtime(&headers, request.agent_id, request.epoch, &request.owner)
        .await?;
    let binding = state.migration(&runtime).await?;
    let custody = state.migration_custody.as_ref().ok_or(Error::Forbidden)?;
    let lease = custody.release(binding.agent.owner_id, &runtime).await?;
    state.migration_phase(&runtime).await?;
    Ok(Json(MigrationKey {
        revision: lease.revision,
        service_key: BASE64_URL_SAFE_NO_PAD.encode(lease.key.expose()),
    }))
}

async fn no_store(State(state): State<ControlPlane>, mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if response.status() == StatusCode::SERVICE_UNAVAILABLE {
        let millis = state.recovery_interval.num_milliseconds();
        let seconds = millis / 1000 + i64::from(millis % 1000 != 0);
        if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
    }
    response
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IdentityRequest {
    agent_id: Uuid,
    epoch: i64,
    owner: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InitializeRequest {
    agent_id: Uuid,
    epoch: i64,
    owner: String,
    head: Head,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PrepareRequest {
    agent_id: Uuid,
    epoch: i64,
    owner: String,
    expected: Head,
    pending: Pending,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfirmRequest {
    agent_id: Uuid,
    epoch: i64,
    owner: String,
    pending: Pending,
    local: Head,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AbortRequest {
    agent_id: Uuid,
    epoch: i64,
    owner: String,
    pending: Pending,
    verified_local: Head,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WakeRequest {
    agent_id: Uuid,
    epoch: i64,
    owner: String,
    wake: WakeState,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WakeState {
    active: bool,
    at: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AuthorityRequest {
    agent_id: Uuid,
    epoch: i64,
    owner: String,
    grant: Option<GrantReference>,
    action: Action,
    tool: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Submit,
    Input,
    Stop,
    Model,
    Tool,
    Owner,
    Rsi,
    Resume,
    Migrate,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantReference {
    id: Uuid,
    version: i64,
}

#[derive(Debug, Serialize)]
struct Decision {
    allowed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Head {
    sequence: i64,
    digest: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Pending {
    operation_id: Uuid,
    generation: i64,
    next: Head,
}

#[derive(Debug, Serialize)]
struct AnchorResponse {
    head: Head,
    pending: Option<Pending>,
}

impl TryFrom<Head> for AnchorHead {
    type Error = Error;

    fn try_from(head: Head) -> Result<Self> {
        let mut digest = [0; 32];
        if head.sequence < 0
            || BASE64_URL_SAFE_NO_PAD
                .decode_slice(&head.digest, &mut digest)
                .map_err(|_| Error::FreshnessMismatch)?
                != digest.len()
        {
            return Err(Error::FreshnessMismatch);
        }
        Ok(Self {
            sequence: head.sequence,
            digest: digest.to_vec(),
        })
    }
}

impl From<AnchorHead> for Head {
    fn from(head: AnchorHead) -> Self {
        Self {
            sequence: head.sequence,
            digest: BASE64_URL_SAFE_NO_PAD.encode(head.digest),
        }
    }
}

impl TryFrom<Pending> for PendingAnchor {
    type Error = Error;

    fn try_from(pending: Pending) -> Result<Self> {
        if pending.generation <= 0 {
            return Err(Error::FreshnessMismatch);
        }
        Ok(Self {
            operation_id: pending.operation_id,
            generation: pending.generation,
            next: pending.next.try_into()?,
        })
    }
}

impl From<Anchor> for AnchorResponse {
    fn from(anchor: Anchor) -> Self {
        Self {
            head: anchor.head.into(),
            pending: anchor.pending.map(|pending| Pending {
                operation_id: pending.operation_id,
                generation: pending.generation,
                next: pending.next.into(),
            }),
        }
    }
}

async fn read(
    State(state): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<IdentityRequest>,
) -> std::result::Result<Json<AnchorResponse>, Failure> {
    let runtime = state
        .runtime(&headers, request.agent_id, request.epoch, &request.owner)
        .await?;
    Ok(Json(state.store.anchor(&runtime).await?.into()))
}

async fn initialize(
    State(state): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<InitializeRequest>,
) -> std::result::Result<Json<AnchorResponse>, Failure> {
    let runtime = state
        .runtime(&headers, request.agent_id, request.epoch, &request.owner)
        .await?;
    let head: AnchorHead = request.head.try_into()?;
    if head.sequence != 0 {
        return Err(Error::FreshnessMismatch.into());
    }
    Ok(Json(
        state
            .store
            .initialize_anchor(&runtime, &head.digest)
            .await?
            .into(),
    ))
}

async fn prepare(
    State(state): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<PrepareRequest>,
) -> std::result::Result<Json<AnchorResponse>, Failure> {
    let runtime = state
        .runtime(&headers, request.agent_id, request.epoch, &request.owner)
        .await?;
    let expected = request.expected.try_into()?;
    let pending = request.pending.try_into()?;
    Ok(Json(
        state
            .store
            .prepare_anchor(&runtime, &expected, &pending)
            .await?
            .into(),
    ))
}

async fn confirm(
    State(state): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<ConfirmRequest>,
) -> std::result::Result<Json<AnchorResponse>, Failure> {
    let runtime = state
        .runtime(&headers, request.agent_id, request.epoch, &request.owner)
        .await?;
    let pending = request.pending.try_into()?;
    let local = request.local.try_into()?;
    Ok(Json(
        state
            .store
            .confirm_anchor(&runtime, &pending, &local)
            .await?
            .into(),
    ))
}

async fn abort(
    State(state): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<AbortRequest>,
) -> std::result::Result<Json<AnchorResponse>, Failure> {
    let runtime = state
        .runtime(&headers, request.agent_id, request.epoch, &request.owner)
        .await?;
    let pending = request.pending.try_into()?;
    let local = request.verified_local.try_into()?;
    Ok(Json(
        state
            .store
            .abort_uncommitted_anchor(&runtime, &pending, &local)
            .await?
            .into(),
    ))
}

async fn wake(
    State(state): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<WakeRequest>,
) -> std::result::Result<StatusCode, Failure> {
    let runtime = state
        .runtime(&headers, request.agent_id, request.epoch, &request.owner)
        .await?;
    let at = request
        .wake
        .at
        .map(|at| {
            DateTime::from_timestamp_millis(at)
                .ok_or_else(|| Error::InvalidCommand("invalid wake timestamp".into()))
        })
        .transpose()?;
    state
        .store
        .report_wake(
            &runtime,
            &Wake {
                active: request.wake.active,
                at,
            },
            state.recovery_interval,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn authority(
    State(state): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<AuthorityRequest>,
) -> std::result::Result<Json<Decision>, Failure> {
    let runtime = state
        .runtime(&headers, request.agent_id, request.epoch, &request.owner)
        .await?;
    if matches!(request.action, Action::Migrate) {
        if request.grant.is_some() || request.tool.is_some() {
            return Err(Error::InvalidCommand(
                "migration admission does not accept task grants or tools".into(),
            )
            .into());
        }
        state.migration(&runtime).await?;
        return Ok(Json(Decision {
            allowed: true,
            reason: None,
        }));
    }
    state.store.verify_execution(&runtime).await?;
    if !matches!(request.action, Action::Tool) && request.tool.is_some() {
        return Err(Error::InvalidCommand("tool is only valid for tool admission".into()).into());
    }
    let action = match request.action {
        Action::Input | Action::Stop | Action::Owner | Action::Resume => {
            return Ok(Json(Decision {
                allowed: true,
                reason: None,
            }));
        }
        Action::Submit => ExecutionAction::Submit,
        Action::Model => ExecutionAction::Model,
        Action::Rsi => ExecutionAction::RsiModel,
        Action::Migrate => return Err(Error::Forbidden.into()),
        Action::Tool => ExecutionAction::Tool {
            name: request
                .tool
                .filter(|name| !name.is_empty())
                .ok_or_else(|| Error::InvalidCommand("tool identifier is required".into()))?,
        },
    };
    let result = async {
        let grant = request.grant.ok_or(Error::Forbidden)?;
        let binding = state.store.agent_binding(runtime.agent_id).await?;
        let scopes = state.policy.requirements(&binding, &action).await?;
        state
            .store
            .authorize_runtime_for(
                &runtime,
                grant.id,
                grant.version,
                if action == ExecutionAction::RsiModel {
                    gesang_api::GrantPurpose::Improvement
                } else {
                    gesang_api::GrantPurpose::Task
                },
                &scopes,
            )
            .await?;
        Ok(())
    }
    .await;
    match result {
        Ok(()) => Ok(Json(Decision {
            allowed: true,
            reason: None,
        })),
        Err(Error::Forbidden) => Ok(Json(Decision {
            allowed: false,
            reason: Some("Execution is not authorized"),
        })),
        Err(error) => Err(Failure(error)),
    }
}

struct Failure(Error);

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        Self(error)
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        let (status, code, message) = match self.0 {
            Error::Forbidden => (
                StatusCode::FORBIDDEN,
                "forbidden",
                "Runtime authentication or authorization failed",
            ),
            Error::NotFound => (StatusCode::NOT_FOUND, "not_found", "Resource not found"),
            Error::InvalidCommand(_) | Error::Serialization(_) => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Request violates the control contract",
            ),
            Error::StaleGeneration | Error::FencingRequired => (
                StatusCode::CONFLICT,
                "stale_runtime",
                "Runtime admission is no longer current",
            ),
            Error::FreshnessMismatch => (
                StatusCode::CONFLICT,
                "freshness_mismatch",
                "Execution history does not match the trusted anchor",
            ),
            Error::VersionConflict | Error::RequestConflict => (
                StatusCode::CONFLICT,
                "version_conflict",
                "The operation conflicts with current state",
            ),
            _ => (
                StatusCode::SERVICE_UNAVAILABLE,
                "control_unavailable",
                "Control state could not be established",
            ),
        };
        (
            status,
            Json(ApiProblem {
                code: code.into(),
                message: message.into(),
            }),
        )
            .into_response()
    }
}
