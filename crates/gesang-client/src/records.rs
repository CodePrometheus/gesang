//! Foreign-language records contain presentation data, never execution internals.

use gesang_api::{
    CapabilityScope, CommandReceipt, DeliveryState, Grant, PermissionMode, SessionSnapshot,
};

/// Actionable client failures without credentials, private payloads, or raw responses.
#[derive(Clone, Debug, thiserror::Error, uniffi::Error)]
pub enum ClientError {
    /// An explicitly closed observation cannot be reused; acquire a fresh snapshot.
    #[error("The observation is closed.")]
    ObservationClosed,
    /// The endpoint is malformed or violates the transport policy.
    #[error("The server address is invalid: {reason}")]
    InvalidAddress {
        /// Public explanation containing no supplied credentials.
        reason: String,
    },
    #[error("Sign in to continue.")]
    /// The device session must be renewed through browser authorization.
    AuthenticationRequired,
    #[error("This operation is outside the current authorization.")]
    /// The current identity or delegated grant cannot perform the action.
    PermissionDenied,
    #[error("The remote object was not found.")]
    /// The referenced resource is absent or hidden by its owner boundary.
    NotFound,
    #[error("The object changed. Refresh before trying again.")]
    /// The expected state or immutable request content does not match.
    Conflict,
    #[error("The server could not be reached. The outcome may be unknown.")]
    /// Transport failed; a submitted command may already have been accepted.
    ConnectionUnavailable,
    #[error("The server returned an invalid response.")]
    /// A response violates the supported public contract.
    InvalidResponse,
    #[error("The server rejected the operation: {code}")]
    /// A public server error code identifies an explicitly rejected operation.
    Rejected {
        /// Stable public rejection code, suitable for localization.
        code: String,
    },
    #[error("The value is invalid: {field}")]
    /// A named input cannot be represented by the public contract.
    InvalidInput {
        /// Public input field requiring correction.
        field: String,
    },
}

/// Authority selected explicitly by the person, within the administrator's ceiling.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum Permission {
    /// Available operations on the person's own permitted resources.
    FullAccess,
    /// Only explicitly selected capabilities and resources.
    Scoped,
    /// Confirmation is required for protected writes and external actions.
    Ask,
}

impl From<Permission> for PermissionMode {
    fn from(value: Permission) -> Self {
        match value {
            Permission::FullAccess => Self::FullAccess,
            Permission::Scoped => Self::Scoped,
            Permission::Ask => Self::Ask,
        }
    }
}

impl From<PermissionMode> for Permission {
    fn from(value: PermissionMode) -> Self {
        match value {
            PermissionMode::FullAccess => Self::FullAccess,
            PermissionMode::Scoped => Self::Scoped,
            PermissionMode::Ask => Self::Ask,
        }
    }
}

/// An explicitly selected operation and resource pair.
#[derive(Clone, Debug, uniffi::Record)]
pub struct Scope {
    /// Stable capability identifier published by the deployment.
    pub capability: String,
    /// Resource identifier within the person's permitted namespace.
    pub resource: String,
}

impl From<Scope> for CapabilityScope {
    fn from(value: Scope) -> Self {
        Self {
            capability: value.capability,
            resource: value.resource,
        }
    }
}

impl From<CapabilityScope> for Scope {
    fn from(value: CapabilityScope) -> Self {
        Self {
            capability: value.capability,
            resource: value.resource,
        }
    }
}

/// The server's durable grant; changing a local selection does not change authority.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GrantView {
    /// Stable grant identifier.
    pub id: String,
    /// Agent to which this grant belongs.
    pub agent_id: String,
    /// Authorization version verified again when an action is admitted.
    pub version: i64,
    /// Effective authority mode returned by the server.
    pub permission: Permission,
    /// Explicit capability selections for a scoped grant.
    pub scopes: Vec<Scope>,
    /// Revocation timestamp, when this grant is no longer usable for new work.
    pub revoked_at: Option<String>,
}

impl From<Grant> for GrantView {
    fn from(value: Grant) -> Self {
        Self {
            id: value.id.to_string(),
            agent_id: value.agent_id.to_string(),
            version: value.version,
            permission: value.mode.into(),
            scopes: value.scopes.into_iter().map(Into::into).collect(),
            revoked_at: value.revoked_at.map(|v| v.to_rfc3339()),
        }
    }
}

/// A verified person's agent and latest authorization observation.
#[derive(Clone, Debug, uniffi::Record)]
pub struct SessionView {
    /// Deployment-local owner identifier, never an unverified email address.
    pub owner_id: String,
    /// Stable logical agent identifier.
    pub agent_id: String,
    /// Current grant, absent until the person explicitly chooses permissions.
    pub grant: Option<GrantView>,
    /// Client observation time in Unix milliseconds.
    pub synchronized_at_ms: i64,
}

impl From<SessionSnapshot> for SessionView {
    fn from(value: SessionSnapshot) -> Self {
        Self {
            owner_id: value.owner_id.to_string(),
            agent_id: value.agent.id.to_string(),
            grant: value.grant.map(Into::into),
            synchronized_at_ms: chrono::Utc::now().timestamp_millis(),
        }
    }
}

/// Delivery acceptance is independent of the goal's execution phase.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum ReceiptState {
    /// The platform has persisted delivery work; engine acceptance is outstanding.
    Pending,
    /// The execution service has durably accepted this immutable request.
    Accepted,
    /// The request was refused with a queryable public error code.
    Rejected,
}

/// The original request's delivery result, suitable for reconciling a lost response.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ReceiptView {
    /// Original immutable request identifier.
    pub request_id: String,
    /// Agent whose execution service receives the request.
    pub agent_id: String,
    /// Current delivery state; accepted never means the task is complete.
    pub state: ReceiptState,
    /// Public rejection code, when available.
    pub problem_code: Option<String>,
    /// Original goal reference supplied by the execution service.
    pub goal_id: Option<String>,
    /// Goal version at durable acceptance.
    pub accepted_version: Option<u64>,
}

impl TryFrom<CommandReceipt> for ReceiptView {
    type Error = ClientError;

    fn try_from(value: CommandReceipt) -> Result<Self, ClientError> {
        let engine = value
            .engine_receipt
            .map(serde_json::from_value::<chopin_protocol::EngineReceipt>)
            .transpose()
            .map_err(|_| ClientError::InvalidResponse)?;
        if let Some(engine) = &engine
            && engine.request_id != value.request_id.to_string()
        {
            return Err(ClientError::InvalidResponse);
        }
        Ok(Self {
            request_id: value.request_id.to_string(),
            agent_id: value.agent_id.to_string(),
            state: match value.state {
                DeliveryState::Pending => ReceiptState::Pending,
                DeliveryState::Accepted => ReceiptState::Accepted,
                DeliveryState::Rejected => ReceiptState::Rejected,
            },
            problem_code: value.problem.map(|p| p.code),
            goal_id: engine.as_ref().map(|e| e.goal_id.clone()),
            accepted_version: engine.map(|e| e.accepted_version),
        })
    }
}
