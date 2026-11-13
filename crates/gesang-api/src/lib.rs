//! Platform contracts shared by hosts and clients; execution contracts remain upstream.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// A stable logical agent, independent of its current runtime instance.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Agent {
    /// Platform-wide logical identity.
    pub id: Uuid,
    /// Explicitly mapped owner, never inferred from an email address.
    pub owner_id: Uuid,
    /// Execution adapter identifier.
    pub engine: String,
    /// Time the logical agent was registered.
    pub created_at: DateTime<Utc>,
}

/// The owner's explicitly selected task permission policy.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PermissionMode {
    /// Automatically admit the owner's available actions within host policy.
    FullAccess,
    /// Automatically admit only explicitly selected capabilities and resources.
    Scoped,
    /// Require a bound decision for actions requiring confirmation.
    Ask,
}

/// One exact capability and resource pair; neither field implies wildcard matching.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilityScope {
    /// Host-defined capability identifier.
    pub capability: String,
    /// Host-verified resource identity.
    pub resource: String,
}

/// Separately revocable execution domains within the same owner's agent.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GrantPurpose {
    /// User-submitted goals and their execution actions.
    #[default]
    Task,
    /// Independently budgeted recursive improvement work.
    Improvement,
}

/// A revocable execution delegation, separate from the device's login session.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Grant {
    /// Stable delegation identity.
    pub id: Uuid,
    /// The only logical agent allowed to use this delegation.
    pub agent_id: Uuid,
    /// Execution domain; a delegation never substitutes for another purpose.
    #[serde(default)]
    pub purpose: GrantPurpose,
    /// Monotonic revision checked again before action admission.
    pub version: i64,
    /// Explicit owner policy.
    pub mode: PermissionMode,
    /// Exact permitted scopes for a scoped policy.
    pub scopes: Vec<CapabilityScope>,
    /// Revocation time; an existing side effect is not undone by revocation.
    pub revoked_at: Option<DateTime<Utc>>,
}

/// An immutable request for at-least-once delivery to the execution authority.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommandSubmission<C = Value> {
    /// Caller-generated identifier reused after every uncertain response.
    pub request_id: Uuid,
    /// Owner control and execution delegation have separate admission requirements.
    pub authorization: CommandAuthorization,
    /// Typed upstream command encoded without changing its execution semantics.
    pub command: C,
}

/// Authentication for control is independent of permission to start execution actions.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommandAuthorization {
    /// The authenticated owner may stop or provide input even after task revocation.
    Owner,
    /// Starting execution requires a current delegation scoped to the agent.
    Grant {
        /// Stable delegation identity.
        id: Uuid,
        /// Exact delegation revision.
        version: i64,
    },
}

/// Delivery state; no variant claims that the goal itself has finished.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    /// Persisted by the platform, without a durable execution acknowledgement yet.
    Pending,
    /// The execution authority acknowledged durable acceptance.
    Accepted,
    /// The command was definitively rejected without uncertain delivery.
    Rejected,
}

/// A queryable result for the original request identifier.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CommandReceipt {
    /// Original caller-generated identifier.
    pub request_id: Uuid,
    /// Logical agent that owns this command.
    pub agent_id: Uuid,
    /// Current delivery acknowledgement state.
    pub state: DeliveryState,
    /// Original upstream acceptance receipt, present only after acceptance.
    pub engine_receipt: Option<Value>,
    /// Definitive rejection reason, excluding private command content.
    pub problem: Option<ApiProblem>,
}

/// A stable machine-readable error with an English diagnostic.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ApiProblem {
    /// Contract-defined error identifier.
    pub code: String,
    /// Diagnostic safe for display without credentials or command content.
    pub message: String,
}

/// The authenticated person's current logical agent and task delegation.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionSnapshot {
    /// Host-mapped owner identity.
    pub owner_id: Uuid,
    /// Same logical agent used by all authenticated clients in this deployment.
    pub agent: Agent,
    /// Absent until the owner explicitly selects a policy.
    pub grant: Option<Grant>,
}

/// Explicit permission selection; the host revokes the previous grant atomically.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GrantSelection {
    /// Owner-selected policy, with no implicit full-access default.
    pub mode: PermissionMode,
    /// Exact permitted scopes for a scoped policy.
    pub scopes: Vec<CapabilityScope>,
}

/// Public metadata used for native authorization with a system browser and PKCE.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfiguration {
    /// Platform contract version supported by this deployment.
    pub api_version: u32,
    /// Trusted identity issuer verified independently by the server.
    pub issuer: String,
    /// Browser authorization endpoint.
    pub authorization_endpoint: String,
    /// Standard authorization-code and refresh-token endpoint.
    pub token_endpoint: String,
    /// Public native client registration; this value is not a secret.
    pub client_id: String,
    /// Registered application callback URI.
    pub redirect_uri: String,
    /// Scopes required by this deployment's public client registration.
    pub scopes: Vec<String>,
}

/// Supported native push provider for an explicitly configured deployment.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PushProvider {
    /// Android cloud messaging, using the deployment's matching project credentials.
    Fcm,
}

/// A typed provider destination, distinct from a user identity or sending credential.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PushTarget {
    /// A current installation identifier obtained from the configured native provider.
    FirebaseInstallation {
        /// Installation identifier; do not log or expose it in a notification payload.
        id: String,
    },
}

/// Registers a destination for the authenticated device; ownership comes from its session.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PushRegistration {
    /// Delivery adapter configured by the deployment.
    pub provider: PushProvider,
    /// Must match the native application's provider project and the server sender identity.
    pub project_id: String,
    /// Exact typed destination, never a provider sending credential.
    pub target: PushTarget,
}

/// Durable registration state, independent of permission prompts or delivery success.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PushRegistrationStatus {
    /// The server accepted this device's destination into persistent registration state.
    Registered,
    /// The server revoked the registration and will not schedule further delivery to it.
    Revoked,
}

/// Server acknowledgement of device registration, not proof that a notification was delivered.
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DevicePushRegistration {
    /// Authenticated installation's platform device identity.
    pub device_id: Uuid,
    /// Actual persisted registration status.
    pub status: PushRegistrationStatus,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn additive_response_fields_remain_compatible() {
        let problem: ApiProblem = serde_json::from_value(
            json!({"code":"forbidden","message":"Permission denied","futureField":true}),
        )
        .unwrap();
        assert_eq!(problem.code, "forbidden");
    }

    #[test]
    fn unknown_request_fields_are_rejected_before_admission() {
        assert!(
            serde_json::from_value::<GrantSelection>(
                json!({"mode":"full_access","scopes":[],"ownerId":"someone-else"})
            )
            .is_err()
        );
    }
}
