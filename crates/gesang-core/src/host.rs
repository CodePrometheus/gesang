use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gesang_api::{Agent, ApiProblem, CapabilityScope, CommandAuthorization};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{Error, InstanceIntent, InstancePlan, PreparationProgress, Result};

/// An issuer and subject already authenticated by the embedding host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedIdentity {
    /// Exact trusted identity issuer, including its deployment scope.
    pub issuer: String,
    /// Stable issuer-local subject; an email address is not a cross-issuer identity.
    pub subject: String,
}

/// A persisted platform identity binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Owner {
    /// Platform identity used for tenant isolation.
    pub id: Uuid,
}

/// Immutable identity and placement needed by trusted host resource adapters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentBinding {
    /// Stable logical agent and authenticated platform owner.
    pub agent: Agent,
    /// Owner identity in the execution service's existing store.
    pub engine_owner: String,
    /// Host-owned workspace locator, independent of any running instance.
    pub placement: Option<String>,
    /// Sorted physical resource identities, each exclusively bound to this agent.
    pub workspace_keys: Vec<String>,
}

/// The admitted execution instance for one logical agent.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Runtime {
    /// Concrete instance identity, distinct from the stable logical agent.
    pub id: Uuid,
    /// Agent whose execution store this runtime may access.
    pub agent_id: Uuid,
    /// Fencing generation checked before every new action.
    pub generation: i64,
    /// Host-owned connection locator, never supplied by an untrusted client.
    pub locator: String,
    /// Owner identity within the isolated execution service.
    pub engine_owner: String,
}

/// Controller admission intent persisted before provisioning side effects.
#[derive(Clone, Debug)]
pub struct RuntimeClaim {
    /// Stable logical agent and owner.
    pub agent: Agent,
    /// Existing execution-local owner that provisioning must preserve.
    pub engine_owner: String,
    /// Immutable host placement binding used to adopt an existing workspace.
    pub placement: Option<String>,
    /// Sorted physical resource identities whose ownership the provider must verify.
    pub workspace_keys: Vec<String>,
    /// Current controller process identity.
    pub controller_id: Uuid,
    /// Generation allocated by the transactional store.
    pub generation: i64,
    /// Database-observed lease deadline.
    pub expires_at: DateTime<Utc>,
    /// Previous admitted writer requiring independent fencing evidence.
    pub previous: Option<Runtime>,
}

/// Evidence obtained by a trusted provider before admitting a replacement writer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreviousWriter {
    /// The provider established that no previous writer exists.
    Absent,
    /// The exact prior instance and its protected process subtree have stopped.
    Terminated(Uuid),
    /// The storage backend revoked all prior-instance I/O capability.
    StorageRevoked(Uuid),
}

/// A provider's completed writer handoff, subject to final generation validation.
#[derive(Clone, Debug)]
pub struct Admission {
    /// Newly admitted instance identity.
    pub runtime_id: Uuid,
    /// Connection locator verified by the host.
    pub locator: String,
    /// Execution-local owner preserving existing data ownership.
    pub engine_owner: String,
    /// Independent evidence that the previous writer cannot continue writing.
    pub previous_writer: PreviousWriter,
}

/// Observation used by lifecycle policy; unknown is never treated as idle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Activity {
    /// Work is active or has not reached a durable waiting point.
    Active,
    /// Work is durably waiting with no attached execution to preserve.
    Waiting,
    /// The provider cannot establish the current activity state.
    Unknown,
}

/// Independently observed instance liveness, distinct from engine activity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InstanceObservation {
    /// The instance exists and its activity observation is available.
    Live(Activity),
    /// The provider verified that this exact instance has stopped.
    Stopped,
    /// Liveness cannot be established; replacement admission remains fenced.
    Unknown,
}

/// The execution scheduler's actual need for a live process or a future wake.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Wake {
    /// Runnable, running, or still-recovering execution exists.
    pub active: bool,
    /// Earliest absolute deadline reported by the execution authority.
    pub at: Option<DateTime<Utc>>,
}

/// Nonblocking storage restoration status while the controller keeps renewing its lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnginePreparation {
    /// The same instance is still opening and verifying its protected storage.
    Pending,
    /// Storage is restored and the execution scheduler remains paused.
    Ready,
}

/// Capabilities derived by the trusted adapter from a validated command.
#[derive(Clone, Debug)]
pub struct CommandRequirements {
    /// Trusted adapter classification, never taken from the caller's authorization claim.
    pub authority: RequiredAuthority,
    /// Exact capabilities and resources required for admission.
    pub scopes: Vec<CapabilityScope>,
}

/// Authorization class derived from a validated upstream operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequiredAuthority {
    /// Owner-only control may quiesce existing work without granting new execution.
    OwnerControl,
    /// New execution requires a current task delegation.
    TaskGrant,
}

/// A definitive execution acknowledgement, distinct from transport uncertainty.
#[derive(Clone, Debug)]
pub enum ExecutionResponse {
    /// Original durable upstream acceptance receipt.
    Accepted(Value),
    /// Rejection known not to have accepted this command.
    Rejected(ApiProblem),
}

/// Host-validated envelope for an existing execution authority.
#[derive(Clone, Debug)]
pub struct Dispatch {
    /// Stable identifier shared by the outbox and upstream deduplication.
    pub request_id: Uuid,
    /// Current admitted writer instance.
    pub runtime: Runtime,
    /// Verified owner-control or execution-delegation admission.
    pub authorization: CommandAuthorization,
    /// Validated upstream command, without a second platform state machine.
    pub command: Value,
}

/// Host provisioning with an explicit independent writer-fencing obligation.
#[async_trait]
pub trait InstanceProvider: Send + Sync {
    /// Resolves a versioned nonsecret definition without changing infrastructure.
    async fn plan(&self, _claim: &RuntimeClaim) -> Result<InstancePlan> {
        Err(Error::InvalidCommand(
            "instance planning is not configured".into(),
        ))
    }
    /// Fences prior writers and allocates an exact gated instance without starting it.
    async fn allocate(&self, _intent: &InstanceIntent) -> Result<Admission> {
        Err(Error::FencingRequired)
    }
    /// Starts or observes the same admitted allocation; it must never recreate a missing instance.
    async fn activate(&self, _intent: &InstanceIntent) -> Result<PreparationProgress> {
        Err(Error::RuntimeNotReady)
    }
    /// Distinguishes a verified stopped instance from unavailable observations.
    async fn observe(&self, runtime: &Runtime) -> Result<InstanceObservation>;
    /// Protects an instance from host reclamation until the supplied deadline.
    async fn retain_until(&self, runtime: &Runtime, until: DateTime<Utc>) -> Result<()>;
    /// Requests and verifies a safe stop, preserving persistent execution records.
    async fn stop(&self, runtime: &Runtime) -> Result<()>;
    /// Stops and independently verifies every launch through the retirement generation.
    async fn retire(&self, _retirement: &crate::RuntimeRetirement) -> Result<()> {
        Err(Error::FencingRequired)
    }
}

/// Versioned execution adapter; the upstream store remains authoritative.
#[async_trait]
pub trait EngineBridge: Send + Sync {
    /// Authenticates and restores storage with execution scheduling still paused.
    async fn prepare(&self, _runtime: &Runtime) -> Result<EnginePreparation> {
        Err(Error::RuntimeNotReady)
    }
    /// Validates the command and derives host-required admission scopes.
    fn requirements(&self, command: &Value) -> Result<CommandRequirements>;
    /// Validates admission against embedded delegation references before persistence or delivery.
    fn authorize_command(
        &self,
        command: &Value,
        authorization: &CommandAuthorization,
    ) -> Result<CommandRequirements> {
        let requirements = self.requirements(command)?;
        match (authorization, requirements.authority) {
            (CommandAuthorization::Owner, RequiredAuthority::OwnerControl)
                if requirements.scopes.is_empty() => {}
            (CommandAuthorization::Grant { .. }, RequiredAuthority::TaskGrant) => {}
            _ => return Err(Error::Forbidden),
        }
        Ok(requirements)
    }
    /// Reconciles an uncertain delivery before any attempt to send it again.
    async fn request(&self, runtime: &Runtime, request_id: Uuid) -> Result<Option<Value>>;
    /// Delivers with stable identity; transport failure must remain uncertain.
    async fn execute(&self, dispatch: &Dispatch) -> Result<ExecutionResponse>;
    /// Queries actual scheduler wake requirements.
    async fn wake(&self, runtime: &Runtime) -> Result<Wake>;
    /// Resumes the existing scheduler; calling this twice is safe.
    async fn resume(&self, runtime: &Runtime) -> Result<()>;
    /// Reads an upstream goal snapshot without maintaining a platform copy.
    async fn snapshot(&self, runtime: &Runtime, goal_id: &str) -> Result<Value>;
}
