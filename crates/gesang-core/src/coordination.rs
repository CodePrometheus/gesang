//! Business operations that a host supplies to the coordination kernel.

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use gesang_api::{Agent, ApiProblem, CapabilityScope, CommandReceipt, CommandSubmission, Grant};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    Admission, AnchorHead, CommandRequirements, InstanceIntent, InstancePhase, InstancePlan, Owner,
    PendingCommand, PreviousWriter, Result, Runtime, RuntimeClaim, RuntimeRetirement, Store,
    WakeClaim,
};

/// Durable coordination operations used by the kernel controller.
///
/// Implementations own transaction boundaries, authorization, and generation
/// fencing. Each mutating operation persists its complete change before returning
/// success. Dropping a future can leave an unknown commit outcome. Stable request
/// and attempt identities let the caller reconcile without issuing a second effect.
#[async_trait]
pub trait CoordinationStore: Send + Sync {
    /// Reads an agent in its authenticated owner's scope.
    async fn agent(&self, owner: &Owner, id: Uuid) -> Result<Agent>;

    /// Atomically persists immutable command delivery and its recovery wake.
    async fn enqueue(
        &self,
        owner: &Owner,
        agent_id: Uuid,
        submission: &CommandSubmission,
        requirements: &CommandRequirements,
    ) -> Result<CommandReceipt>;

    /// Claims the original pending command under a unique delivery attempt.
    async fn claim_command(
        &self,
        claimant: Uuid,
        duration: TimeDelta,
    ) -> Result<Option<PendingCommand>>;

    /// Commits the original receipt only for the current attempt.
    async fn accept_command(&self, command: &PendingCommand, receipt: &Value) -> Result<()>;

    /// Commits a definitive rejection only for the current attempt.
    async fn reject_command(&self, command: &PendingCommand, problem: &ApiProblem) -> Result<()>;

    /// Preserves uncertain work and schedules its original request for reconciliation.
    async fn defer_command(&self, command: &PendingCommand, due_at: DateTime<Utc>) -> Result<()>;

    /// Verifies the current task delegation, its exact revision, and required scopes.
    async fn authorize(
        &self,
        agent_id: Uuid,
        grant_id: Uuid,
        version: i64,
        scopes: &[CapabilityScope],
    ) -> Result<Grant>;

    /// Creates or renews a controller lease without proving prior-writer fencing.
    async fn claim_runtime(
        &self,
        agent_id: Uuid,
        controller_id: Uuid,
        duration: TimeDelta,
    ) -> Result<RuntimeClaim>;

    /// Reads the instance intent for the current controller claim.
    async fn instance_intent(&self, claim: &RuntimeClaim) -> Result<Option<InstanceIntent>>;

    /// Persists a preparation intent before external allocation.
    async fn begin_instance(
        &self,
        claim: &RuntimeClaim,
        plan: &InstancePlan,
    ) -> Result<InstanceIntent>;

    /// Requires the exact instance intent and current controller claim.
    async fn verify_instance_intent(&self, intent: &InstanceIntent) -> Result<()>;

    /// Records the actual physical writer only with verified admission evidence.
    async fn admit_instance(
        &self,
        intent: &InstanceIntent,
        admission: &Admission,
    ) -> Result<InstanceIntent>;

    /// Advances the current admitted instance through a legal lifecycle transition.
    async fn transition_instance(
        &self,
        intent: &InstanceIntent,
        next: InstancePhase,
    ) -> Result<InstanceIntent>;

    /// Records completion only with the original intent and independent fencing evidence.
    async fn finish_instance(
        &self,
        intent: &InstanceIntent,
        proof: &PreviousWriter,
        completed_head: Option<&AnchorHead>,
        problem: Option<&ApiProblem>,
    ) -> Result<InstanceIntent>;

    /// Advances the exact controller claim after independent fencing.
    async fn advance_fenced_generation(&self, claim: &RuntimeClaim) -> Result<RuntimeClaim>;

    /// Requires the exact current, execution-ready writer.
    async fn verify_generation(&self, runtime: &Runtime) -> Result<()>;

    /// Claims one pending durable retirement without consuming its evidence.
    async fn claim_retirement(
        &self,
        claimant: Uuid,
        duration: TimeDelta,
    ) -> Result<Option<RuntimeRetirement>>;

    /// Requires a current, unchanged retirement attempt.
    async fn verify_retirement(&self, retirement: &RuntimeRetirement) -> Result<()>;

    /// Settles only the verified retirement after the provider's confirmation.
    async fn finish_retirement(&self, retirement: &RuntimeRetirement) -> Result<()>;

    /// Claims due recovery work with a new attempt revision.
    async fn claim_wake(&self, claimant: Uuid, duration: TimeDelta) -> Result<Option<WakeClaim>>;

    /// Persists the earliest due time and invalidates an older wake attempt.
    async fn schedule(&self, agent_id: Uuid, due_at: DateTime<Utc>) -> Result<()>;

    /// Settles the exact wake revision without erasing newer work.
    async fn settle_wake(&self, claim: &WakeClaim, next: Option<DateTime<Utc>>) -> Result<()>;
}

#[async_trait]
impl CoordinationStore for Store {
    async fn agent(&self, owner: &Owner, id: Uuid) -> Result<Agent> {
        Store::agent(self, owner, id).await
    }

    async fn enqueue(
        &self,
        owner: &Owner,
        agent_id: Uuid,
        submission: &CommandSubmission,
        requirements: &CommandRequirements,
    ) -> Result<CommandReceipt> {
        Store::enqueue(self, owner, agent_id, submission, requirements).await
    }

    async fn claim_command(
        &self,
        claimant: Uuid,
        duration: TimeDelta,
    ) -> Result<Option<PendingCommand>> {
        Store::claim_command(self, claimant, duration).await
    }

    async fn accept_command(&self, command: &PendingCommand, receipt: &Value) -> Result<()> {
        Store::accept_command(self, command, receipt).await
    }

    async fn reject_command(&self, command: &PendingCommand, problem: &ApiProblem) -> Result<()> {
        Store::reject_command(self, command, problem).await
    }

    async fn defer_command(&self, command: &PendingCommand, due_at: DateTime<Utc>) -> Result<()> {
        Store::defer_command(self, command, due_at).await
    }

    async fn authorize(
        &self,
        agent_id: Uuid,
        grant_id: Uuid,
        version: i64,
        scopes: &[CapabilityScope],
    ) -> Result<Grant> {
        Store::authorize(self, agent_id, grant_id, version, scopes).await
    }

    async fn claim_runtime(
        &self,
        agent_id: Uuid,
        controller_id: Uuid,
        duration: TimeDelta,
    ) -> Result<RuntimeClaim> {
        Store::claim_runtime(self, agent_id, controller_id, duration).await
    }

    async fn instance_intent(&self, claim: &RuntimeClaim) -> Result<Option<InstanceIntent>> {
        Store::instance_intent(self, claim).await
    }

    async fn begin_instance(
        &self,
        claim: &RuntimeClaim,
        plan: &InstancePlan,
    ) -> Result<InstanceIntent> {
        Store::begin_instance(self, claim, plan).await
    }

    async fn verify_instance_intent(&self, intent: &InstanceIntent) -> Result<()> {
        Store::verify_instance_intent(self, intent).await
    }

    async fn admit_instance(
        &self,
        intent: &InstanceIntent,
        admission: &Admission,
    ) -> Result<InstanceIntent> {
        Store::admit_instance(self, intent, admission).await
    }

    async fn transition_instance(
        &self,
        intent: &InstanceIntent,
        next: InstancePhase,
    ) -> Result<InstanceIntent> {
        Store::transition_instance(self, intent, next).await
    }

    async fn finish_instance(
        &self,
        intent: &InstanceIntent,
        proof: &PreviousWriter,
        completed_head: Option<&AnchorHead>,
        problem: Option<&ApiProblem>,
    ) -> Result<InstanceIntent> {
        Store::finish_instance(self, intent, proof, completed_head, problem).await
    }

    async fn advance_fenced_generation(&self, claim: &RuntimeClaim) -> Result<RuntimeClaim> {
        Store::advance_fenced_generation(self, claim).await
    }

    async fn verify_generation(&self, runtime: &Runtime) -> Result<()> {
        Store::verify_generation(self, runtime).await
    }

    async fn claim_retirement(
        &self,
        claimant: Uuid,
        duration: TimeDelta,
    ) -> Result<Option<RuntimeRetirement>> {
        Store::claim_retirement(self, claimant, duration).await
    }

    async fn verify_retirement(&self, retirement: &RuntimeRetirement) -> Result<()> {
        Store::verify_retirement(self, retirement).await
    }

    async fn finish_retirement(&self, retirement: &RuntimeRetirement) -> Result<()> {
        Store::finish_retirement(self, retirement).await
    }

    async fn claim_wake(&self, claimant: Uuid, duration: TimeDelta) -> Result<Option<WakeClaim>> {
        Store::claim_wake(self, claimant, duration).await
    }

    async fn schedule(&self, agent_id: Uuid, due_at: DateTime<Utc>) -> Result<()> {
        Store::schedule(self, agent_id, due_at).await
    }

    async fn settle_wake(&self, claim: &WakeClaim, next: Option<DateTime<Utc>>) -> Result<()> {
        Store::settle_wake(self, claim, next).await
    }
}
