//! Submission payloads retain their original identity across a lost response.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use chopin_protocol::{
    GoalCommand, GoalContinuation, GoalGrant, GoalPage, GoalSnapshot, GoalStatus,
};
use gesang_api::{CommandAuthorization, CommandReceipt, CommandSubmission};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Client, ClientError, GrantView, ReceiptView};

/// Authoritative execution phase, independent of device connectivity and receipt state.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum GoalPhase {
    /// Execution accepted the goal but has not begun advancing it.
    Accepted,
    /// Work can advance or previously started work is being coordinated.
    Running,
    /// A persisted question needs a response bound to this goal.
    WaitingUser,
    /// Valid delegated authority is needed before more work starts.
    WaitingAuthorization,
    /// A persisted time or event condition prevents further work for now.
    WaitingTime,
    /// A required runtime, definition, or operation result is unavailable.
    WaitingDependency,
    /// Stop intent was accepted; in-flight effects still need coordination.
    Stopping,
    /// The engine verified completion evidence against its explicit conditions.
    Completed,
    /// The engine confirmed the stopping boundary.
    Stopped,
    /// The engine recorded an unrecoverable failure.
    Failed,
}

/// Execution evidence identifying a criterion and its successful source record.
#[derive(Clone, Debug, uniffi::Record)]
pub struct EvidenceView {
    /// Whether the verified source is an assistant delivery or a completed tool action.
    pub kind: EvidenceKind,
    /// Zero-based index into the goal's explicit completion conditions.
    pub criterion: u64,
    /// Original durable conversation entry supporting the condition.
    pub entry_id: u64,
    /// Optional verified workspace artifact location.
    pub artifact_path: Option<String>,
    /// Digest verified by the execution environment for that artifact.
    pub artifact_sha256: Option<String>,
}

/// Kind of durable source supporting a completion decision.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum EvidenceKind {
    /// Real assistant text delivered in the goal's own conversation.
    Assistant,
    /// A completed tool result, optionally carrying a verified artifact.
    Tool,
}

/// Presentation projection of one upstream snapshot; no execution state is synthesized here.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GoalView {
    /// Stable engine-local reference, scoped by the logical agent.
    pub goal_id: String,
    /// Conversation reference in the same execution store.
    pub conversation_id: u64,
    /// Monotonic state version used to reject stale updates.
    pub version: u64,
    /// Engine-owned execution phase.
    pub phase: GoalPhase,
    /// User's original objective.
    pub objective: String,
    /// Explicit completion conditions; initially empty when still being established.
    pub completion_criteria: Vec<String>,
    /// Persisted reason the goal cannot currently advance.
    pub waiting_reason: Option<String>,
    /// Question reference required when answering a specific wait.
    pub question_id: Option<String>,
    /// Absolute persisted wake deadline in Unix milliseconds.
    pub waiting_until_ms: Option<f64>,
    /// Engine's result summary, absent before a verified finish decision.
    pub result_summary: Option<String>,
    /// Verifiable source references supporting completion.
    pub evidence: Vec<EvidenceView>,
    /// Engine state update time in Unix milliseconds.
    pub updated_at_ms: f64,
    /// Last successful observation time, retained verbatim in offline cache.
    pub synchronized_at_ms: i64,
}

/// One authoritative source page; a cursor can follow an empty page.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GoalPageView {
    /// Observed goals from this page only.
    pub goals: Vec<GoalView>,
    /// Opaque source cursor encoded as JSON; callers pass it back unchanged.
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CachedGoal {
    snapshot: GoalSnapshot,
    #[serde(serialize_with = "gesang_api::timestamp::serialize")]
    synchronized_at: jiff::Timestamp,
}

#[derive(Default, Debug)]
pub(crate) struct GoalCache(Mutex<BTreeMap<(String, String), CachedGoal>>);

impl GoalCache {
    pub(crate) fn merge(
        &self,
        agent_id: &str,
        snapshot: GoalSnapshot,
    ) -> Result<GoalView, ClientError> {
        let mut goals = self.0.lock().map_err(|_| ClientError::InvalidResponse)?;
        let key = (agent_id.to_owned(), snapshot.goal_id.clone());
        if let Some(current) = goals.get(&key) {
            if snapshot.version < current.snapshot.version {
                return Ok(current.view());
            }
            if snapshot.version == current.snapshot.version && snapshot != current.snapshot {
                return Err(ClientError::InvalidResponse);
            }
        }
        let cached = CachedGoal {
            snapshot,
            synchronized_at: jiff::Timestamp::now(),
        };
        let view = cached.view();
        goals.insert(key, cached);
        Ok(view)
    }
}

impl CachedGoal {
    fn view(&self) -> GoalView {
        let mut view = GoalView::from(self.snapshot.clone());
        view.synchronized_at_ms = crate::unix_milliseconds(self.synchronized_at);
        view
    }
}

impl From<GoalSnapshot> for GoalView {
    fn from(value: GoalSnapshot) -> Self {
        let (waiting_reason, question_id, waiting_until_ms) = match value.waiting {
            Some(waiting) => (Some(waiting.reason), waiting.question_id, waiting.until),
            None => (None, None, None),
        };
        let (result_summary, evidence) = match value.result {
            Some(result) => (
                Some(result.summary),
                result
                    .evidence
                    .into_iter()
                    .map(|e| EvidenceView {
                        kind: match &e {
                            chopin_protocol::GoalEvidence::Assistant { .. } => {
                                EvidenceKind::Assistant
                            }
                            chopin_protocol::GoalEvidence::Tool { .. } => EvidenceKind::Tool,
                        },
                        criterion: e.criterion(),
                        entry_id: e.entry_id(),
                        artifact_path: e.artifact().map(|a| a.path.clone()),
                        artifact_sha256: e.artifact().map(|a| a.sha256.clone()),
                    })
                    .collect(),
            ),
            None => (None, Vec::new()),
        };
        Self {
            goal_id: value.goal_id,
            conversation_id: value.conversation_id,
            version: value.version,
            phase: match value.status {
                GoalStatus::Accepted => GoalPhase::Accepted,
                GoalStatus::Running => GoalPhase::Running,
                GoalStatus::WaitingUser => GoalPhase::WaitingUser,
                GoalStatus::WaitingAuthorization => GoalPhase::WaitingAuthorization,
                GoalStatus::WaitingTime => GoalPhase::WaitingTime,
                GoalStatus::WaitingDependency => GoalPhase::WaitingDependency,
                GoalStatus::Stopping => GoalPhase::Stopping,
                GoalStatus::Completed => GoalPhase::Completed,
                GoalStatus::Stopped => GoalPhase::Stopped,
                GoalStatus::Failed => GoalPhase::Failed,
            },
            objective: value.objective,
            completion_criteria: value.completion_criteria,
            waiting_reason,
            question_id,
            waiting_until_ms,
            result_summary,
            evidence,
            updated_at_ms: value.updated_at,
            synchronized_at_ms: crate::unix_milliseconds(jiff::Timestamp::now()),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredCommand {
    version: u8,
    agent_id: Uuid,
    submission: CommandSubmission,
}

/// Immutable delivery data retained before submission and reused after a lost response.
#[derive(uniffi::Object)]
pub struct PendingCommand {
    agent_id: Uuid,
    submission: CommandSubmission,
}

impl std::fmt::Debug for PendingCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingCommand")
            .field("request_id", &self.submission.request_id)
            .finish_non_exhaustive()
    }
}

#[uniffi::export]
impl PendingCommand {
    /// Original delivery identity; retrying must preserve it.
    pub fn request_id(&self) -> String {
        self.submission.request_id.to_string()
    }
    /// Logical agent that owns this delivery.
    pub fn agent_id(&self) -> String {
        self.agent_id.to_string()
    }

    /// Encode an opaque record for encrypted platform-keystore storage.
    pub fn encode(&self) -> Result<Vec<u8>, ClientError> {
        serde_json::to_vec(&StoredCommand {
            version: 1,
            agent_id: self.agent_id,
            submission: self.submission.clone(),
        })
        .map_err(|_| ClientError::InvalidResponse)
    }

    #[uniffi::constructor]
    /// Restore the exact supported command instead of generating a new request identifier.
    pub fn restore(encoded: Vec<u8>) -> Result<Arc<Self>, ClientError> {
        let stored: StoredCommand =
            serde_json::from_slice(&encoded).map_err(|_| ClientError::InvalidResponse)?;
        if stored.version != 1 {
            return Err(ClientError::InvalidResponse);
        }
        let command: GoalCommand = serde_json::from_value(stored.submission.command.clone())
            .map_err(|_| ClientError::InvalidResponse)?;
        command
            .validate()
            .map_err(|_| ClientError::InvalidResponse)?;
        Ok(Arc::new(Self {
            agent_id: stored.agent_id,
            submission: stored.submission,
        }))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl Client {
    /// Prepare a goal without network effects; persist the returned delivery before sending.
    pub fn prepare_goal(
        &self,
        agent_id: String,
        grant: GrantView,
        objective: String,
        completion_criteria: Vec<String>,
    ) -> Result<Arc<PendingCommand>, ClientError> {
        if objective.trim().is_empty() {
            return Err(ClientError::InvalidInput {
                field: "objective".into(),
            });
        }
        let version = u64::try_from(grant.version).map_err(|_| ClientError::InvalidInput {
            field: "grantVersion".into(),
        })?;
        let command = GoalCommand::submit(
            objective,
            completion_criteria,
            GoalGrant {
                id: grant.id.clone(),
                version,
            },
        );
        prepare_execution(agent_id, grant, command)
    }

    /// Start explicit new work using a terminal goal's context while preserving its record.
    pub fn prepare_continuation(
        &self,
        agent_id: String,
        grant: GrantView,
        source_goal_id: String,
        source_version: u64,
        objective: String,
        completion_criteria: Vec<String>,
    ) -> Result<Arc<PendingCommand>, ClientError> {
        if objective.trim().is_empty() {
            return Err(ClientError::InvalidInput {
                field: "objective".into(),
            });
        }
        let version = u64::try_from(grant.version).map_err(|_| ClientError::InvalidInput {
            field: "grantVersion".into(),
        })?;
        let command = GoalCommand::continue_goal(
            objective,
            completion_criteria,
            GoalGrant {
                id: grant.id.clone(),
                version,
            },
            GoalContinuation {
                goal_id: source_goal_id,
                expected_version: source_version,
            },
        );
        prepare_execution(agent_id, grant, command)
    }

    /// Bind additional input to the goal version and its persisted question, when present.
    pub fn prepare_input(
        &self,
        agent_id: String,
        goal_id: String,
        expected_version: u64,
        text: String,
        question_id: Option<String>,
    ) -> Result<Arc<PendingCommand>, ClientError> {
        if text.trim().is_empty() {
            return Err(ClientError::InvalidInput {
                field: "text".into(),
            });
        }
        prepare(
            agent_id,
            CommandAuthorization::Owner,
            GoalCommand::Input {
                goal_id,
                expected_version,
                text,
                question_id,
                grant: None,
            },
        )
    }

    /// Prepare stop intent without claiming that running effects have stopped.
    pub fn prepare_stop(
        &self,
        agent_id: String,
        goal_id: String,
        expected_version: u64,
    ) -> Result<Arc<PendingCommand>, ClientError> {
        prepare(
            agent_id,
            CommandAuthorization::Owner,
            GoalCommand::Stop {
                goal_id,
                expected_version,
            },
        )
    }

    /// Explicitly adopt a replacement execution grant for the original waiting goal.
    pub fn prepare_resume(
        &self,
        agent_id: String,
        grant: GrantView,
        goal_id: String,
        expected_version: u64,
        text: String,
        question_id: Option<String>,
    ) -> Result<Arc<PendingCommand>, ClientError> {
        if text.trim().is_empty() {
            return Err(ClientError::InvalidInput {
                field: "text".into(),
            });
        }
        let version = u64::try_from(grant.version).map_err(|_| ClientError::InvalidInput {
            field: "grantVersion".into(),
        })?;
        let command = GoalCommand::Input {
            goal_id,
            expected_version,
            text,
            question_id,
            grant: Some(GoalGrant {
                id: grant.id.clone(),
                version,
            }),
        };
        prepare_execution(agent_id, grant, command)
    }

    /// Deliver immutable content; a transport error leaves acceptance unknown until queried.
    pub async fn submit(&self, command: Arc<PendingCommand>) -> Result<ReceiptView, ClientError> {
        let agent_id = command.agent_id.to_string();
        let receipt: CommandReceipt = self
            .send_json(
                Method::POST,
                &["v1", "agents", &agent_id, "commands"],
                &command.submission,
            )
            .await?;
        if receipt.request_id != command.submission.request_id
            || receipt.agent_id != command.agent_id
        {
            return Err(ClientError::InvalidResponse);
        }
        receipt.try_into()
    }

    /// Fetch one snapshot and prevent an older response from replacing newer observed state.
    pub async fn goal(&self, agent_id: String, goal_id: String) -> Result<GoalView, ClientError> {
        crate::transport::checked_id(&agent_id, "agentId")?;
        let snapshot: GoalSnapshot = self
            .get(&["v1", "agents", &agent_id, "goals", &goal_id])
            .await?;
        if snapshot.goal_id != goal_id || !snapshot.updated_at.is_finite() {
            return Err(ClientError::InvalidResponse);
        }
        self.goals.merge(&agent_id, snapshot)
    }

    /// Read the execution service's goals without inventing platform-owned terminal state.
    pub async fn goals(
        &self,
        agent_id: String,
        limit: u64,
        cursor: Option<String>,
    ) -> Result<GoalPageView, ClientError> {
        crate::transport::checked_id(&agent_id, "agentId")?;
        if limit == 0 {
            return Err(ClientError::InvalidInput {
                field: "limit".into(),
            });
        }
        let mut url = self.url(&["v1", "agents", &agent_id, "goals"])?;
        url.query_pairs_mut()
            .append_pair("limit", &limit.to_string());
        if let Some(cursor) = cursor {
            let _: serde_json::Value =
                serde_json::from_str(&cursor).map_err(|_| ClientError::InvalidInput {
                    field: "cursor".into(),
                })?;
            url.query_pairs_mut().append_pair("cursor", &cursor);
        }
        let response = self
            .request(Method::GET, url)
            .send()
            .await
            .map_err(|_| ClientError::ConnectionUnavailable)?;
        let page: GoalPage = crate::transport::decode_response(response).await?;
        let goals = page
            .goals
            .into_iter()
            .map(|snapshot| self.goals.merge(&agent_id, snapshot))
            .collect::<Result<Vec<_>, _>>()?;
        let cursor = page
            .cursor
            .map(|value| serde_json::to_string(&value))
            .transpose()
            .map_err(|_| ClientError::InvalidResponse)?;
        Ok(GoalPageView { goals, cursor })
    }

    /// Encode observed state for encrypted device storage; timestamps remain observations.
    pub fn cached_goals(&self, agent_id: String) -> Result<Vec<u8>, ClientError> {
        let cache = self
            .goals
            .0
            .lock()
            .map_err(|_| ClientError::InvalidResponse)?;
        let goals: Vec<_> = cache
            .iter()
            .filter(|((agent, _), _)| agent == &agent_id)
            .map(|(_, cached)| cached)
            .collect();
        serde_json::to_vec(&goals).map_err(|_| ClientError::InvalidResponse)
    }

    /// Restore last-known state; this does not assert a successful server synchronization.
    pub fn restore_cached_goals(
        &self,
        agent_id: String,
        data: Vec<u8>,
    ) -> Result<Vec<GoalView>, ClientError> {
        crate::transport::checked_id(&agent_id, "agentId")?;
        let restored: Vec<CachedGoal> =
            serde_json::from_slice(&data).map_err(|_| ClientError::InvalidResponse)?;
        let mut cache = self
            .goals
            .0
            .lock()
            .map_err(|_| ClientError::InvalidResponse)?;
        let mut views = Vec::new();
        for observed in restored {
            let key = (agent_id.clone(), observed.snapshot.goal_id.clone());
            match cache.get(&key) {
                Some(current)
                    if current.snapshot.version == observed.snapshot.version
                        && current.snapshot != observed.snapshot =>
                {
                    return Err(ClientError::InvalidResponse);
                }
                Some(current) if current.snapshot.version > observed.snapshot.version => {
                    views.push(current.view())
                }
                _ => {
                    views.push(observed.view());
                    cache.insert(key, observed);
                }
            }
        }
        Ok(views)
    }
}

fn prepare_execution(
    agent_id: String,
    grant: GrantView,
    command: GoalCommand,
) -> Result<Arc<PendingCommand>, ClientError> {
    if grant.agent_id != agent_id || grant.revoked_at.is_some() || grant.version < 1 {
        return Err(ClientError::PermissionDenied);
    }
    let grant_id = crate::transport::checked_id(&grant.id, "grantId")?;
    prepare(
        agent_id,
        CommandAuthorization::Grant {
            id: grant_id,
            version: grant.version,
        },
        command,
    )
}

fn prepare(
    agent_id: String,
    authorization: CommandAuthorization,
    command: GoalCommand,
) -> Result<Arc<PendingCommand>, ClientError> {
    command.validate().map_err(|_| ClientError::InvalidInput {
        field: "command".into(),
    })?;
    let agent_id = crate::transport::checked_id(&agent_id, "agentId")?;
    let submission = CommandSubmission {
        request_id: Uuid::new_v4(),
        authorization,
        command: serde_json::to_value(command).map_err(|_| ClientError::InvalidResponse)?,
    };
    Ok(Arc::new(PendingCommand {
        agent_id,
        submission,
    }))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::Permission;

    #[test]
    fn pending_command_survives_process_storage_without_replacing_its_identity() {
        let agent_id = Uuid::new_v4().to_string();
        let grant = GrantView {
            id: Uuid::new_v4().to_string(),
            agent_id: agent_id.clone(),
            version: 1,
            permission: Permission::Ask,
            scopes: Vec::new(),
            revoked_at: None,
        };
        let client = Client::new("https://agent.example".into(), None, false).unwrap();
        let pending = client
            .prepare_goal(
                agent_id,
                grant,
                "Complete the requested work".into(),
                Vec::new(),
            )
            .unwrap();
        let restored = PendingCommand::restore(pending.encode().unwrap()).unwrap();
        assert_eq!(restored.request_id(), pending.request_id());
        assert_eq!(restored.encode().unwrap(), pending.encode().unwrap());
    }
}
