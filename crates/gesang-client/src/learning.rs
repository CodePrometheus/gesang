//! Owner-driven RSI operations use the upstream project's existing loop contract.

use chopin_protocol::{LoopSettings, LoopSettingsPatch};

use crate::{Client, ClientError};

/// One real evaluated learning change.
#[derive(Clone, Debug, uniffi::Record)]
pub struct LearningChange {
    /// Source change identity, used for explicit undo.
    pub id: String,
    /// Original source timestamp representation.
    pub added: String,
    /// Source extensible change kind.
    pub kind: String,
    /// Archived changes are not currently adopted.
    pub archived: bool,
    /// Actual change description.
    pub text: String,
}

/// A recurring correction awaiting the owner's learning policy.
#[derive(Clone, Debug, uniffi::Record)]
pub struct LearningCorrection {
    /// Source policy identity.
    pub key: String,
    /// Number of relevant source sessions.
    pub sessions: u64,
    /// Real examples supplied by the execution service.
    pub examples: Vec<String>,
}

/// A held candidate; approval only permits evaluation, not adoption.
#[derive(Clone, Debug, uniffi::Record)]
pub struct LearningHeld {
    /// Source candidate identity.
    pub id: String,
    /// Actual failure or audit reason.
    pub failure: String,
    /// Source extensible candidate kind.
    pub kind: String,
    /// Target skill when the source supplies one.
    pub skill: Option<String>,
    /// Proposed change.
    pub text: String,
    /// Source prediction to be tested by evaluation.
    pub prediction: String,
    /// Audit reasons requiring the owner's decision.
    pub reasons: Vec<String>,
}

/// Current upstream learning configuration.
#[derive(Clone, Debug, uniffi::Record)]
pub struct LearningSettingsView {
    /// Configured weekly budget; absence does not authorize an evaluation to run.
    pub weekly_budget_usd: Option<f64>,
    /// Configured regular model reference.
    pub regular_model: Option<String>,
    /// Configured strong model reference.
    pub strong_model: Option<String>,
    /// Whether candidates must wait for an explicit owner review.
    pub hold_for_review: bool,
}

/// One refresh of actual RSI reads; these separate upstream calls are not an atomic transaction.
#[derive(Clone, Debug, uniffi::Record)]
pub struct LearningView {
    /// Upstream learning version from the standing read.
    pub version: u64,
    /// Number of changes currently adopted.
    pub in_use: u64,
    /// Number of archived changes.
    pub archived: u64,
    /// Number of correction kinds reported by the source.
    pub kinds: u64,
    /// Number of recurring correction kinds.
    pub recurring: u64,
    /// Number of shelved kinds.
    pub shelved: u64,
    /// Number of source correction records.
    pub corrections_count: u64,
    /// Number of candidates waiting for review.
    pub held_count: u64,
    /// Actual spending recorded by the source.
    pub spent_usd: f64,
    /// Source configuration, not a client-side spending guarantee.
    pub settings: LearningSettingsView,
    /// Real active and archived changes.
    pub changes: Vec<LearningChange>,
    /// Recurring corrections awaiting owner policy.
    pub corrections: Vec<LearningCorrection>,
    /// Held candidates awaiting review.
    pub held: Vec<LearningHeld>,
    /// Device time after these reads completed.
    pub synchronized_at_ms: i64,
}

/// An explicit owner action; none of these operations is retried automatically.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum LearningAction {
    /// Permit evaluation of a held candidate; evaluation may consume its configured budget.
    Approve {
        /// Original candidate identity.
        id: String,
    },
    /// Keep the current approach for a held candidate.
    Reject {
        /// Original candidate identity.
        id: String,
    },
    /// Archive a previously adopted change.
    Undo {
        /// Original change identity.
        id: String,
    },
    /// Set the learning policy for one recurring correction kind.
    AnswerCorrection {
        /// Original correction policy key.
        key: String,
        /// Whether the owner permits learning this kind.
        learn: bool,
    },
}

/// A nullable numeric setting edit; omission and clearing remain distinct.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum NumberChange {
    /// Leave the setting unchanged.
    Unchanged,
    /// Clear the source setting.
    Clear,
    /// Set an explicit finite, nonnegative value.
    Set {
        /// New setting value.
        value: f64,
    },
}

/// A nullable model reference edit.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum TextChange {
    /// Leave the setting unchanged.
    Unchanged,
    /// Use the source's default model.
    Clear,
    /// Use the explicit source-supported model reference.
    Set {
        /// New model reference.
        value: String,
    },
}

/// Explicit upstream configuration edits, without reset-by-omission behavior.
#[derive(Clone, Debug, uniffi::Record)]
pub struct LearningSettingsChange {
    /// Weekly budget edit; clearing does not cancel an evaluation already running.
    pub weekly_budget_usd: NumberChange,
    /// Regular model reference edit.
    pub regular_model: TextChange,
    /// Strong model reference edit.
    pub strong_model: TextChange,
    /// Review policy edit; absent leaves the current policy unchanged.
    pub hold_for_review: Option<bool>,
}

impl From<LoopSettings> for LearningSettingsView {
    fn from(value: LoopSettings) -> Self {
        Self {
            weekly_budget_usd: value.weekly_budget_usd,
            regular_model: value.regular_model,
            strong_model: value.strong_model,
            hold_for_review: value.hold_for_review,
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl Client {
    /// Refresh real learning standing, changes, reviews, policies, and settings.
    pub async fn learning(&self, agent_id: String) -> Result<LearningView, ClientError> {
        let mut channel = self.engine(&agent_id).await?;
        let result = async {
            let standing = channel.loop_standing().await?;
            let changes = channel.loop_changes().await?;
            let corrections = channel.loop_corrections().await?;
            let held = channel.loop_held().await?;
            let settings = channel.loop_settings().await?;
            Ok::<_, chopin_protocol::ProtocolError>(LearningView {
                version: standing.version,
                in_use: standing.in_use,
                archived: standing.archived,
                kinds: standing.kinds,
                recurring: standing.recurring,
                shelved: standing.shelved,
                corrections_count: standing.corrections,
                held_count: standing.held,
                spent_usd: standing.spent_usd,
                settings: settings.into(),
                changes: changes
                    .into_iter()
                    .map(|value| LearningChange {
                        id: value.id,
                        added: value.added,
                        kind: value.kind,
                        archived: value.archived,
                        text: value.text,
                    })
                    .collect(),
                corrections: corrections
                    .into_iter()
                    .map(|value| LearningCorrection {
                        key: value.key,
                        sessions: value.sessions,
                        examples: value.examples,
                    })
                    .collect(),
                held: held
                    .into_iter()
                    .map(|value| LearningHeld {
                        id: value.id,
                        failure: value.failure,
                        kind: value.kind,
                        skill: value.skill,
                        text: value.text,
                        prediction: value.prediction,
                        reasons: value.reasons,
                    })
                    .collect(),
                synchronized_at_ms: crate::unix_milliseconds(jiff::Timestamp::now()),
            })
        }
        .await;
        let _ = channel.close().await;
        result.map_err(crate::observation::protocol_error)
    }

    /// Apply the owner's explicit review or undo once; refresh after an uncertain response.
    pub async fn act_on_learning(
        &self,
        agent_id: String,
        action: LearningAction,
    ) -> Result<(), ClientError> {
        let mut channel = self.engine(&agent_id).await?;
        let result = match action {
            LearningAction::Approve { id } => channel.decide_loop_change(&id, true).await,
            LearningAction::Reject { id } => channel.decide_loop_change(&id, false).await,
            LearningAction::Undo { id } => channel.undo_loop_change(&id).await,
            LearningAction::AnswerCorrection { key, learn } => {
                channel.answer_loop_correction(&key, learn).await
            }
        };
        let _ = channel.close().await;
        result.map_err(crate::observation::protocol_error)
    }

    /// Apply explicit learning settings once; no client background process starts evaluations.
    pub async fn update_learning_settings(
        &self,
        agent_id: String,
        change: LearningSettingsChange,
    ) -> Result<LearningSettingsView, ClientError> {
        let weekly_budget_usd = match change.weekly_budget_usd {
            NumberChange::Unchanged => None,
            NumberChange::Clear => Some(None),
            NumberChange::Set { value } if value.is_finite() && value >= 0.0 => Some(Some(value)),
            _ => {
                return Err(ClientError::InvalidInput {
                    field: "weeklyBudgetUsd".into(),
                });
            }
        };
        let patch = LoopSettingsPatch {
            weekly_budget_usd,
            regular_model: text_edit(change.regular_model)?,
            strong_model: text_edit(change.strong_model)?,
            hold_for_review: change.hold_for_review,
        };
        let mut channel = self.engine(&agent_id).await?;
        let result = channel.set_loop_settings(&patch).await;
        let _ = channel.close().await;
        result
            .map(Into::into)
            .map_err(crate::observation::protocol_error)
    }
}

fn text_edit(edit: TextChange) -> Result<Option<Option<String>>, ClientError> {
    match edit {
        TextChange::Unchanged => Ok(None),
        TextChange::Clear => Ok(Some(None)),
        TextChange::Set { value } if !value.trim().is_empty() => Ok(Some(Some(value))),
        _ => Err(ClientError::InvalidInput {
            field: "model".into(),
        }),
    }
}
