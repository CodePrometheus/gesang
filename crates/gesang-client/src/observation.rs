//! Dedicated read connections apply upstream deltas before exposing native presentation batches.

use std::{collections::BTreeSet, sync::Arc};

use chopin_protocol::{GoalSnapshot, ProtocolError, StateObserver, StateUpdate};
use serde_json::Value;
use tokio::sync::{Mutex, watch};

use crate::{Client, ClientError, GoalView, TranscriptPageView, TranscriptUpdate};

struct Observation {
    source: Option<StateObserver>,
    first: bool,
}

/// A dedicated goal reader; closing it never stops the cloud task.
#[derive(uniffi::Object)]
pub struct GoalObservation {
    inner: Mutex<Observation>,
    cancel: watch::Sender<bool>,
    agent_id: String,
    cache: Arc<crate::goals::GoalCache>,
}

/// A dedicated conversation reader that transfers changed entries across UniFFI.
#[derive(uniffi::Object)]
pub struct ConversationObservation {
    inner: Mutex<Observation>,
    cancel: watch::Sender<bool>,
}

impl std::fmt::Debug for GoalObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoalObservation").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ConversationObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConversationObservation")
            .finish_non_exhaustive()
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl Client {
    /// Acquire a new authoritative snapshot followed by real goal changes.
    pub async fn observe_goal(
        &self,
        agent_id: String,
        goal_id: String,
    ) -> Result<Arc<GoalObservation>, ClientError> {
        let source = self
            .engine(&agent_id)
            .await?
            .observe_goal(&goal_id)
            .await
            .map_err(protocol_error)?;
        Ok(Arc::new(GoalObservation {
            inner: Mutex::new(Observation {
                source: Some(source),
                first: true,
            }),
            cancel: watch::channel(false).0,
            agent_id,
            cache: self.goals.clone(),
        }))
    }

    /// Observe the source's bounded live window; use history pages for earlier entries.
    pub async fn observe_conversation(
        &self,
        agent_id: String,
        conversation_id: u64,
    ) -> Result<Arc<ConversationObservation>, ClientError> {
        let source = self
            .engine(&agent_id)
            .await?
            .observe_conversation(conversation_id)
            .await
            .map_err(protocol_error)?;
        Ok(Arc::new(ConversationObservation {
            inner: Mutex::new(Observation {
                source: Some(source),
                first: true,
            }),
            cancel: watch::channel(false).0,
        }))
    }

    /// Fetch one real history page, including fork ancestors outside the live window.
    pub async fn transcript_page(
        &self,
        agent_id: String,
        conversation_id: u64,
        limit: u64,
        cursor: Option<String>,
        at_or_before: Option<u64>,
    ) -> Result<TranscriptPageView, ClientError> {
        if limit == 0 {
            return Err(ClientError::InvalidInput {
                field: "limit".into(),
            });
        }
        let cursor = cursor
            .map(|value| serde_json::from_str(&value))
            .transpose()
            .map_err(|_| ClientError::InvalidInput {
                field: "cursor".into(),
            })?;
        let mut channel = self.engine(&agent_id).await?;
        let result = channel
            .entries(conversation_id, limit, cursor, at_or_before)
            .await
            .map_err(protocol_error);
        let _ = channel.close().await;
        let page = result?;
        Ok(TranscriptPageView {
            entries: page.entries.into_iter().map(Into::into).collect(),
            cursor: page
                .cursor
                .map(|value| serde_json::to_string(&value))
                .transpose()
                .map_err(|_| ClientError::InvalidResponse)?,
        })
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl GoalObservation {
    /// Read the initial snapshot, then wait for a real source change. Calls are serialized.
    pub async fn next(&self) -> Result<GoalView, ClientError> {
        let mut cancel = self.cancel.subscribe();
        let mut inner = self.inner.lock().await;
        let _ = advance(&mut inner, &mut cancel).await?;
        let value = inner
            .source
            .as_ref()
            .ok_or(ClientError::ObservationClosed)?
            .state()
            .map_err(protocol_error)?;
        let snapshot: GoalSnapshot = serde_json::from_value(
            value
                .get("snapshot")
                .ok_or(ClientError::InvalidResponse)?
                .clone(),
        )
        .map_err(|_| ClientError::InvalidResponse)?;
        self.cache.merge(&self.agent_id, snapshot)
    }

    /// Interrupt a pending read and release this connection without sending a task command.
    pub async fn disconnect(&self) {
        self.cancel.send_replace(true);
        let source = self.inner.lock().await.source.take();
        if let Some(source) = source {
            let _ = source.close().await;
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl ConversationObservation {
    /// Return the first live window or only entries touched by the next source batch.
    pub async fn next(&self) -> Result<TranscriptUpdate, ClientError> {
        let mut cancel = self.cancel.subscribe();
        let mut inner = self.inner.lock().await;
        let update = advance(&mut inner, &mut cancel).await?;
        let value = inner
            .source
            .as_ref()
            .ok_or(ClientError::ObservationClosed)?
            .state()
            .map_err(protocol_error)?;
        project_update(value, update.as_ref())
    }

    /// Interrupt a pending read and release this connection; execution remains in the cloud.
    pub async fn disconnect(&self) {
        self.cancel.send_replace(true);
        let source = self.inner.lock().await.source.take();
        if let Some(source) = source {
            let _ = source.close().await;
        }
    }
}

async fn advance(
    inner: &mut Observation,
    cancel: &mut watch::Receiver<bool>,
) -> Result<Option<StateUpdate>, ClientError> {
    if *cancel.borrow() {
        return Err(ClientError::ObservationClosed);
    }
    if inner.first {
        inner.first = false;
        return Ok(None);
    }
    let source = inner
        .source
        .as_mut()
        .ok_or(ClientError::ObservationClosed)?;
    tokio::select! {
        _ = cancel.changed() => Err(ClientError::ObservationClosed),
        result = source.next() => {
            let update = result.map_err(protocol_error)?;
            if !update.retired_members.is_empty() { return Err(ClientError::ObservationClosed); }
            Ok(Some(update))
        }
    }
}

fn project_update(
    state: &Value,
    update: Option<&StateUpdate>,
) -> Result<TranscriptUpdate, ClientError> {
    let view = state.get("view").ok_or(ClientError::InvalidResponse)?;
    let entries = view["entries"]
        .as_array()
        .ok_or(ClientError::InvalidResponse)?;
    let paths = update.and_then(|value| value.changed_paths.get("view"));
    let entire = update.is_none()
        || paths.is_some_and(|paths| {
            paths.iter().any(|path| {
                path.is_empty()
                    || (path[0] == "entries" && path.get(1).and_then(Value::as_u64).is_none())
            })
        });
    let changed: BTreeSet<usize> = paths
        .into_iter()
        .flatten()
        .filter_map(|path| {
            (path.first().is_some_and(|part| part == "entries"))
                .then(|| path.get(1).and_then(Value::as_u64))
                .flatten()
                .and_then(|index| usize::try_from(index).ok())
        })
        .collect();
    let projected = entries
        .iter()
        .enumerate()
        .filter(|(index, _)| entire || changed.contains(index))
        .map(|(_, entry)| crate::transcript::decode_entry(entry))
        .collect::<Result<Vec<_>, _>>()?;
    let visible_entry_ids = entries
        .iter()
        .map(|entry| entry["id"].as_u64().ok_or(ClientError::InvalidResponse))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(TranscriptUpdate {
        revision: update.map_or(0, |value| value.revision),
        entries: projected,
        visible_entry_ids,
        older_before: view.get("before").and_then(Value::as_u64),
        synchronized_at_ms: crate::unix_milliseconds(jiff::Timestamp::now()),
        live: crate::transcript::project_live(
            view.get("docs").and_then(|docs| docs.get("pi.live")),
        ),
    })
}

pub(crate) fn protocol_error(error: ProtocolError) -> ClientError {
    match error.http_status() {
        Some(401) => return ClientError::AuthenticationRequired,
        Some(403) => return ClientError::PermissionDenied,
        Some(404) => return ClientError::NotFound,
        _ => {}
    }
    match error {
        ProtocolError::Closed => ClientError::ConnectionUnavailable,
        ProtocolError::Transport(_) => ClientError::ConnectionUnavailable,
        ProtocolError::Method { error, .. } => {
            let code = error.get("code").and_then(Value::as_str).filter(|code| {
                !code.is_empty()
                    && code.len() <= 128
                    && code
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'.')
            });
            code.map_or(ClientError::InvalidResponse, |code| ClientError::Rejected {
                code: code.to_owned(),
            })
        }
        _ => ClientError::InvalidResponse,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn token_change_projects_only_the_changed_entry() {
        let state = json!({"view":{"entries":[
            {"id":1,"conversationId":1,"kind":"user","model":[{"role":"user","content":"hello"}]},
            {"id":2,"conversationId":1,"kind":"assistant","model":[{"role":"assistant","content":[{"type":"text","text":"Hello."}]}]}
        ],"before":0}});
        let update = StateUpdate {
            revision: 7,
            changed_paths: [(
                "view".into(),
                vec![vec![
                    json!("entries"),
                    json!(1),
                    json!("model"),
                    json!(0),
                    json!("content"),
                    json!(0),
                    json!("text"),
                ]],
            )]
            .into(),
            retired_members: Vec::new(),
        };
        let projected = project_update(&state, Some(&update));
        assert!(projected.is_ok());
        if let Ok(projected) = projected {
            assert_eq!(projected.entries.len(), 1);
            assert_eq!(projected.entries[0].entry_id, 2);
            assert_eq!(projected.visible_entry_ids, vec![1, 2]);
        }
    }

    #[tokio::test]
    async fn close_signal_interrupts_reads_without_waiting_for_a_network_frame() {
        let (cancel, mut receiver) = watch::channel(false);
        let mut inner = Observation {
            source: None,
            first: false,
        };
        cancel.send_replace(true);
        assert!(matches!(
            advance(&mut inner, &mut receiver).await,
            Err(ClientError::ObservationClosed)
        ));
    }
}
