//! Presentation records are projected from the upstream model payloads in Rust.

use chopin_protocol::TranscriptEntry;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ClientError;

/// An upstream model message's semantic role.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, uniffi::Enum)]
pub enum MessageRole {
    /// Input persisted by the execution service.
    User,
    /// Text or tool calls produced by a model.
    Assistant,
    /// A durable tool execution result.
    Tool,
    /// A system, reset, compaction, or extension record.
    Activity,
}

/// The public type of one content block.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, uniffi::Enum)]
pub enum ContentKind {
    /// Ordinary user-visible text.
    Text,
    /// Reasoning supplied by the upstream public transcript.
    Reasoning,
    /// A requested tool operation, not proof of success.
    ToolCall,
    /// Inline image data supplied by the actual transcript.
    Image,
    /// An extension block that this client cannot yet render directly.
    Unsupported,
}

/// One native content block; no wire delta is parsed in Kotlin.
#[derive(Clone, Debug, Serialize, Deserialize, uniffi::Record)]
pub struct ContentBlock {
    /// Public content category.
    pub kind: ContentKind,
    /// Text or a formatted extension value, depending on the category.
    pub text: String,
    /// Actual declared tool name, when this block is a call.
    pub tool_name: Option<String>,
    /// Original tool call identity, when present.
    pub operation_id: Option<String>,
    /// Inline image MIME type, when supplied by the source.
    pub image_mime_type: Option<String>,
    /// Base64 image data; never logged or treated as a network location.
    pub image_data: Option<String>,
}

/// An actual model message, projected without synthesizing completion or tool outcomes.
#[derive(Clone, Debug, Serialize, Deserialize, uniffi::Record)]
pub struct MessageView {
    /// Sender or activity category.
    pub role: MessageRole,
    /// Ordered content blocks from this message.
    pub content: Vec<ContentBlock>,
    /// Tool whose result this message records.
    pub tool_name: Option<String>,
    /// Original tool operation identity.
    pub operation_id: Option<String>,
    /// Explicit upstream tool or provider failure.
    pub failed: bool,
    /// The producer still marks this assistant message pending.
    pub pending: bool,
    /// Upstream timestamp in Unix milliseconds, absent for extensions without one.
    pub timestamp_ms: Option<f64>,
    /// Public provider failure text; not a transport diagnostic.
    pub error_message: Option<String>,
    /// Structured tool details formatted once in Rust for an optional inspector.
    pub details: Option<String>,
}

/// One materialized, durable entry in the owner's conversation history.
#[derive(Clone, Debug, Serialize, Deserialize, uniffi::Record)]
pub struct TranscriptEntryView {
    /// Stable entry identity; a later observation replaces the same entry.
    pub entry_id: u64,
    /// Source conversation, including an ancestor when following a fork.
    pub conversation_id: u64,
    /// Upstream extensible entry kind.
    pub kind: String,
    /// Ordered model messages, absent for an application-only activity.
    pub messages: Vec<MessageView>,
    /// Original application data, available in an explicit details inspector.
    pub details: Option<String>,
}

/// A batch of changed entries; the live window is not the entire transcript.
#[derive(Clone, Debug, uniffi::Record)]
pub struct TranscriptUpdate {
    /// Connection-local delivery count; never persist this as a resume cursor.
    pub revision: u64,
    /// Only new or changed entries, except the first snapshot of a new observation.
    pub entries: Vec<TranscriptEntryView>,
    /// Authoritative current live-window membership in source order.
    pub visible_entry_ids: Vec<u64>,
    /// Older history is available through this inclusive boundary.
    pub older_before: Option<u64>,
    /// Device observation time, separate from message creation time.
    pub synchronized_at_ms: i64,
    /// Actual committed generation and tool progress; absent between runs.
    pub live: Option<LiveView>,
}

/// A tool's live presentation phase, separate from its durable result and goal completion.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, uniffi::Enum)]
pub enum LiveToolPhase {
    /// The source has not started this call.
    Pending,
    /// The source reports an executing call.
    Running,
    /// The live call ended; inspect its real result to determine success.
    Done,
    /// A newer source supplied a phase this client does not recognize.
    Unknown,
}

/// Actual live tool output from pi-durable's public `pi.live` document.
#[derive(Clone, Debug, Serialize, Deserialize, uniffi::Record)]
pub struct LiveToolView {
    /// Original call identity, including the parent path for nested calls.
    pub operation_id: String,
    /// Declared source tool name.
    pub name: String,
    /// Source progress phase; done does not imply success.
    pub phase: LiveToolPhase,
    /// Retained running output, when supplied.
    pub output: Option<String>,
    /// Number of earlier output bytes omitted by the source's own retention policy.
    pub dropped_bytes: Option<u64>,
    /// Number of earlier output lines omitted by the source.
    pub dropped_lines: Option<u64>,
    /// Committed result entry, when this model-issued call has settled.
    pub entry_id: Option<u64>,
    /// Parent call for an actual nested tool, absent for model-issued calls.
    pub parent_operation_id: Option<String>,
    /// Structured progress or diagnostics, formatted for an explicit inspector.
    pub details: Option<String>,
}

/// Committed live progress, which can survive reconnects but is not a finish decision.
#[derive(Clone, Debug, Serialize, Deserialize, uniffi::Record)]
pub struct LiveView {
    /// A run still owns execution, including its after-run work.
    pub run_active: bool,
    /// The current committed partial assistant message.
    pub generation: Option<MessageView>,
    /// Source generation attempt number, when present.
    pub attempt: Option<u64>,
    /// Actual persisted retry deadline in Unix milliseconds.
    pub retry_at_ms: Option<f64>,
    /// Actual source retry reason.
    pub retry_error: Option<String>,
    /// Provider-side deferred polling deadline in Unix milliseconds.
    pub deferred_poll_at_ms: Option<f64>,
    /// Current model-issued and nested calls in source order.
    pub tools: Vec<LiveToolView>,
    /// The source is currently compacting conversation history.
    pub compacting: bool,
    /// Source reason for the newest run without an answer.
    pub failure_reason: Option<String>,
    /// Actual failure details retained by the source.
    pub failure_detail: Option<String>,
}

/// A source history page; callers round-trip the opaque cursor unchanged.
#[derive(Clone, Debug, uniffi::Record)]
pub struct TranscriptPageView {
    /// Source entries in its declared descending history order.
    pub entries: Vec<TranscriptEntryView>,
    /// Opaque JSON cursor, including on an empty nonterminal page.
    pub cursor: Option<String>,
}

impl From<TranscriptEntry> for TranscriptEntryView {
    fn from(entry: TranscriptEntry) -> Self {
        let internal = entry.kind == "chopin.goal-control";
        Self {
            entry_id: entry.id,
            conversation_id: entry.conversation_id,
            kind: entry.kind,
            messages: entry
                .model
                .unwrap_or_default()
                .iter()
                .map(|message| {
                    let mut view = project_message(message);
                    if internal {
                        view.role = MessageRole::Activity;
                    }
                    view
                })
                .collect(),
            details: entry.data.as_ref().map(format_value),
        }
    }
}

fn format_value(value: &Value) -> String {
    // JSON serialization of a serde_json::Value is infallible except resource exhaustion.
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn text_block(kind: ContentKind, text: String) -> ContentBlock {
    ContentBlock {
        kind,
        text,
        tool_name: None,
        operation_id: None,
        image_mime_type: None,
        image_data: None,
    }
}

fn project_message(message: &Value) -> MessageView {
    let role = match message["role"].as_str() {
        Some("user") => MessageRole::User,
        Some("assistant") => MessageRole::Assistant,
        Some("toolResult") => MessageRole::Tool,
        _ => MessageRole::Activity,
    };
    let content = match &message["content"] {
        Value::String(text) => vec![text_block(ContentKind::Text, text.clone())],
        Value::Array(blocks) => blocks.iter().map(project_block).collect(),
        Value::Null => Vec::new(),
        value => vec![text_block(ContentKind::Unsupported, format_value(value))],
    };
    MessageView {
        role,
        content,
        tool_name: message["toolName"].as_str().map(str::to_owned),
        operation_id: message["toolCallId"].as_str().map(str::to_owned),
        failed: message["isError"].as_bool() == Some(true) || message["stopReason"] == "error",
        pending: message["stopReason"] == "pending",
        timestamp_ms: message["timestamp"].as_f64(),
        error_message: message["errorMessage"].as_str().map(str::to_owned),
        details: message.get("details").map(format_value),
    }
}

pub(crate) fn project_live(value: Option<&Value>) -> Option<LiveView> {
    let value = value.filter(|value| value.is_object())?;
    let generation = value.get("generation");
    let mut tools = Vec::new();
    for key in ["tools", "nestedTools"] {
        for slot in value
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            tools.push(LiveToolView {
                operation_id: slot["callId"].as_str().unwrap_or_default().to_owned(),
                name: slot["name"].as_str().unwrap_or_default().to_owned(),
                phase: match slot["status"].as_str() {
                    Some("pending") => LiveToolPhase::Pending,
                    Some("running") => LiveToolPhase::Running,
                    Some("done") => LiveToolPhase::Done,
                    _ => LiveToolPhase::Unknown,
                },
                output: slot["output"].as_str().map(str::to_owned),
                dropped_bytes: slot["droppedBytes"].as_u64(),
                dropped_lines: slot["droppedLines"].as_u64(),
                entry_id: slot["entry"].as_u64(),
                parent_operation_id: slot["parentCallId"].as_str().map(str::to_owned),
                details: Some(format_value(slot)),
            });
        }
    }
    Some(LiveView {
        run_active: value.get("run").is_some(),
        generation: generation
            .and_then(|generation| generation.get("message"))
            .map(project_message),
        attempt: generation.and_then(|generation| generation["attempt"].as_u64()),
        retry_at_ms: generation.and_then(|generation| generation["retry"]["at"].as_f64()),
        retry_error: generation
            .and_then(|generation| generation["retry"]["error"].as_str())
            .map(str::to_owned),
        deferred_poll_at_ms: generation
            .and_then(|generation| generation["deferred"]["pollAt"].as_f64()),
        tools,
        compacting: value
            .get("compactions")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty()),
        failure_reason: value["runFailure"]["reason"].as_str().map(str::to_owned),
        failure_detail: value["runFailure"]["detail"].as_str().map(str::to_owned),
    })
}

fn project_block(block: &Value) -> ContentBlock {
    match block["type"].as_str() {
        Some("text") => text_block(
            ContentKind::Text,
            block["text"].as_str().unwrap_or_default().to_owned(),
        ),
        Some("thinking") => text_block(
            ContentKind::Reasoning,
            block["thinking"].as_str().unwrap_or_default().to_owned(),
        ),
        Some("toolCall") => ContentBlock {
            tool_name: block["name"].as_str().map(str::to_owned),
            operation_id: block["id"].as_str().map(str::to_owned),
            ..text_block(
                ContentKind::ToolCall,
                block.get("arguments").map(format_value).unwrap_or_default(),
            )
        },
        Some("image") => ContentBlock {
            image_mime_type: block["mimeType"].as_str().map(str::to_owned),
            image_data: block["data"].as_str().map(str::to_owned),
            ..text_block(ContentKind::Image, String::new())
        },
        _ => text_block(ContentKind::Unsupported, format_value(block)),
    }
}

pub(crate) fn decode_entry(value: &Value) -> Result<TranscriptEntryView, ClientError> {
    serde_json::from_value::<TranscriptEntry>(value.clone())
        .map(Into::into)
        .map_err(|_| ClientError::InvalidResponse)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tool_request_and_failed_result_remain_distinct() {
        let assistant = project_message(&json!({"role":"assistant","content":[
            {"type":"text","text":"Checking the file."},
            {"type":"toolCall","id":"op-1","name":"read","arguments":{"path":"a.txt"}}
        ],"stopReason":"toolUse"}));
        assert!(!assistant.failed);
        assert!(matches!(assistant.content[1].kind, ContentKind::ToolCall));
        let result = project_message(
            &json!({"role":"toolResult","toolCallId":"op-1","toolName":"read","content":[{"type":"text","text":"Permission denied"}],"isError":true}),
        );
        assert!(result.failed);
        assert_eq!(result.operation_id.as_deref(), Some("op-1"));
    }

    #[test]
    fn unknown_blocks_are_preserved_for_an_explicit_inspector() {
        let block = project_block(&json!({"type":"future_attachment","ref":"object-1"}));
        assert!(matches!(block.kind, ContentKind::Unsupported));
        assert!(block.text.contains("object-1"));
    }
}
