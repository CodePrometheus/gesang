//! Native client transport and state; cloud execution remains independent of this process.

mod authorization;
mod cache;
mod goals;
mod learning;
mod observation;
mod records;
mod transcript;
mod transport;

pub use authorization::{AuthorizationAttempt, ServerConfiguration, TokenSet};
pub use cache::{
    TranscriptCacheMetadata, decode_transcript_entry, decode_transcript_metadata,
    encode_transcript_entry, encode_transcript_metadata,
};
pub use goals::{EvidenceKind, EvidenceView, GoalPageView, GoalPhase, GoalView, PendingCommand};
pub use learning::{
    LearningAction, LearningChange, LearningCorrection, LearningHeld, LearningSettingsChange,
    LearningSettingsView, LearningView, NumberChange, TextChange,
};
pub use observation::{ConversationObservation, GoalObservation};
pub use records::*;
pub use transcript::{
    ContentBlock, ContentKind, LiveToolPhase, LiveToolView, LiveView, MessageRole, MessageView,
    TranscriptEntryView, TranscriptPageView, TranscriptUpdate,
};
pub use transport::Client;

uniffi::setup_scaffolding!();
