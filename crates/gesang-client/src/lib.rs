//! Native client transport and state; cloud execution remains independent of this process.

mod authorization;
mod cache;
mod goals;
mod learning;
mod observation;
mod records;
mod transcript;
mod transport;

fn unix_milliseconds(value: jiff::Timestamp) -> i64 {
    // The Unix-millisecond representation floors negative fractions and fits i64.
    value.as_nanosecond().div_euclid(1_000_000) as i64
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_keep_native_client_units_and_offset() {
        let before_epoch = "1969-12-31T23:59:59.999999999Z".parse().unwrap();
        assert_eq!(unix_milliseconds(before_epoch), -1);
        let revoked = "2026-11-20T16:00:00.120300+08:00".parse().unwrap();
        assert_eq!(
            gesang_api::timestamp::display_with_utc_offset(revoked).to_string(),
            "2026-11-20T08:00:00.120300+00:00"
        );
    }
}

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
