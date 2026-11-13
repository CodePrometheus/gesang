//! Versioned presentation cache encoding; the platform encrypts every record before persistence.

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{ClientError, LiveView, TranscriptEntryView};

/// Last-observed transcript membership; a cached window is never asserted to be current.
#[derive(Clone, Debug, Serialize, Deserialize, uniffi::Record)]
pub struct TranscriptCacheMetadata {
    /// Authoritative source conversation identity, checked before displaying a cache.
    pub conversation_id: u64,
    /// Explicit membership avoids loading stale rows left from an earlier window.
    pub entry_ids: Vec<u64>,
    /// Last observed source boundary for older history.
    pub older_before: Option<u64>,
    /// Source history continuation, retained unchanged.
    pub cursor: Option<String>,
    /// Whether history paging has already begun at that source boundary.
    pub history_started: bool,
    /// Actual last observation time, not the time the cache is opened.
    pub synchronized_at_ms: i64,
    /// Last committed live progress, displayed only as cached when disconnected.
    pub live: Option<LiveView>,
}

#[derive(Serialize, Deserialize)]
struct CacheRecord<T> {
    version: u8,
    value: T,
}

/// Encode one presentation entry; the returned bytes require encrypted platform storage.
#[uniffi::export]
pub fn encode_transcript_entry(entry: TranscriptEntryView) -> Result<Vec<u8>, ClientError> {
    encode(entry)
}

/// Decode a supported cached entry without claiming a successful server synchronization.
#[uniffi::export]
pub fn decode_transcript_entry(encoded: Vec<u8>) -> Result<TranscriptEntryView, ClientError> {
    decode(&encoded)
}

/// Encode exact loaded membership and its original observation time for encrypted storage.
#[uniffi::export]
pub fn encode_transcript_metadata(
    metadata: TranscriptCacheMetadata,
) -> Result<Vec<u8>, ClientError> {
    encode(metadata)
}

/// Decode cache metadata; callers must check the expected source conversation identity.
#[uniffi::export]
pub fn decode_transcript_metadata(
    encoded: Vec<u8>,
) -> Result<TranscriptCacheMetadata, ClientError> {
    decode(&encoded)
}

fn encode<T: Serialize>(value: T) -> Result<Vec<u8>, ClientError> {
    serde_json::to_vec(&CacheRecord { version: 1, value }).map_err(|_| ClientError::InvalidResponse)
}

fn decode<T: DeserializeOwned>(encoded: &[u8]) -> Result<T, ClientError> {
    let record: CacheRecord<T> =
        serde_json::from_slice(encoded).map_err(|_| ClientError::InvalidResponse)?;
    if record.version != 1 {
        return Err(ClientError::InvalidResponse);
    }
    Ok(record.value)
}
