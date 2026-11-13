//! Result types for Atlas Stream Processing commands.

use serde::{Deserialize, Serialize};

use crate::bson::{DateTime, Document};

/// Information about a single stream processor, returned by `getStreamProcessor`.
///
/// Fields the spec marks as optional may be absent depending on server version
/// and are deserialized as `None`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct StreamProcessorInfo {
    /// Processor id. Optional: not returned by all server versions.
    pub id: Option<String>,

    /// Processor name.
    pub name: String,

    /// Current state. Drivers MUST surface unknown state values as-is rather
    /// than mapping them to a known state, so this is intentionally a plain
    /// `String` rather than an enum.
    pub state: String,

    /// Aggregation pipeline of the processor.
    #[serde(default)]
    pub pipeline: Vec<Document>,

    /// Pipeline version. Optional: not returned by all server versions.
    pub pipeline_version: Option<i32>,

    /// Compute tier.
    pub tier: Option<String>,

    /// Dead letter queue configuration.
    pub dlq: Option<Document>,

    /// Field name used for stream metadata.
    pub stream_meta_field_name: Option<String>,

    /// Whether auto-scaling is enabled.
    #[serde(default)]
    pub enable_auto_scaling: bool,

    /// Whether failover is enabled.
    #[serde(default)]
    pub failover_enabled: bool,

    /// Active region for the processor.
    pub active_region: Option<String>,

    /// Workspace default region.
    pub workspace_default_region: Option<String>,

    /// Timestamp of the last state change.
    pub last_state_change: Option<DateTime>,

    /// Timestamp of the last modification.
    pub last_modified_at: Option<DateTime>,

    /// User who last modified the processor.
    pub modified_by: Option<String>,

    /// Whether the processor has been started at least once.
    #[serde(default)]
    pub has_started: bool,

    /// Error message. Always present; empty when no error has occurred.
    #[serde(default)]
    pub error_msg: String,

    /// Whether the most recent error is retryable.
    #[serde(default)]
    pub error_retryable: bool,

    /// Error code from the most recent error.
    pub error_code: Option<i32>,
}

/// A batch of sampled documents from a running stream processor.
///
/// Callers MUST stop iterating when [`cursor_id`](Self::cursor_id) is `0` —
/// the cursor is exhausted and no further calls should be made.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct StreamProcessorSamples {
    /// The cursor id to pass to the next call. A value of `0` means the cursor
    /// is exhausted.
    pub cursor_id: i64,

    /// The batch of sampled documents returned by this call.
    pub documents: Vec<Document>,
}

impl StreamProcessorSamples {
    /// Returns `true` when the cursor is exhausted (cursor id is 0).
    pub fn is_exhausted(&self) -> bool {
        self.cursor_id == 0
    }
}
