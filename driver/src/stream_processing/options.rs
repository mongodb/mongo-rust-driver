//! Options for Atlas Stream Processing commands.

use macro_magic::export_tokens;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use typed_builder::TypedBuilder;

use crate::bson::{Document, Timestamp};

/// Options for [`StreamProcessors::create`](super::StreamProcessors::create).
#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
#[export_tokens]
pub struct CreateStreamProcessorOptions {
    /// Dead letter queue configuration.
    pub dlq: Option<Document>,

    /// Field name used for stream metadata.
    pub stream_meta_field_name: Option<String>,

    /// Compute tier (e.g. `"SP2"`, `"SP5"`, `"SP10"`, `"SP30"`, `"SP50"`).
    pub tier: Option<String>,

    /// Whether failover is enabled.
    pub failover: Option<bool>,
}

/// Failover configuration for [`StartStreamProcessorOptions`].
#[skip_serializing_none]
#[derive(Clone, Debug, Deserialize, Serialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct FailoverOptions {
    /// Target region for failover. Required when failover is sent.
    #[builder(setter(into))]
    pub region: String,

    /// Failover mode. Valid values: `"GRACEFUL"` (default), `"FORCED"`.
    #[builder(default, setter(into))]
    pub mode: Option<String>,

    /// If true, validates the failover request without executing it.
    #[builder(default, setter(into))]
    pub dry_run: Option<bool>,
}

/// Options for [`StreamProcessor::start`](super::StreamProcessor::start).
#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
#[export_tokens]
pub struct StartStreamProcessorOptions {
    /// Number of workers.
    pub workers: Option<i32>,

    /// Clear checkpoints before starting.
    pub clear_checkpoints: Option<bool>,

    /// Resume from a specific operation time.
    pub start_at_operation_time: Option<Timestamp>,

    /// Compute tier. Valid values: `"SP2"`, `"SP5"`, `"SP10"`, `"SP30"`, `"SP50"`.
    pub tier: Option<String>,

    /// Enable auto-scaling.
    pub enable_auto_scaling: Option<bool>,

    /// Failover configuration. The `region` is required when failover is sent.
    pub failover: Option<FailoverOptions>,
    // Note: the spec's `startAfter` option is RESERVED for future use and is
    // not yet accepted by the server; this driver MUST NOT send it.
}

/// Options for [`StreamProcessor::stats`](super::StreamProcessor::stats).
#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
#[export_tokens]
pub struct GetStreamProcessorStatsOptions {
    /// If true, includes per-operator statistics.
    pub verbose: Option<bool>,
}

/// Options for [`StreamProcessor::samples`](super::StreamProcessor::samples).
#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
#[export_tokens]
pub struct GetStreamProcessorSamplesOptions {
    /// The cursor id from a previous call. If absent or 0, a new sample cursor
    /// is opened via `startSampleStreamProcessor`. If non-zero, the next batch
    /// is fetched via `getMoreSampleStreamProcessor`.
    pub cursor_id: Option<i64>,

    /// Maximum number of documents to sample. Only sent on the initial call
    /// (when `cursor_id` is absent or 0). Ignored on subsequent calls.
    pub limit: Option<i32>,

    /// Number of documents to return per batch. Only sent on subsequent calls
    /// (when `cursor_id` is non-zero). Ignored on the initial call.
    pub batch_size: Option<i32>,
}
