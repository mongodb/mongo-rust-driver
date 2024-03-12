use serde::Deserialize;

use crate::{bson::Bson, error::WriteError, operation::CursorInfo, results::BulkWriteResult};

/// The top-level response to the bulkWrite command.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Response {
    pub(super) cursor: CursorInfo,
    #[serde(flatten)]
    pub(super) summary: SummaryInfo,
}

/// The summary information contained within the top-level response to the bulkWrite command.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SummaryInfo {
    pub(super) n_errors: i64,
    pub(super) n_inserted: i64,
    pub(super) n_matched: i64,
    pub(super) n_modified: i64,
    pub(super) n_upserted: i64,
    pub(super) n_deleted: i64,
}

impl BulkWriteResult {
    pub(super) fn populate_summary_info(&mut self, summary_info: &SummaryInfo) {
        self.inserted_count += summary_info.n_inserted;
        self.upserted_count += summary_info.n_upserted;
        self.matched_count += summary_info.n_matched;
        self.modified_count += summary_info.n_modified;
        self.deleted_count += summary_info.n_deleted;
    }
}

/// The structure of the response for a single operation within the results cursor.
#[derive(Debug, Deserialize)]
pub(super) struct SingleOperationResponse {
    #[serde(rename = "idx")]
    pub(super) index: usize,
    #[serde(flatten)]
    pub(super) result: SingleOperationResult,
}

/// The structure of the non-index fields for a single operation within the results cursor.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(super) enum SingleOperationResult {
    // This variant must be listed first for proper deserialization.
    Error(WriteError),
    #[serde(rename_all = "camelCase")]
    Success {
        n: u64,
        n_modified: Option<u64>,
        upserted: Option<UpsertedId>,
    },
}

/// The structure of the inserted ID for an upserted document.
#[derive(Debug, Deserialize)]
pub(super) struct UpsertedId {
    #[serde(rename = "_id")]
    pub(super) id: Bson,
}
