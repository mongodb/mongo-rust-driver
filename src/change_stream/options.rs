//! Contains options for ChangeStreams.
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::time::Duration;

use crate::{
    bson::{Bson, Timestamp},
    change_stream::event::ResumeToken,
    collation::Collation,
    concern::ReadConcern,
    options::AggregateOptions,
    selection_criteria::SelectionCriteria,
};

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub(crate) struct ChangeStreamOptions {
    pub(crate) full_document: Option<FullDocumentType>,
    pub(crate) full_document_before_change: Option<FullDocumentBeforeChangeType>,
    pub(crate) resume_after: Option<ResumeToken>,
    pub(crate) start_at_operation_time: Option<Timestamp>,
    pub(crate) start_after: Option<ResumeToken>,
    pub(crate) all_changes_for_cluster: Option<bool>,
    #[serde(skip_serializing)]
    pub(crate) max_await_time: Option<Duration>,
    #[serde(skip_serializing)]
    pub(crate) batch_size: Option<u32>,
    #[serde(skip_serializing)]
    pub(crate) collation: Option<Collation>,
    #[serde(skip_serializing)]
    pub(crate) read_concern: Option<ReadConcern>,
    #[serde(skip_serializing)]
    pub(crate) selection_criteria: Option<SelectionCriteria>,
    pub(crate) comment: Option<Bson>,
}

impl ChangeStreamOptions {
    pub(crate) fn aggregate_options(&self) -> AggregateOptions {
        AggregateOptions::builder()
            .batch_size(self.batch_size)
            .collation(self.collation.clone())
            .max_await_time(self.max_await_time)
            .read_concern(self.read_concern.clone())
            .selection_criteria(self.selection_criteria.clone())
            .comment_bson(self.comment.clone())
            .build()
    }
}

/// Describes the modes for configuring the
/// [`ChangeStreamEvent::full_document`](
/// crate::change_stream::event::ChangeStreamEvent::full_document) field.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum FullDocumentType {
    /// The field will be populated with a copy of the entire document that was updated.
    UpdateLookup,

    /// The field will be populated for replace and update change events if the post-image for this
    /// event is available.
    WhenAvailable,

    /// The same behavior as `WhenAvailable` except that an error is raised if the post-image is
    /// not available.
    Required,

    /// User-defined other types for forward compatibility.
    Other(String),
}

/// Describes the modes for configuring the
/// [`ChangeStreamEvent::full_document_before_change`](
/// crate::change_stream::event::ChangeStreamEvent::full_document_before_change) field.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum FullDocumentBeforeChangeType {
    /// The field will be populated for replace, update, and delete change events if the pre-image
    /// for this event is available.
    WhenAvailable,

    /// The same behavior as `WhenAvailable` except that an error is raised if the pre-image is
    /// not available.
    Required,

    /// Do not send a value.
    Off,

    /// User-defined other types for forward compatibility.
    Other(String),
}
