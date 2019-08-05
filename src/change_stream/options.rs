use std::time::Duration;

use crate::{
    change_stream::document::ResumeToken,
    options::{collation::Collation, AggregateOptions},
};

#[derive(Clone, Debug, Default, Deserialize, Serialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
pub struct ChangeStreamOptions {
    /// When set to "updateLookup", the `full_document` field of the `ChangeStreamEventDocument`
    /// will be populated with a copy of the entire document that was updated from some time
    /// after the change occurred when an "update" event occurs. By default, the `full_document`
    /// field will be empty for updates.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_document: Option<FullDocumentType>,

    /// Specifies the logical starting point for the new change stream. Note that if a watched
    /// collection is dropped and recreated or newly renamed, `start_after` should be set instead.
    /// `resume_after` and `start_after` cannot be set simultaneously.
    ///
    /// For more information on resuming a change stream see the documentation [here](https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-after)
    // TODO: The above doc link is 4.2 and will need to be updated
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume_after: Option<ResumeToken>,

    /// The maximum amount of time for the server to wait on new documents to satisfy a change
    /// stream query.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub max_await_time: Option<Duration>,

    /// The number of documents to return per batch.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub batch_size: Option<i32>,

    /// Specifies a collation.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub collation: Option<Collation>,

    /// The change stream will only provide changes that occurred at or after the specified
    /// timestamp. Any command run against the server will return an operation time that can be
    /// used here.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_at_operation_time: Option<i64>,

    /// Takes a resume token and starts a new change stream returning the first notification after
    /// the token. This will allow users to watch collections that have been dropped and
    /// recreated or newly renamed collections without missing any notifications.
    ///
    /// This feature is only available on MongoDB 4.2+.
    ///
    /// See the documentation [here](https://docs.mongodb.com/master/changeStreams/#change-stream-start-after) for more
    /// information.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_after: Option<ResumeToken>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum FullDocumentType {
    /// The `full_document` field of the `ChangeStreamEventDocument` will be populated with a copy
    /// of the entire document that was updated.
    UpdateLookup,

    /// User-defined other types for forward compatibility.
    Other(String),
}

impl ChangeStreamOptions {
    pub(crate) fn get_resume_token(&self) -> Option<ResumeToken> {
        self.start_after
            .as_ref()
            .or_else(|| self.resume_after.as_ref())
            .map(Clone::clone)
    }

    pub(crate) fn get_aggregation_options(&self) -> AggregateOptions {
        AggregateOptions::builder()
            .batch_size(self.batch_size)
            .collation(self.collation.as_ref().map(Clone::clone))
            .max_await_time(self.max_await_time)
            .build()
    }
}
