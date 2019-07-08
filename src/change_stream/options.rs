use std::time::Duration;

use bson::Document;

#[derive(Debug, TypedBuilder)]
pub struct ChangeStreamOptions {
    /// When set to "updateLookup", the `full_document` field of the
    /// `ChangeStreamDocument` will be populated with a copy of the
    /// entire document that was updated from some time after the
    /// change occurred. By default, the `full_document`
    /// field will be empty for updates.
    #[builder(default)]
    full_document: Option<FullDocumentType>,

    /// Specifies the logical starting point for the new change stream.
    #[builder(default)]
    resume_after: Option<Document>,

    /// The maximum amount of time for the server to wait on new documents to
    /// satisfy a change stream query.
    #[builder(default)]
    max_await_time: Option<Duration>,

    /// The number of documents to return per batch.
    #[builder(default)]
    batch_size: Option<i32>,

    /// Specifies a collation.
    #[builder(default)]
    collation: Option<Document>,

    /// The change stream will only provide changes that occurred at or after
    /// the specified timestamp. Any command run against the server will
    /// return an operation time that can be used here.
    #[builder(default)]
    start_at_operation_time: Option<i64>,

    /// Takes a resume token and starts a new change stream returning the first
    /// notification after the token. This will allow users to watch collections
    /// that have been dropped and recreated or newly renamed collections without
    /// missing any notifications.
    #[builder(default)]
    start_after: Option<Document>,
}

#[derive(Debug)]
pub enum FullDocumentType {
    /// The `full_document` field of the `ChangeStreamDocument`
    /// will be populated with a copy of the entire document that was updated
    UpdateLookup,

    /// User-defined other types for forward compatibility
    Other(String),
}
