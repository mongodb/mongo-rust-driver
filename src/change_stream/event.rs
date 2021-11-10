//! Contains documents related to a ChangeStream event.
use crate::coll::Namespace;
use bson::{Bson, Document};
use serde::{Deserialize, Serialize};

/// An opaque token used for resuming an interrupted
/// [`ChangeStream`](../struct.ChangeStream.html).
///
/// When starting a new change stream,
/// [`start_after`](../option/struct.ChangeStreamOptions.html#structfield.start_after)
/// and [`resume_after`](../option/struct.ChangeStreamOptions.html#structfield.resume_after) fields
/// on [`ChangeStreamOptions`](../option/struct.ChangeStreamOptions.html) can be specified
/// with instances of `ResumeToken`.
///
/// See the documentation
/// [here](https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token) for more
/// information on resume tokens.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResumeToken(pub(crate) Bson);

/// A `ChangeStreamEvent` represents a
/// [change event](https://docs.mongodb.com/manual/reference/change-events/) in the associated change stream.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ChangeStreamEvent<T> {
    /// An opaque token for use when resuming an interrupted `ChangeStream`.
    ///
    /// See the documentation
    /// [here](https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token) for
    /// more information on resume tokens.
    ///
    /// Also see the documentation on [resuming a change
    /// stream](https://docs.mongodb.com/manual/changeStreams/#resume-a-change-stream).
    #[serde(rename = "_id")]
    pub id: ResumeToken,

    /// Describes the type of operation represented in this change notification.
    pub operation_type: OperationType,

    /// Identifies which collection or database where the event occurred.
    pub ns: Option<ChangeStreamEventSource>,

    /// The new name for the ns collection.  Only included for `OperationType::Rename`.
    pub to: Option<Namespace>,

    /// For unsharded collections this contains a single field, id, with the value of the id of the
    /// document updated.  For sharded collections, this will contain all the components of the
    /// shard key in order, followed by the id if the id isn’t part of the shard key.
    pub document_key: Option<Document>,

    /// Contains a description of updated and removed fields in this operation.
    pub update_description: Option<UpdateDescription>,

    /// For operations of type "insert" and "replace", this key will contain the document being
    /// inserted, or the new version of the document that is replacing the existing
    /// document, respectively.
    ///
    /// For operations of type "update", when the `ChangeStream's` full document type is
    /// `UpdateLookup`, this key will contain a copy of the full version of the document from
    /// some point after the update occurred. If the document was deleted since the updated
    /// happened, it will be `None`.
    pub full_document: Option<T>,
}

/// Describes which fields have been updated or removed from a document.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct UpdateDescription {
    /// A `Document` containing key:value pairs of names of the fields that were changed, and the
    /// new value for those fields.
    pub updated_fields: Document,

    /// An array of field names that were removed from the `Document`.
    pub removed_fields: Vec<String>,
}

/// The operation type represented in a given change notification.
#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum OperationType {
    /// See [insert-event](https://docs.mongodb.com/manual/reference/change-events/#insert-event)
    Insert,

    /// See [update-event](https://docs.mongodb.com/manual/reference/change-events/#update-event)
    Update,

    /// See [replace-event](https://docs.mongodb.com/manual/reference/change-events/#replace-event)
    Replace,

    /// See [delete-event](https://docs.mongodb.com/manual/reference/change-events/#delete-event)
    Delete,

    /// See [drop-event](https://docs.mongodb.com/manual/reference/change-events/#drop-event)
    Drop,

    /// See [rename-event](https://docs.mongodb.com/manual/reference/change-events/#rename-event)
    Rename,

    /// See [dropdatabase-event](https://docs.mongodb.com/manual/reference/change-events/#dropdatabase-event)
    DropDatabase,

    /// See [invalidate-event](https://docs.mongodb.com/manual/reference/change-events/#invalidate-event)
    Invalidate,
}

/// Identifies which collection or database where an event occurred.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum ChangeStreamEventSource {
    /// Contains two fields: "db" and "coll" containing the database and collection name in which
    /// the change happened.
    Namespace(Namespace),

    // Contains the name of the dabatase in which the change happened.
    Database(String),
}
