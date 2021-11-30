//! Contains the types related to a `ChangeStream` event.
use std::convert::TryInto;

use crate::{coll::Namespace, cursor::CursorSpecification, options::ChangeStreamOptions};

use bson::{Bson, Document};
use serde::{Deserialize, Serialize};

/// An opaque token used for resuming an interrupted
/// [`ChangeStream`](crate::change_stream::ChangeStream).
///
/// When starting a new change stream,
/// [`crate::options::ChangeStreamOptions::start_after`] and
/// [`crate::options::ChangeStreamOptions::resume_after`] fields can be specified
/// with instances of `ResumeToken`.
///
/// See the documentation
/// [here](https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token) for more
/// information on resume tokens.
// TODO(RUST-1045): Switch this to RawBson
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResumeToken(pub(crate) Bson);

impl ResumeToken {
    pub(crate) fn initial(
        options: &Option<ChangeStreamOptions>,
        spec: &CursorSpecification,
    ) -> Option<ResumeToken> {
        // Token from options passed to `watch`
        let options_token = options
            .as_ref()
            .and_then(|o| o.start_after.as_ref().or(o.resume_after.as_ref()))
            .cloned();
        // Token from initial response from `aggregate`
        let spec_token = if spec.initial_buffer.is_empty() {
            spec.post_batch_resume_token
                .clone()
                .and_then(|d| d.try_into().ok())
                .map(|d| ResumeToken(Bson::Document(d)))
        } else {
            None
        };
        spec_token.or(options_token)
    }
}

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

    /// Identifies the collection or database on which the event occurred.
    pub ns: Option<ChangeStreamEventSource>,

    /// The new name for the `ns` collection.  Only included for `OperationType::Rename`.
    pub to: Option<Namespace>,

    /// A `Document` that contains the `_id` of the document created or modified by the `insert`,
    /// `replace`, `delete`, `update` operations (i.e. CRUD operations). For sharded collections,
    /// also displays the full shard key for the document. The `_id` field is not repeated if it is
    /// already a part of the shard key.
    pub document_key: Option<Document>,

    /// A description of the fields that were updated or removed by the update operation.
    /// Only specified if `operation_type` is `OperationType::Update`.
    pub update_description: Option<UpdateDescription>,

    /// The `Document` created or modified by the `insert`, `replace`, `delete`, `update`
    /// operations (i.e. CRUD operations).
    ///
    /// For `insert` and `replace` operations, this represents the new document created by the
    /// operation.  For `delete` operations, this field is `None`.
    ///
    /// For `update` operations, this field only appears if you configured the change stream with
    /// [`full_document`](crate::options::ChangeStreamOptions::full_document) set to
    /// [`UpdateLookup`](crate::options::FullDocumentType::UpdateLookup). This field then
    /// represents the most current majority-committed version of the document modified by the
    /// update operation.
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

/// Identifies the collection or database on which an event occurred.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub enum ChangeStreamEventSource {
    /// The [`Namespace`] containing the database and collection in which the change occurred.
    Namespace(Namespace),

    /// Contains the name of the database in which the change happened.
    Database(String),
}
