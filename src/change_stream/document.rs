use bson::Document;

/// An opaque token for use when resuming an interrupted `ChangeStream`. `ResumeToken` is used
/// during an automatic resume of a change stream.
///
/// When starting a new change stream, `start_after` and `resume_after` fields can be specified
/// with instances of `ResumeToken`.
///
/// See the documentation
/// [here](https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token) for more
/// information on resume tokens.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResumeToken(Document);

/// A `ChangeStreamEventDocument` represents a [change event]
/// (https://docs.mongodb.com/manual/reference/change-events/) in the associated change stream.
/// Instances of `ChangeStreamEventDocument` are returned from calls to `ChangeStream::next`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeStreamEventDocument {
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

    /// Contains two fields: "db" and "coll" containing the database and collection name in which
    /// the change happened.
    pub ns: Document,

    /// For unsharded collections this contains a single field, id, with the value of the id of the
    /// document updated.  For sharded collections, this will contain all the components of the
    /// shard key in order, followed by the id if the id isn’t part of the shard key.
    pub document_key: Option<Document>,

    /// Contains a description of updated and removed fields in this operation.
    pub update_description: Option<UpdateDescription>,

    /// For operations of type "insert" and "replace", this key will contain the `Document` being
    /// inserted, or the new version of the `Document` that is replacing the existing
    /// `Document`, respectively.
    ///
    /// For operations of type "update", when the change stream's full document type is
    /// UpdateLookup, this key will contain a copy of the full version of the `Document` from
    /// some point after the update occurred. If the `Document` was deleted since the updated
    /// happened, it will be None.
    pub full_document: Option<Document>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateDescription {
    /// A `Document` containing key:value pairs of names of the fields that were changed, and the
    /// new value for those fields.
    pub updated_fields: Document,

    /// An array of field names that were removed from the `Document`.
    pub removed_fields: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "camelCase")]
pub enum OperationType {
    Insert,
    Update,
    Delete,
    Invalidate,
    Drop,
    DropDatabase,
    Rename,
}
