use std::time::Duration;

use bson::{Bson, Document};

use crate::{
    concern::{ReadConcern, WriteConcern},
    read_preference::ReadPreference,
};

/// These are the valid options for creating a `Collection` with
/// `Database::collection_with_options`.
#[derive(Debug, Default, TypedBuilder)]
pub struct CollectionOptions {
    /// The default read preference for operations.
    #[builder(default)]
    pub read_preference: Option<ReadPreference>,

    /// The default read concern for operations.
    #[builder(default)]
    pub read_concern: Option<ReadConcern>,

    /// The default write concern for operations.
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,
}

/// Specifies whether a `Collection::find_one_and_replace` and `Collection::find_one_and_update`
/// operation should return the document before or after modification.
#[derive(Debug)]
pub enum ReturnDocument {
    /// Return the document after modification.
    After,
    /// Return the document before modification.
    Before,
}

impl ReturnDocument {
    pub(crate) fn is_after(&self) -> bool {
        match *self {
            ReturnDocument::After => true,
            ReturnDocument::Before => false,
        }
    }
}

/// Specifies the index to use for an operation.
#[derive(Debug)]
pub enum Hint {
    /// Specifies the keys of the index to use.
    Keys(Document),
    /// Specifies the name of the index to use.
    Name(String),
}

impl Hint {
    pub(crate) fn into_bson(self) -> Bson {
        match self {
            Hint::Keys(doc) => Bson::Document(doc),
            Hint::Name(s) => Bson::String(s),
        }
    }
}

/// Specifies the type of cursor to return from a find operation.
#[derive(Debug)]
pub enum CursorType {
    /// Default; close the cursor after the last document is received from the server.
    NonTailable,

    /// Do not close the cursor after the last document is received from the server. If more
    /// results become available later, the cursor will return them.
    Tailable,

    /// Similar to `Tailable`, except that the cursor should block on receiving more results if
    /// none are available.
    TailableAwait,
}

/// Specifies the options to a `Collection::insert_one` operation.
#[derive(Debug, Default, TypedBuilder)]
pub struct InsertOneOptions {
    /// Opt out of document-level validation.
    #[builder(default)]
    pub bypass_document_validation: Option<bool>,
}

/// Specifies the options to a `Collection::insert_many` operation.
#[derive(Debug, Default, TypedBuilder)]
pub struct InsertManyOptions {
    /// Opt out of document-level validation.
    #[builder(default)]
    pub bypass_document_validation: Option<bool>,

    /// If true, when an insert fails, return without performing the remaining writes. If false,
    /// when a write fails, continue with the remaining writes, if any.
    #[builder(default)]
    pub ordered: Option<bool>,
}

/// Specifies the options to a `Collection::update_one` or `Collection::update_many` operation.
#[derive(Debug, Default, TypedBuilder)]
pub struct UpdateOptions {
    /// A set of filters specifying to which array elements an update should apply.
    ///
    /// See the documentation [here](https://docs.mongodb.com/manual/reference/command/update/) for
    /// more information on array filters.
    #[builder(default)]
    pub array_filters: Option<Vec<Document>>,

    /// Opt out of document-level validation.
    #[builder(default)]
    pub bypass_document_validation: Option<bool>,

    /// If true, insert a document if no matching document is found.
    #[builder(default)]
    pub upsert: Option<bool>,
}

/// Specifies the options to a `Collection::replace_one` operation.
#[derive(Debug, Default, TypedBuilder)]
pub struct ReplaceOptions {
    /// Opt out of document-level validation.
    #[builder(default)]
    pub bypass_document_validation: Option<bool>,

    /// If true, insert a document if no matching document is found.
    #[builder(default)]
    pub upsert: Option<bool>,
}

/// Specifies the options to a `Collection::delete_one` or `Collection::delete_many` operation.
#[derive(Debug, Default, TypedBuilder)]
pub struct DeleteOptions {}

/// Specifies the options to a `Collection::find_one_and_delete` operation.
#[derive(Debug, Default, TypedBuilder)]
pub struct FindOneAndDeleteOptions {
    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[builder(default)]
    pub max_time: Option<Duration>,

    /// Limits the fields of the document being returned.
    #[builder(default)]
    pub projection: Option<Document>,

    /// The order of the documents for the purposes of the operation.
    #[builder(default)]
    pub sort: Option<Document>,
}

/// Specifies the options to a `Collection::find_one_and_replace` operation.
#[derive(Debug, Default, TypedBuilder)]
pub struct FindOneAndReplaceOptions {
    /// Opt out of document-level validation.
    #[builder(default)]
    pub bypass_document_validation: Option<bool>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[builder(default)]
    pub max_time: Option<Duration>,

    /// Limits the fields of the document being returned.
    #[builder(default)]
    pub projection: Option<Document>,

    /// Whether the operation should return the document before or after modification.
    #[builder(default)]
    pub return_document: Option<ReturnDocument>,

    /// The order of the documents for the purposes of the operation.
    #[builder(default)]
    pub sort: Option<Document>,

    /// If true, insert a document if no matching document is found.
    #[builder(default)]
    pub upsert: Option<bool>,
}

/// Specifies the options to a `Collection::find_one_and_update` operation.
#[derive(Debug, Default, TypedBuilder)]
pub struct FindOneAndUpdateOptions {
    /// A set of filters specifying to which array elements an update should apply.
    ///
    /// See the documentation [here](https://docs.mongodb.com/manual/reference/command/update/) for
    /// more information on array filters.#[builder(default)]
    pub array_filters: Option<Vec<Document>>,

    /// Opt out of document-level validation.
    #[builder(default)]
    pub bypass_document_validation: Option<bool>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[builder(default)]
    pub max_time: Option<Duration>,

    /// Limits the fields of the document being returned.
    #[builder(default)]
    pub projection: Option<Document>,

    /// Whether the operation should return the document before or after modification.
    #[builder(default)]
    pub return_document: Option<ReturnDocument>,

    /// The order of the documents for the purposes of the operation.
    #[builder(default)]
    pub sort: Option<Document>,

    /// If true, insert a document if no matching document is found.
    #[builder(default)]
    pub upsert: Option<bool>,
}

/// Specifies the options to a `Collection::aggregate` operation.
#[derive(Debug, Default, TypedBuilder)]
pub struct AggregateOptions {
    /// Enables writing to temporary files. When set to true, aggregation stages can write data to
    /// the _tmp subdirectory in the dbPath directory.
    #[builder(default)]
    pub allow_disk_use: Option<bool>,

    /// The number of documents the server should return per cursor batch.
    ///
    /// Note that this does not have any affect on the documents that are returned by a cursor,
    /// only the number of documents kept in memory at a given time (and by extension, the
    /// number of round trips needed to return the entire set of documents returned by the
    /// query).
    #[builder(default)]
    pub batch_size: Option<i32>,

    /// Opt out of document-level validation.
    #[builder(default)]
    pub bypass_document_validation: Option<bool>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[builder(default)]
    pub max_time: Option<Duration>,

    /// Tags the query with an arbitrary string to help trace the operation through the database
    /// profiler, currentOp and logs.
    #[builder(default)]
    pub comment: Option<String>,

    /// The index to use for the operation.
    #[builder(default)]
    pub hint: Option<Hint>,
}

/// Specifies the options to a `Collection::count_documents` operation.
#[derive(Debug, Default, TypedBuilder)]
pub struct CountOptions {
    /// The index to use for the operation.
    #[builder(default)]
    pub hint: Option<Hint>,

    /// The maximum number of documents to count.
    #[builder(default)]
    pub limit: Option<i64>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[builder(default)]
    pub max_time: Option<Duration>,

    /// The number of documents to skip before counting.
    #[builder(default)]
    pub skip: Option<i64>,
}

/// Specifies the options to a `Collection::estimated_document_count` operation.
#[derive(Debug, Default, TypedBuilder)]
pub struct EstimatedDocumentCountOptions {
    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[builder(default)]
    pub max_time: Option<Duration>,
}

/// Specifies the options to a `Collection::distinct` operation.
#[derive(Debug, Default, TypedBuilder)]
pub struct DistinctOptions {
    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[builder(default)]
    pub max_time: Option<Duration>,
}

/// Specifies the options to a `Collection::find` operation.
#[derive(Debug, Default, TypedBuilder)]
pub struct FindOptions {
    /// If true, partial results will be returned from a mongos rather than an error being
    /// returned if one or more shards is down.
    #[builder(default)]
    pub allow_partial_results: Option<bool>,

    /// The number of documents the server should return per cursor batch.
    ///
    /// Note that this does not have any affect on the documents that are returned by a cursor,
    /// only the number of documents kept in memory at a given time (and by extension, the
    /// number of round trips needed to return the entire set of documents returned by the
    /// query.
    #[builder(default)]
    pub batch_size: Option<i32>,

    /// Tags the query with an arbitrary string to help trace the operation through the database
    /// profiler, currentOp and logs.
    #[builder(default)]
    pub comment: Option<String>,

    /// The type of cursor to return.
    #[builder(default)]
    pub cursor_type: Option<CursorType>,

    /// The index to use for the operation.
    #[builder(default)]
    pub hint: Option<Hint>,

    /// The maximum number of documents to query.
    #[builder(default)]
    pub limit: Option<i64>,

    /// The exclusive upper bound for a specific index.
    #[builder(default)]
    pub max: Option<Document>,

    /// The maximum amount of time for the server to wait on new documents to satisfy a tailable
    /// cursor query. If the cursor is not tailable, this option is ignored.
    #[builder(default)]
    pub max_await_time: Option<Duration>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[builder(default)]
    pub max_time: Option<Duration>,

    /// The inclusive lower bound for a specific index.
    #[builder(default)]
    pub min: Option<Document>,

    /// Whether the server should close the cursor after a period of inactivity.
    #[builder(default)]
    pub no_cursor_timeout: Option<bool>,

    /// Limits the fields of the document being returned.
    #[builder(default)]
    pub projection: Option<Document>,

    /// Whether to return only the index keys in the documents.
    #[builder(default)]
    pub return_key: Option<bool>,

    /// Whether to return the record identifier for each document.
    #[builder(default)]
    pub show_record_id: Option<bool>,

    /// The number of documents to skip before counting.
    #[builder(default)]
    pub skip: Option<i64>,

    /// Prevents the cursor from returning a document more than once because of an intervening
    /// write operation.
    #[builder(default)]
    pub snapshot: Option<bool>,

    /// The order of the documents for the purposes of the operation.
    #[builder(default)]
    pub sort: Option<Document>,
}

/// Specifies an index to create.
#[derive(Debug, TypedBuilder)]
pub struct IndexModel {
    /// The fields to index, along with their sort order.
    pub keys: Document,

    /// Extra options to use when creating the index.
    #[builder(default)]
    pub options: Option<Document>,
}
