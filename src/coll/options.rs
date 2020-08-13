use std::{
    time::Duration,
    collections::HashMap,
};

use serde::{Deserialize, Serialize, Serializer};
use serde_with::skip_serializing_none;
use typed_builder::TypedBuilder;

use crate::{
    bson::{doc, Bson, Document},
    bson_util::{
        deserialize_duration_from_u64_millis,
        serialize_batch_size,
        serialize_duration_as_int_millis,
        serialize_u32_as_i32,
    },
    concern::{ReadConcern, WriteConcern},
    options::Collation,
    selection_criteria::SelectionCriteria,
};

use either::Either;

/// These are the valid options for creating a [`Collection`](../struct.Collection.html) with
/// [`Database::collection_with_options`](../struct.Database.html#method.collection_with_options).
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CollectionOptions {
    /// The default read preference for operations.
    #[builder(default)]
    pub selection_criteria: Option<SelectionCriteria>,

    /// The default read concern for operations.
    #[builder(default)]
    pub read_concern: Option<ReadConcern>,

    /// The default write concern for operations.
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,
}

/// Specifies whether a
/// [`Collection::find_one_and_replace`](../struct.Collection.html#method.find_one_and_replace) and
/// [`Collection::find_one_and_update`](../struct.Collection.html#method.find_one_and_update)
/// operation should return the document before or after modification.
#[derive(Clone, Debug, Deserialize)]
#[non_exhaustive]
pub enum ReturnDocument {
    /// Return the document after modification.
    After,
    /// Return the document before modification.
    Before,
}

/// Specifies the index to use for an operation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Hint {
    /// Specifies the keys of the index to use.
    Keys(Document),
    /// Specifies the name of the index to use.
    Name(String),
}

impl Hint {
    pub(crate) fn to_bson(&self) -> Bson {
        match self {
            Hint::Keys(ref d) => Bson::Document(d.clone()),
            Hint::Name(ref s) => Bson::String(s.clone()),
        }
    }
}

/// Specifies the type of cursor to return from a find operation.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
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

/// Specifies the options to a
/// [`Collection::insert_one`](../struct.Collection.html#method.insert_one) operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct InsertOneOptions {
    /// Opt out of document-level validation.
    #[builder(default)]
    pub bypass_document_validation: Option<bool>,

    /// The write concern for the operation.
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,
}

/// Specifies the options to a
/// [`Collection::insert_many`](../struct.Collection.html#method.insert_many) operation.
#[skip_serializing_none]
#[derive(Clone, Debug, Default, TypedBuilder, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct InsertManyOptions {
    /// Opt out of document-level validation.
    #[builder(default)]
    pub bypass_document_validation: Option<bool>,

    /// If true, when an insert fails, return without performing the remaining writes. If false,
    /// when a write fails, continue with the remaining writes, if any.
    ///
    /// Defaults to true.
    #[builder(default)]
    pub ordered: Option<bool>,

    /// The write concern for the operation.
    #[builder(default)]
    #[serde(skip_deserializing)]
    pub write_concern: Option<WriteConcern>,
}

impl InsertManyOptions {
    pub(crate) fn from_insert_one_options(options: InsertOneOptions) -> Self {
        Self {
            bypass_document_validation: options.bypass_document_validation,
            ordered: None,
            write_concern: options.write_concern,
        }
    }
}

/// Enum modeling the modifications to apply during an update.
/// For details, see the official MongoDB
/// [documentation](https://docs.mongodb.com/manual/reference/command/update/#update-command-behaviors)
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum UpdateModifications {
    /// A document that contains only update operator expressions.
    Document(Document),

    /// An aggregation pipeline.
    /// Only available in MongoDB 4.2+.
    Pipeline(Vec<Document>),
}

impl UpdateModifications {
    pub(crate) fn to_bson(&self) -> Bson {
        match self {
            UpdateModifications::Document(ref d) => Bson::Document(d.clone()),
            UpdateModifications::Pipeline(ref p) => {
                Bson::Array(p.iter().map(|d| Bson::Document(d.clone())).collect())
            }
        }
    }
}

impl From<Document> for UpdateModifications {
    fn from(item: Document) -> Self {
        UpdateModifications::Document(item)
    }
}

impl From<Vec<Document>> for UpdateModifications {
    fn from(item: Vec<Document>) -> Self {
        UpdateModifications::Pipeline(item)
    }
}

/// Specifies the options to a
/// [`Collection::update_one`](../struct.Collection.html#method.update_one) or
/// [`Collection::update_many`](../struct.Collection.html#method.update_many) operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
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

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    #[builder(default)]
    pub collation: Option<Collation>,

    /// A document or string that specifies the index to use to support the query predicate.
    ///
    /// Only available in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://docs.mongodb.com/manual/reference/command/update/#ex-update-command-hint) for examples.
    #[builder(default)]
    pub hint: Option<Hint>,

    /// The write concern for the operation.
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,
}

impl UpdateOptions {
    pub(crate) fn from_replace_options(options: ReplaceOptions) -> Self {
        Self {
            bypass_document_validation: options.bypass_document_validation,
            upsert: options.upsert,
            hint: options.hint,
            write_concern: options.write_concern,
            collation: options.collation,
            ..Default::default()
        }
    }
}

/// Specifies the options to a
/// [`Collection::replace_one`](../struct.Collection.html#method.replace_one) operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ReplaceOptions {
    /// Opt out of document-level validation.
    #[builder(default)]
    pub bypass_document_validation: Option<bool>,

    /// If true, insert a document if no matching document is found.
    #[builder(default)]
    pub upsert: Option<bool>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    #[builder(default)]
    pub collation: Option<Collation>,

    /// A document or string that specifies the index to use to support the query predicate.
    ///
    /// Only available in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://docs.mongodb.com/manual/reference/command/update/#ex-update-command-hint) for examples.
    #[builder(default)]
    pub hint: Option<Hint>,

    /// The write concern for the operation.
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,
}

/// Specifies the options to a
/// [`Collection::delete_one`](../struct.Collection.html#method.delete_one) or
/// [`Collection::delete_many`](../struct.Collection.html#method.delete_many) operation.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DeleteOptions {
    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    #[builder(default)]
    pub collation: Option<Collation>,

    /// The write concern for the operation.
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,

    /// The index to use for the operation.
    /// Only available in MongoDB 4.4+.
    #[builder(default)]
    pub hint: Option<Hint>,
}

/// Specifies the options to a
/// [`Collection::find_one_and_delete`](../struct.Collection.html#method.find_one_and_delete)
/// operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[non_exhaustive]
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

    /// The level of the write concern
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    #[builder(default)]
    pub collation: Option<Collation>,

    /// The index to use for the operation.
    /// Only available in MongoDB 4.4+.
    #[builder(default)]
    pub hint: Option<Hint>,
}

/// Specifies the options to a
/// [`Collection::find_one_and_replace`](../struct.Collection.html#method.find_one_and_replace)
/// operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[non_exhaustive]
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

    /// The level of the write concern
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    #[builder(default)]
    pub collation: Option<Collation>,

    /// The index to use for the operation.
    /// Only available in MongoDB 4.4+.
    #[builder(default)]
    pub hint: Option<Hint>,
}

/// Specifies the options to a
/// [`Collection::find_one_and_update`](../struct.Collection.html#method.find_one_and_update)
/// operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[non_exhaustive]
pub struct FindOneAndUpdateOptions {
    /// A set of filters specifying to which array elements an update should apply.
    ///
    /// See the documentation [here](https://docs.mongodb.com/manual/reference/command/update/) for
    /// more information on array filters.
    #[builder(default)]
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

    /// The level of the write concern
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    #[builder(default)]
    pub collation: Option<Collation>,

    /// The index to use for the operation.
    /// Only available in MongoDB 4.4+.
    #[builder(default)]
    pub hint: Option<Hint>,
}

/// Specifies the options to a [`Collection::aggregate`](../struct.Collection.html#method.aggregate)
/// operation.
#[skip_serializing_none]
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder, Serialize)]
#[non_exhaustive]
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
    #[serde(serialize_with = "serialize_batch_size", rename(serialize = "cursor"))]
    pub batch_size: Option<u32>,

    /// Opt out of document-level validation.
    #[builder(default)]
    pub bypass_document_validation: Option<bool>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    #[builder(default)]
    pub collation: Option<Collation>,

    /// Tags the query with an arbitrary string to help trace the operation through the database
    /// profiler, currentOp and logs.
    #[builder(default)]
    pub comment: Option<String>,

    /// The index to use for the operation.
    #[builder(default)]
    pub hint: Option<Hint>,

    /// The maximum amount of time for the server to wait on new documents to satisfy a tailable
    /// await cursor query.
    ///
    /// This option will have no effect on non-tailable cursors that result from this operation.
    #[builder(default)]
    #[serde(
        skip_serializing,
        deserialize_with = "deserialize_duration_from_u64_millis"
    )]
    pub max_await_time: Option<Duration>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[builder(default)]
    #[serde(
        serialize_with = "serialize_duration_as_int_millis",
        rename = "maxTimeMS",
        deserialize_with = "deserialize_duration_from_u64_millis"
    )]
    pub max_time: Option<Duration>,

    /// The read concern to use for the operation.
    ///
    /// If none is specified, the read concern defined on the object executing this operation will
    /// be used.
    #[builder(default)]
    pub read_concern: Option<ReadConcern>,

    /// The criteria used to select a server for this operation.
    ///
    /// If none is specified, the selection criteria defined on the object executing this operation
    /// will be used.
    #[builder(default)]
    #[serde(skip_serializing)]
    #[serde(rename = "readPreference")]
    pub selection_criteria: Option<SelectionCriteria>,

    /// The write concern to use for the operation.
    ///
    /// If none is specified, the write concern defined on the object executing this operation will
    /// be used.
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,
}

/// Specifies the options to a
/// [`Collection::count_documents`](../struct.Collection.html#method.count_documents) operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
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
    #[serde(deserialize_with = "deserialize_duration_from_u64_millis")]
    pub max_time: Option<Duration>,

    /// The number of documents to skip before counting.
    #[builder(default)]
    pub skip: Option<i64>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    #[builder(default)]
    pub collation: Option<Collation>,
}

// rustfmt tries to split the link up when it's all on one line, which breaks the link, so we wrap
// the link contents in whitespace to get it to render correctly.
//
/// Specifies the options to a
/// [
///  `Collection::estimated_document_count`
/// ](../struct.Collection.html#method.estimated_document_count) operation.
#[serde_with::skip_serializing_none]
#[derive(Debug, Default, Deserialize, TypedBuilder, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct EstimatedDocumentCountOptions {
    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[builder(default)]
    #[serde(
        serialize_with = "serialize_duration_as_int_millis",
        rename = "maxTimeMS",
        deserialize_with = "deserialize_duration_from_u64_millis"
    )]
    pub max_time: Option<Duration>,

    /// The criteria used to select a server for this operation.
    ///
    /// If none specified, the default set on the collection will be used.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub selection_criteria: Option<SelectionCriteria>,

    /// The level of the read concern.
    #[builder(default)]
    pub read_concern: Option<ReadConcern>,
}

/// Specifies the options to a [`Collection::distinct`](../struct.Collection.html#method.distinct)
/// operation.
#[serde_with::skip_serializing_none]
#[derive(Debug, Default, Deserialize, TypedBuilder, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DistinctOptions {
    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[builder(default)]
    #[serde(
        serialize_with = "serialize_duration_as_int_millis",
        rename = "maxTimeMS",
        deserialize_with = "deserialize_duration_from_u64_millis"
    )]
    pub max_time: Option<Duration>,

    /// The criteria used to select a server for this operation.
    ///
    /// If none specified, the default set on the collection will be used.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub selection_criteria: Option<SelectionCriteria>,

    /// The level of the read concern.
    #[builder(default)]
    pub read_concern: Option<ReadConcern>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    #[builder(default)]
    pub collation: Option<Collation>,
}

/// Specifies the options to a [`Collection::find`](../struct.Collection.html#method.find)
/// operation.
#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct FindOptions {
    /// Enables writing to temporary files by the server. When set to true, the find operation can
    /// write data to the _tmp subdirectory in the dbPath directory. Only supported in server
    /// versions 4.4+.
    #[builder(default)]
    pub allow_disk_use: Option<bool>,

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
    #[serde(serialize_with = "serialize_u32_as_i32")]
    pub batch_size: Option<u32>,

    /// Tags the query with an arbitrary string to help trace the operation through the database
    /// profiler, currentOp and logs.
    #[builder(default)]
    pub comment: Option<String>,

    /// The type of cursor to return.
    #[builder(default)]
    #[serde(skip)]
    pub cursor_type: Option<CursorType>,

    /// The index to use for the operation.
    #[builder(default)]
    pub hint: Option<Hint>,

    /// The maximum number of documents to query.
    /// If a negative number is specified, the documents will be returned in a single batch limited
    /// in number by the positive value of the specified limit.
    #[builder(default)]
    #[serde(serialize_with = "serialize_absolute_value")]
    pub limit: Option<i64>,

    /// The exclusive upper bound for a specific index.
    #[builder(default)]
    pub max: Option<Document>,

    /// The maximum amount of time for the server to wait on new documents to satisfy a tailable
    /// cursor query. If the cursor is not tailable, this option is ignored.
    #[builder(default)]
    #[serde(skip)]
    pub max_await_time: Option<Duration>,

    /// Maximum number of documents or index keys to scan when executing the query.
    ///
    /// Note: this option is deprecated starting in MongoDB version 4.0 and removed in MongoDB 4.2.
    /// Use the maxTimeMS option instead.
    #[builder(default)]
    pub max_scan: Option<i64>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[builder(default)]
    #[serde(
        rename = "maxTimeMS",
        serialize_with = "serialize_duration_as_int_millis"
    )]
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

    /// The read concern to use for this find query.
    ///
    /// If none specified, the default set on the collection will be used.
    #[builder(default)]
    pub read_concern: Option<ReadConcern>,

    /// Whether to return only the index keys in the documents.
    #[builder(default)]
    pub return_key: Option<bool>,

    /// The criteria used to select a server for this find query.
    ///
    /// If none specified, the default set on the collection will be used.
    #[builder(default)]
    #[serde(skip)]
    pub selection_criteria: Option<SelectionCriteria>,

    /// Whether to return the record identifier for each document.
    #[builder(default)]
    pub show_record_id: Option<bool>,

    /// The number of documents to skip before counting.
    #[builder(default)]
    pub skip: Option<i64>,

    /// The order of the documents for the purposes of the operation.
    #[builder(default)]
    pub sort: Option<Document>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    #[builder(default)]
    pub collation: Option<Collation>,
}

impl From<FindOneOptions> for FindOptions {
    fn from(options: FindOneOptions) -> Self {
        FindOptions {
            allow_disk_use: None,
            allow_partial_results: options.allow_partial_results,
            collation: options.collation,
            comment: options.comment,
            hint: options.hint,
            max: options.max,
            max_scan: options.max_scan,
            max_time: options.max_time,
            min: options.min,
            projection: options.projection,
            read_concern: options.read_concern,
            return_key: options.return_key,
            selection_criteria: options.selection_criteria,
            show_record_id: options.show_record_id,
            skip: options.skip,
            batch_size: None,
            cursor_type: None,
            limit: None,
            max_await_time: None,
            no_cursor_timeout: None,
            sort: options.sort,
        }
    }
}

/// Custom serializer used to serialize limit as its absolute value.
fn serialize_absolute_value<S>(
    val: &Option<i64>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match val {
        Some(v) => serializer.serialize_i64(v.abs()),
        None => serializer.serialize_none(),
    }
}

/// Specifies the options to a [`Collection::find_one`](../struct.Collection.html#method.find_one)
/// operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct FindOneOptions {
    /// If true, partial results will be returned from a mongos rather than an error being
    /// returned if one or more shards is down.
    #[builder(default)]
    pub allow_partial_results: Option<bool>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    #[builder(default)]
    pub collation: Option<Collation>,

    /// Tags the query with an arbitrary string to help trace the operation through the database
    /// profiler, currentOp and logs.
    #[builder(default)]
    pub comment: Option<String>,

    /// The index to use for the operation.
    #[builder(default)]
    pub hint: Option<Hint>,

    /// The exclusive upper bound for a specific index.
    #[builder(default)]
    pub max: Option<Document>,

    /// Maximum number of documents or index keys to scan when executing the query.
    ///
    /// Note: this option is deprecated starting in MongoDB version 4.0 and removed in MongoDB 4.2.
    /// Use the maxTimeMS option instead.
    #[builder(default)]
    pub max_scan: Option<i64>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[builder(default)]
    #[serde(deserialize_with = "deserialize_duration_from_u64_millis")]
    pub max_time: Option<Duration>,

    /// The inclusive lower bound for a specific index.
    #[builder(default)]
    pub min: Option<Document>,

    /// Limits the fields of the document being returned.
    #[builder(default)]
    pub projection: Option<Document>,

    /// The read concern to use for this find query.
    ///
    /// If none specified, the default set on the collection will be used.
    #[builder(default)]
    pub read_concern: Option<ReadConcern>,

    /// Whether to return only the index keys in the documents.
    #[builder(default)]
    pub return_key: Option<bool>,

    /// The criteria used to select a server for this find query.
    ///
    /// If none specified, the default set on the collection will be used.
    #[builder(default)]
    pub selection_criteria: Option<SelectionCriteria>,

    /// Whether to return the record identifier for each document.
    #[builder(default)]
    pub show_record_id: Option<bool>,

    /// The number of documents to skip before counting.
    #[builder(default)]
    pub skip: Option<i64>,

    /// The order of the documents for the purposes of the operation.
    #[builder(default)]
    pub sort: Option<Document>,
}

/// Specifies indexes to create.
/// 
/// [See](https://docs.mongodb.com/manual/reference/command/createIndexes/) for more.
#[derive(Clone, Debug, Default, TypedBuilder, Serialize)]
#[non_exhaustive]
//#[serde(rename_all = "camelCase")]
pub struct CreateIndexesOptions {
    /// The write concern for the operation.
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,

    /// Either integer or string
    /// Optional. The minimum number of data-bearing voting replica set members
    /// 
    /// [See](https://docs.mongodb.com/manual/reference/command/createIndexes/#createindexes-cmd-commitquorum) for more.
    #[serde(with = "either::serde_untagged_optional")]
    #[builder(default)]
    pub commit_quorum: Option<Either<String, i32>>,

    /// Tags the query with an arbitrary string to help trace the operation through the database
    /// profiler, currentOp and logs.
    #[builder(default)]
    pub comment: Option<Document>,
}

/// We must implement serialize on this
#[derive(Clone, Debug)]
pub enum IndexType {
    Descending, // -1
    Ascending, // 1
    Hashed, // "hashed"
    Sphere2d, // "2dsphere"
    D2, // "2d"
    #[deprecated]
    GeoSpacial, // "geoHaystack"
    Text, // "text"
}

impl Serialize for IndexType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer {
            match self {
                IndexType::Descending => serializer.serialize_i8(-1),
                IndexType::Ascending => serializer.serialize_i8(1),
                IndexType::Hashed => serializer.serialize_str("hashed"),
                IndexType::Sphere2d => serializer.serialize_str("2dsphere"),
                IndexType::D2 => serializer.serialize_str("2d"),
                #[allow(deprecated)] IndexType::GeoSpacial => serializer.serialize_str("geoHaystack"),
                IndexType::Text => serializer.serialize_str("text"),
            }
    }
}

/// Specifies the options to a [`Collection::create_index`](../struct.Collection.html#method.create_index)
/// operation.
/// See the
/// [documentation](https://docs.mongodb.com/manual/reference/method/db.collection.createIndex/#options-for-all-index-types)
/// for more information on how to use this option.
#[derive(Debug, Default, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Index {
    /// Specifies the index’s fields. For each field, specify a key-value pair in which the key is the name of the field to
    /// index and the value is index type.
    #[builder(default)]
    pub key: HashMap<String, IndexType>,

    /// Optional. Deprecated in MongoDB 4.2.
    #[deprecated]
    #[builder(default)]
    pub background: bool,

    /// Optional. Creates a unique index so that the collection will not accept insertion or update of documents
    /// where the index key value matches an existing value in the index.
    ///
    /// Specify true to create a unique index. The default value is false.
    ///
    /// The option is unavailable for [hashed](https://docs.mongodb.com/manual/core/index-hashed/) indexes.
    #[builder(default)]
    pub unique: Option<bool>,

    /// Optional. The name of the index.
    ///
    /// If unspecified, MongoDB generates an index name by concatenating the names of the indexed fields and the sort order.
    #[builder(default)]
    pub name: Option<String>,

    /// Optional. If specified, the index only references documents that match the filter expression. See Partial Indexes for
    /// more information.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/core/index-partial/) for more
    /// information on how to use this option.
    #[builder(default)]
    pub partial_filter_expression: Option<Document>,

    /// Optional. If true, the index only references documents with the specified field.
    ///
    /// These indexes use less space but behave differently in some situations (particularly sorts).
    /// 
    /// The default value is false.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/core/index-sparse/)
    /// for more information on how to use this option.
    #[builder(default)]
    pub sparse: Option<bool>,

    /// Optional. Specifies a value, in seconds, as a TTL to control how long MongoDB retains documents in this collection.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/tutorial/expire-data/)
    /// for more information on how to use this option.
    ///
    /// This applies only to [TTL](https://docs.mongodb.com/manual/reference/glossary/#term-ttl) indexes.
    #[builder(default)]
    #[serde(serialize_with = "serialize_u32_as_i32")]
    pub expire_after_seconds: Option<u32>,

    /// Optional. A flag that determines whether the index is hidden from the query planner. A hidden index is not evaluated as 
    /// part of the query plan selection.
    ///
    /// Default is false.
    /// 
    /// See the
    /// [documentation](https://docs.mongodb.com/manual/reference/method/db.collection.createIndex/#method-createindex-hidden)
    /// for more information on how to use this option.
    #[builder(default)]
    pub hidden: Option<bool>,

    /// Optional. Allows users to configure the storage engine on a per-index basis when creating an index.
    ///
    /// The storageEngine option should take the following form:
    ///
    /// storageEngine: { <storage-engine-name>: <options> }
    #[builder(default)]
    pub storage_engine: Option<Document>,

    /// Optional. Specifies the collation for the index.
    /// 
    /// [Collation](https://docs.mongodb.com/manual/reference/collation/) allows users to specify language-specific rules for
    /// string comparison, such as rules for lettercase and accent marks.
    #[builder(default)]
    pub collation: Option<Collation>,

    // text

    /// Optional. For text indexes, a document that contains field and weight pairs.
    /// 
    /// The weight is an integer ranging from 1 to 99,999 and denotes the significance of the field relative to the other
    /// indexed fields in terms of the score.
    /// You can specify weights for some or all the indexed fields.
    /// See [Control Search Results with Weights](https://docs.mongodb.com/manual/tutorial/control-results-of-text-search/)
    /// to adjust the scores.
    /// 
    /// The default value is 1.
    #[builder(default)]
    pub weights: Option<Document>,

    /// Optional. For text indexes, the language that determines the list of stop words and the rules for the stemmer and
    /// tokenizer.
    /// 
    /// See [Text Search Languages](https://docs.mongodb.com/manual/reference/text-search-languages/#text-search-languages)
    /// for the available languages and
    /// [Specify a Language for Text Index](https://docs.mongodb.com/manual/tutorial/specify-language-for-text-index/) for
    /// more information and examples.
    /// 
    /// The default value is english.
    #[builder(default)]
    pub default_language: Option<String>,

    /// Optional. For text indexes, the name of the field, in the collection’s documents, that contains the override language
    /// for the document. The default value is language. See
    /// [Use any Field to Specify the Language for a Document](https://docs.mongodb.com/manual/tutorial/specify-language-for-text-index/#specify-language-field-text-index-example) for an example.
    #[builder(default)]
    pub language_override: Option<String>,

    /// Optional. The text index version number. Users can use this option to override the default version number.
    #[serde(serialize_with = "serialize_u32_as_i32")]
    #[builder(default)]
    pub text_index_version: Option<u32>,

    // wildcard

    /// Optional. Allows users to include or exclude specific field paths from a
    /// [wildcard index](https://docs.mongodb.com/manual/core/index-wildcard/#wildcard-index-core)
    /// using the { "$**" : 1} key pattern.
    /// 
    /// This is only used when you specific a wildcard index field
    #[builder(default)]
    pub wildcard_projection: Option<Document>,

    // 2d

    /// Optional. For 2d indexes, the number of precision of the stored geohash value of the location data.
    /// The bits value ranges from 1 to 32 inclusive.
    /// 
    /// The default value is 26.
    #[builder(default)]
    #[serde(serialize_with = "serialize_u32_as_i32")]
    pub bits: Option<u32>,

    /// Optional. For 2d indexes, the lower inclusive boundary for the longitude and latitude values.
    /// 
    /// The default value is -180.0.
    #[builder(default)]
    pub min: Option<f64>,

    /// Optional. For 2d indexes, the upper inclusive boundary for the longitude and latitude values.
    /// 
    /// The default value is -180.0.
    #[builder(default)]
    pub max: Option<f64>,

    // 2dsphere

    /// Optional. The 2dsphere index version number.
    /// Users can use this option to override the default version number.
    #[builder(default)]
    #[serde(serialize_with = "serialize_u32_as_i32")]
    #[serde(rename = "2dsphereIndexVersion")]
    pub sphere2d_index_version: Option<u32>,

    // GeoHayStack

    /// For geoHaystack indexes, specify the number of units within which to group the location values;
    /// i.e. group in the same bucket those location values that are within the specified number of units to each other.
    /// 
    /// The value must be greater than 0.
    #[builder(default)]
    bucket_size: Option<f64>,
}

/// Specifies the options to a [`Collection::drop`](../struct.Collection.html#method.drop)
/// operation.
#[derive(Debug, Default, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DropCollectionOptions {
    /// The write concern for the operation.
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,
}
