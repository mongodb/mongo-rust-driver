use std::time::Duration;

use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
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

/// These are the valid options for creating a [`Collection`](../struct.Collection.html) with
/// [`Database::collection_with_options`](../struct.Database.html#method.collection_with_options).
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CollectionOptions {
    /// The default read preference for operations.
    pub selection_criteria: Option<SelectionCriteria>,

    /// The default read concern for operations.
    pub read_concern: Option<ReadConcern>,

    /// The default write concern for operations.
    pub write_concern: Option<WriteConcern>,
}

/// Specifies whether a
/// [`Collection::find_one_and_replace`](../struct.Collection.html#method.find_one_and_replace) and
/// [`Collection::find_one_and_update`](../struct.Collection.html#method.find_one_and_update)
/// operation should return the document before or after modification.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ReturnDocument {
    /// Return the document after modification.
    After,
    /// Return the document before modification.
    Before,
}

impl<'de> Deserialize<'de> for ReturnDocument {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "after" => Ok(ReturnDocument::After),
            "before" => Ok(ReturnDocument::Before),
            other => Err(D::Error::custom(format!(
                "Unknown return document value: {}",
                other
            ))),
        }
    }
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
#[builder(field_defaults(default, setter(strip_option)))]
#[non_exhaustive]
pub struct InsertOneOptions {
    /// Opt out of document-level validation.
    pub bypass_document_validation: Option<bool>,

    /// The write concern for the operation.
    pub write_concern: Option<WriteConcern>,
}

/// Specifies the options to a
/// [`Collection::insert_many`](../struct.Collection.html#method.insert_many) operation.
#[skip_serializing_none]
#[derive(Clone, Debug, Default, TypedBuilder, Serialize, Deserialize)]
#[builder(field_defaults(default, setter(strip_option)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct InsertManyOptions {
    /// Opt out of document-level validation.
    pub bypass_document_validation: Option<bool>,

    /// If true, when an insert fails, return without performing the remaining writes. If false,
    /// when a write fails, continue with the remaining writes, if any.
    ///
    /// Defaults to true.
    pub ordered: Option<bool>,

    /// The write concern for the operation.
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
#[builder(field_defaults(default, setter(strip_option)))]
#[non_exhaustive]
pub struct UpdateOptions {
    /// A set of filters specifying to which array elements an update should apply.
    ///
    /// See the documentation [here](https://docs.mongodb.com/manual/reference/command/update/) for
    /// more information on array filters.
    pub array_filters: Option<Vec<Document>>,

    /// Opt out of document-level validation.
    pub bypass_document_validation: Option<bool>,

    /// If true, insert a document if no matching document is found.
    pub upsert: Option<bool>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    pub collation: Option<Collation>,

    /// A document or string that specifies the index to use to support the query predicate.
    ///
    /// Only available in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://docs.mongodb.com/manual/reference/command/update/#ex-update-command-hint) for examples.
    pub hint: Option<Hint>,

    /// The write concern for the operation.
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
#[builder(field_defaults(default, setter(strip_option)))]
#[non_exhaustive]
pub struct ReplaceOptions {
    /// Opt out of document-level validation.
    pub bypass_document_validation: Option<bool>,

    /// If true, insert a document if no matching document is found.
    pub upsert: Option<bool>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    pub collation: Option<Collation>,

    /// A document or string that specifies the index to use to support the query predicate.
    ///
    /// Only available in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://docs.mongodb.com/manual/reference/command/update/#ex-update-command-hint) for examples.
    pub hint: Option<Hint>,

    /// The write concern for the operation.
    pub write_concern: Option<WriteConcern>,
}

/// Specifies the options to a
/// [`Collection::delete_one`](../struct.Collection.html#method.delete_one) or
/// [`Collection::delete_many`](../struct.Collection.html#method.delete_many) operation.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(strip_option)))]
#[non_exhaustive]
pub struct DeleteOptions {
    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    pub collation: Option<Collation>,

    /// The write concern for the operation.
    pub write_concern: Option<WriteConcern>,

    /// The index to use for the operation.
    /// Only available in MongoDB 4.4+.
    pub hint: Option<Hint>,
}

/// Specifies the options to a
/// [`Collection::find_one_and_delete`](../struct.Collection.html#method.find_one_and_delete)
/// operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(strip_option)))]
#[non_exhaustive]
pub struct FindOneAndDeleteOptions {
    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    pub max_time: Option<Duration>,

    /// Limits the fields of the document being returned.
    pub projection: Option<Document>,

    /// The order of the documents for the purposes of the operation.
    pub sort: Option<Document>,

    /// The level of the write concern
    pub write_concern: Option<WriteConcern>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    pub collation: Option<Collation>,

    /// The index to use for the operation.
    /// Only available in MongoDB 4.4+.
    pub hint: Option<Hint>,
}

/// Specifies the options to a
/// [`Collection::find_one_and_replace`](../struct.Collection.html#method.find_one_and_replace)
/// operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct FindOneAndReplaceOptions {
    /// Opt out of document-level validation.
    pub bypass_document_validation: Option<bool>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    pub max_time: Option<Duration>,

    /// Limits the fields of the document being returned.
    pub projection: Option<Document>,

    /// Whether the operation should return the document before or after modification.
    pub return_document: Option<ReturnDocument>,

    /// The order of the documents for the purposes of the operation.
    pub sort: Option<Document>,

    /// If true, insert a document if no matching document is found.
    pub upsert: Option<bool>,

    /// The level of the write concern
    pub write_concern: Option<WriteConcern>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    pub collation: Option<Collation>,

    /// The index to use for the operation.
    /// Only available in MongoDB 4.4+.
    pub hint: Option<Hint>,
}

/// Specifies the options to a
/// [`Collection::find_one_and_update`](../struct.Collection.html#method.find_one_and_update)
/// operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(strip_option)))]
#[non_exhaustive]
pub struct FindOneAndUpdateOptions {
    /// A set of filters specifying to which array elements an update should apply.
    ///
    /// See the documentation [here](https://docs.mongodb.com/manual/reference/command/update/) for
    /// more information on array filters.
    pub array_filters: Option<Vec<Document>>,

    /// Opt out of document-level validation.
    pub bypass_document_validation: Option<bool>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    pub max_time: Option<Duration>,

    /// Limits the fields of the document being returned.
    pub projection: Option<Document>,

    /// Whether the operation should return the document before or after modification.
    pub return_document: Option<ReturnDocument>,

    /// The order of the documents for the purposes of the operation.
    pub sort: Option<Document>,

    /// If true, insert a document if no matching document is found.
    pub upsert: Option<bool>,

    /// The level of the write concern
    pub write_concern: Option<WriteConcern>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    pub collation: Option<Collation>,

    /// The index to use for the operation.
    /// Only available in MongoDB 4.4+.
    pub hint: Option<Hint>,
}

/// Specifies the options to a [`Collection::aggregate`](../struct.Collection.html#method.aggregate)
/// operation.
#[skip_serializing_none]
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder, Serialize)]
#[builder(field_defaults(default, setter(strip_option)))]
#[non_exhaustive]
pub struct AggregateOptions {
    /// Enables writing to temporary files. When set to true, aggregation stages can write data to
    /// the _tmp subdirectory in the dbPath directory.
    pub allow_disk_use: Option<bool>,

    /// The number of documents the server should return per cursor batch.
    ///
    /// Note that this does not have any affect on the documents that are returned by a cursor,
    /// only the number of documents kept in memory at a given time (and by extension, the
    /// number of round trips needed to return the entire set of documents returned by the
    /// query).
    #[serde(serialize_with = "serialize_batch_size", rename(serialize = "cursor"))]
    pub batch_size: Option<u32>,

    /// Opt out of document-level validation.
    pub bypass_document_validation: Option<bool>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    pub collation: Option<Collation>,

    /// Tags the query with an arbitrary string to help trace the operation through the database
    /// profiler, currentOp and logs.
    pub comment: Option<String>,

    /// The index to use for the operation.
    pub hint: Option<Hint>,

    /// The maximum amount of time for the server to wait on new documents to satisfy a tailable
    /// await cursor query.
    ///
    /// This option will have no effect on non-tailable cursors that result from this operation.
    #[serde(
        skip_serializing,
        deserialize_with = "deserialize_duration_from_u64_millis"
    )]
    pub max_await_time: Option<Duration>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
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
    pub read_concern: Option<ReadConcern>,

    /// The criteria used to select a server for this operation.
    ///
    /// If none is specified, the selection criteria defined on the object executing this operation
    /// will be used.
    #[serde(skip_serializing)]
    #[serde(rename = "readPreference")]
    pub selection_criteria: Option<SelectionCriteria>,

    /// The write concern to use for the operation.
    ///
    /// If none is specified, the write concern defined on the object executing this operation will
    /// be used.
    pub write_concern: Option<WriteConcern>,
}

/// Specifies the options to a
/// [`Collection::count_documents`](../struct.Collection.html#method.count_documents) operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(strip_option)))]
#[non_exhaustive]
pub struct CountOptions {
    /// The index to use for the operation.
    pub hint: Option<Hint>,

    /// The maximum number of documents to count.
    pub limit: Option<i64>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[serde(deserialize_with = "deserialize_duration_from_u64_millis")]
    pub max_time: Option<Duration>,

    /// The number of documents to skip before counting.
    pub skip: Option<i64>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    pub collation: Option<Collation>,

    /// The criteria used to select a server for this operation.
    ///
    /// If none specified, the default set on the collection will be used.
    #[serde(skip_serializing)]
    pub selection_criteria: Option<SelectionCriteria>,

    /// The level of the read concern.
    pub read_concern: Option<ReadConcern>,
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
#[builder(field_defaults(default, setter(strip_option)))]
#[non_exhaustive]
pub struct EstimatedDocumentCountOptions {
    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[serde(
        serialize_with = "serialize_duration_as_int_millis",
        rename = "maxTimeMS",
        deserialize_with = "deserialize_duration_from_u64_millis"
    )]
    pub max_time: Option<Duration>,

    /// The criteria used to select a server for this operation.
    ///
    /// If none specified, the default set on the collection will be used.
    #[serde(skip_serializing)]
    pub selection_criteria: Option<SelectionCriteria>,

    /// The level of the read concern.
    pub read_concern: Option<ReadConcern>,
}

/// Specifies the options to a [`Collection::distinct`](../struct.Collection.html#method.distinct)
/// operation.
#[serde_with::skip_serializing_none]
#[derive(Debug, Default, Deserialize, TypedBuilder, Serialize, Clone)]
#[builder(field_defaults(default, setter(strip_option)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DistinctOptions {
    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[serde(
        serialize_with = "serialize_duration_as_int_millis",
        rename = "maxTimeMS",
        deserialize_with = "deserialize_duration_from_u64_millis"
    )]
    pub max_time: Option<Duration>,

    /// The criteria used to select a server for this operation.
    ///
    /// If none specified, the default set on the collection will be used.
    #[serde(skip_serializing)]
    pub selection_criteria: Option<SelectionCriteria>,

    /// The level of the read concern.
    pub read_concern: Option<ReadConcern>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    pub collation: Option<Collation>,
}

/// Specifies the options to a [`Collection::find`](../struct.Collection.html#method.find)
/// operation.
#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder, Serialize)]
#[builder(field_defaults(default, setter(strip_option)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct FindOptions {
    /// Enables writing to temporary files by the server. When set to true, the find operation can
    /// write data to the _tmp subdirectory in the dbPath directory. Only supported in server
    /// versions 4.4+.
    pub allow_disk_use: Option<bool>,

    /// If true, partial results will be returned from a mongos rather than an error being
    /// returned if one or more shards is down.
    pub allow_partial_results: Option<bool>,

    /// The number of documents the server should return per cursor batch.
    ///
    /// Note that this does not have any affect on the documents that are returned by a cursor,
    /// only the number of documents kept in memory at a given time (and by extension, the
    /// number of round trips needed to return the entire set of documents returned by the
    /// query.
    #[serde(serialize_with = "serialize_u32_as_i32")]
    pub batch_size: Option<u32>,

    /// Tags the query with an arbitrary string to help trace the operation through the database
    /// profiler, currentOp and logs.
    pub comment: Option<String>,

    /// The type of cursor to return.
    #[serde(skip)]
    pub cursor_type: Option<CursorType>,

    /// The index to use for the operation.
    pub hint: Option<Hint>,

    /// The maximum number of documents to query.
    /// If a negative number is specified, the documents will be returned in a single batch limited
    /// in number by the positive value of the specified limit.
    #[serde(serialize_with = "serialize_absolute_value")]
    pub limit: Option<i64>,

    /// The exclusive upper bound for a specific index.
    pub max: Option<Document>,

    /// The maximum amount of time for the server to wait on new documents to satisfy a tailable
    /// cursor query. If the cursor is not tailable, this option is ignored.
    #[serde(skip)]
    pub max_await_time: Option<Duration>,

    /// Maximum number of documents or index keys to scan when executing the query.
    ///
    /// Note: this option is deprecated starting in MongoDB version 4.0 and removed in MongoDB 4.2.
    /// Use the maxTimeMS option instead.
    pub max_scan: Option<i64>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[serde(
        rename = "maxTimeMS",
        serialize_with = "serialize_duration_as_int_millis"
    )]
    pub max_time: Option<Duration>,

    /// The inclusive lower bound for a specific index.
    pub min: Option<Document>,

    /// Whether the server should close the cursor after a period of inactivity.
    pub no_cursor_timeout: Option<bool>,

    /// Limits the fields of the document being returned.
    pub projection: Option<Document>,

    /// The read concern to use for this find query.
    ///
    /// If none specified, the default set on the collection will be used.
    pub read_concern: Option<ReadConcern>,

    /// Whether to return only the index keys in the documents.
    pub return_key: Option<bool>,

    /// The criteria used to select a server for this find query.
    ///
    /// If none specified, the default set on the collection will be used.
    #[serde(skip)]
    pub selection_criteria: Option<SelectionCriteria>,

    /// Whether to return the record identifier for each document.
    pub show_record_id: Option<bool>,

    /// The number of documents to skip before counting.
    pub skip: Option<i64>,

    /// The order of the documents for the purposes of the operation.
    pub sort: Option<Document>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
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
            limit: Some(-1),
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
#[builder(field_defaults(default, setter(strip_option)))]
#[non_exhaustive]
pub struct FindOneOptions {
    /// If true, partial results will be returned from a mongos rather than an error being
    /// returned if one or more shards is down.
    pub allow_partial_results: Option<bool>,

    /// The collation to use for the operation.
    ///
    /// See the [documentation](https://docs.mongodb.com/manual/reference/collation/) for more
    /// information on how to use this option.
    pub collation: Option<Collation>,

    /// Tags the query with an arbitrary string to help trace the operation through the database
    /// profiler, currentOp and logs.
    pub comment: Option<String>,

    /// The index to use for the operation.
    pub hint: Option<Hint>,

    /// The exclusive upper bound for a specific index.
    pub max: Option<Document>,

    /// Maximum number of documents or index keys to scan when executing the query.
    ///
    /// Note: this option is deprecated starting in MongoDB version 4.0 and removed in MongoDB 4.2.
    /// Use the maxTimeMS option instead.
    pub max_scan: Option<i64>,

    /// The maximum amount of time to allow the query to run.
    ///
    /// This options maps to the `maxTimeMS` MongoDB query option, so the duration will be sent
    /// across the wire as an integer number of milliseconds.
    #[serde(deserialize_with = "deserialize_duration_from_u64_millis")]
    pub max_time: Option<Duration>,

    /// The inclusive lower bound for a specific index.
    pub min: Option<Document>,

    /// Limits the fields of the document being returned.
    pub projection: Option<Document>,

    /// The read concern to use for this find query.
    ///
    /// If none specified, the default set on the collection will be used.
    pub read_concern: Option<ReadConcern>,

    /// Whether to return only the index keys in the documents.
    pub return_key: Option<bool>,

    /// The criteria used to select a server for this find query.
    ///
    /// If none specified, the default set on the collection will be used.
    pub selection_criteria: Option<SelectionCriteria>,

    /// Whether to return the record identifier for each document.
    pub show_record_id: Option<bool>,

    /// The number of documents to skip before counting.
    pub skip: Option<i64>,

    /// The order of the documents for the purposes of the operation.
    pub sort: Option<Document>,
}

/// Specifies the options to a [`Collection::drop`](../struct.Collection.html#method.drop)
/// operation.
#[serde_with::skip_serializing_none]
#[derive(Debug, Default, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(strip_option)))]
#[non_exhaustive]
pub struct DropCollectionOptions {
    /// The write concern for the operation.
    pub write_concern: Option<WriteConcern>,
}
