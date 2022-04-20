use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use typed_builder::TypedBuilder;

use crate::{
    bson::Document,
    bson_util,
    concern::{ReadConcern, WriteConcern},
    options::Collation,
    selection_criteria::SelectionCriteria,
};

/// These are the valid options for creating a [`Database`](../struct.Database.html) with
/// [`Client::database_with_options`](../struct.Client.html#method.database_with_options).
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct DatabaseOptions {
    /// The default read preference for operations.
    pub selection_criteria: Option<SelectionCriteria>,

    /// The default read concern for operations.
    pub read_concern: Option<ReadConcern>,

    /// The default write concern for operations.
    pub write_concern: Option<WriteConcern>,
}

/// These are the valid options for creating a collection with
/// [`Database::create_collection`](../struct.Database.html#method.create_collection).
#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct CreateCollectionOptions {
    /// Whether the collection should be capped. If true, `size` must also be set.
    pub capped: Option<bool>,

    /// The maximum size (in bytes) for a capped collection. This option is ignored if `capped` is
    /// not set to true.
    #[serde(serialize_with = "bson_util::serialize_u64_option_as_i64")]
    pub size: Option<u64>,

    /// The maximum number of documents in a capped collection. The `size` limit takes precedence
    /// over this option. If a capped collection reaches the size limit before it reaches the
    /// maximum number of documents, MongoDB removes old documents.
    #[serde(serialize_with = "bson_util::serialize_u64_option_as_i64")]
    pub max: Option<u64>,

    /// The storage engine that the collection should use. The value should take the following
    /// form:
    ///
    /// `{ <storage-engine-name>: <options> }`
    pub storage_engine: Option<Document>,

    /// Specifies a validator to restrict the schema of documents which can exist in the
    /// collection. Expressions can be specified using any query operators except `$near`,
    /// `$nearSphere`, `$text`, and `$where`.
    pub validator: Option<Document>,

    /// Specifies how strictly the database should apply the validation rules to existing documents
    /// during an update.
    pub validation_level: Option<ValidationLevel>,

    /// Specifies whether the database should return an error or simply raise a warning if inserted
    /// documents do not pass the validation.
    pub validation_action: Option<ValidationAction>,

    /// The name of the source collection or view to base this view on. If specified, this will
    /// cause a view to be created rather than a collection.
    pub view_on: Option<String>,

    /// An array that consists of the aggregation pipeline stages to run against `view_on` to
    /// determine the contents of the view.
    pub pipeline: Option<Vec<Document>>,

    /// The default collation for the collection or view.   
    pub collation: Option<Collation>,

    /// The write concern for the operation.   
    pub write_concern: Option<WriteConcern>,

    /// The default configuration for indexes created on this collection, including the _id index.
    pub index_option_defaults: Option<IndexOptionDefaults>,

    /// An object containing options for creating time series collections. See the [`create`
    /// command documentation](https://docs.mongodb.com/manual/reference/command/create/) for
    /// supported options, and the [Time Series Collections documentation](
    /// https://docs.mongodb.com/manual/core/timeseries-collections/) for more information.
    ///
    /// This feature is only available on server versions 5.0 and above.
    pub timeseries: Option<TimeseriesOptions>,

    /// Used to automatically delete documents in time series collections. See the [`create`
    /// command documentation](https://docs.mongodb.com/manual/reference/command/create/) for more
    /// information.
    #[serde(
        default,
        deserialize_with = "bson_util::deserialize_duration_option_from_u64_seconds",
        serialize_with = "bson_util::serialize_duration_option_as_int_secs"
    )]
    pub expire_after_seconds: Option<Duration>,

    /// Options for supporting change stream pre- and post-images.
    pub change_stream_pre_and_post_images: Option<ChangeStreamPreAndPostImages>,
}

/// Specifies how strictly the database should apply validation rules to existing documents during
/// an update.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum ValidationLevel {
    /// Perform no validation for inserts and updates.
    Off,
    /// Perform validation on all inserts and updates.
    Strict,
    /// Perform validation on inserts as well as updates on existing valid documents, but do not
    /// perform validations on updates on existing invalid documents.
    Moderate,
}

/// Specifies whether the database should return an error or simply raise a warning if inserted
/// documents do not pass the validation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum ValidationAction {
    /// Return an error if inserted documents do not pass the validation.
    Error,
    /// Raise a warning if inserted documents do not pass the validation.
    Warn,
}

/// Specifies default configuration for indexes created on a collection, including the _id index.
#[derive(Clone, Debug, TypedBuilder, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct IndexOptionDefaults {
    /// The `storageEngine` document should be in the following form:
    ///
    /// `{ <storage-engine-name>: <options> }`
    pub storage_engine: Document,
}

/// Specifies options for creating a timeseries collection.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TimeseriesOptions {
    /// Name of the top-level field to be used for time. Inserted documents must have this field,
    /// and the field must be of the BSON UTC datetime type.
    pub time_field: String,

    /// Name of the top-level field describing the series. This field is used to group related data
    /// and may be of any BSON type, except for array. This name may not be the same as the
    /// timeField or _id.
    pub meta_field: Option<String>,

    /// The units you'd use to describe the expected interval between subsequent measurements for a
    /// time-series.  Defaults to `TimeseriesGranularity::Seconds` if unset.
    pub granularity: Option<TimeseriesGranularity>,
}

/// The units you'd use to describe the expected interval between subsequent measurements for a
/// time-series.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum TimeseriesGranularity {
    /// The expected interval between subsequent measurements is in seconds.
    Seconds,
    /// The expected interval between subsequent measurements is in minutes.
    Minutes,
    /// The expected interval between subsequent measurements is in hours.
    Hours,
}

/// Specifies the options to a [`Database::drop`](../struct.Database.html#method.drop) operation.
#[derive(Clone, Debug, Default, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct DropDatabaseOptions {
    /// The write concern for the operation.
    pub write_concern: Option<WriteConcern>,
}

/// Specifies the options to a
/// [`Database::list_collections`](../struct.Database.html#method.list_collections) operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct ListCollectionsOptions {
    /// The number of documents the server should return per cursor batch.
    ///
    /// Note that this does not have any affect on the documents that are returned by a cursor,
    /// only the number of documents kept in memory at a given time (and by extension, the
    /// number of round trips needed to return the entire set of documents returned by the
    /// query).
    #[serde(
        serialize_with = "bson_util::serialize_u32_option_as_batch_size",
        rename(serialize = "cursor")
    )]
    pub batch_size: Option<u32>,
}

/// Specifies the options to a
/// [`Client::list_databases`](../struct.Client.html#method.list_databases) operation.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct ListDatabasesOptions {
    /// Determines which databases to return based on the user's access privileges. This option is
    /// only supported on server versions 4.0.5+.
    pub authorized_databases: Option<bool>,
}

/// Specifies how change stream pre- and post-images should be supported.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct ChangeStreamPreAndPostImages {
    /// If `true`, change streams will be able to include pre- and post-images.
    pub enabled: bool,
}
