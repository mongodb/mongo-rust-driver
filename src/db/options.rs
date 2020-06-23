use serde::Serialize;
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
#[derive(Clone, Debug, Default, TypedBuilder)]
#[non_exhaustive]
pub struct DatabaseOptions {
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

/// These are the valid options for creating a collection with
/// [`Database::create_collection`](../struct.Database.html#method.create_collection).
#[skip_serializing_none]
#[derive(Debug, Default, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CreateCollectionOptions {
    /// Whether the collection should be capped. If true, `size` must also be set.
    #[builder(default)]
    pub capped: Option<bool>,

    /// The maximum size (in bytes) for a capped collection. This option is ignored if `capped` is
    /// not set to true.
    #[builder(default)]
    pub size: Option<i64>,

    /// The maximum number of documents in a capped collection. The `size` limit takes precedence
    /// over this option. If a capped collection reaches the size limit before it reaches the
    /// maximum number of documents, MongoDB removes old documents.
    #[builder(default)]
    pub max: Option<i64>,

    /// The storage engine that the collection should use. The value should take the following
    /// form:
    ///
    /// `{ <storage-engine-name>: <options> }`
    #[builder(default)]
    pub storage_engine: Option<Document>,

    /// Specifies a validator to restrict the schema of documents which can exist in the
    /// collection. Expressions can be specified using any query operators except `$near`,
    /// `$nearSphere`, `$text`, and `$where`.
    #[builder(default)]
    #[serde(rename = "validator")]
    pub validation: Option<Document>,

    /// Specifies how strictly the database should apply the validation rules to existing documents
    /// during an update.
    #[builder(default)]
    pub validation_level: Option<ValidationLevel>,

    /// Specifies whether the database should return an error or simply raise a warning if inserted
    /// documents do not pass the validation.
    #[builder(default)]
    pub validation_action: Option<ValidationAction>,

    /// The name of the source collection or view to base this view on. If specified, this will
    /// cause a view to be created rather than a collection.
    #[builder(default)]
    pub view_on: Option<String>,

    /// An array that consists of the aggregation pipeline stages to run against `view_on` to
    /// determine the contents of the view.
    #[builder(default)]
    pub pipeline: Option<Vec<Document>>,

    /// The default collation for the collection or view.   
    #[builder(default)]
    pub collation: Option<Collation>,

    /// The write concern for the operation.   
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,

    /// The default configuration for indexes created on this collection, including the _id index.
    #[builder(default)]
    pub index_option_defaults: Option<IndexOptionDefaults>,
}

/// Specifies how strictly the database should apply validation rules to existing documents during
/// an update.
#[derive(Debug, Serialize)]
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
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum ValidationAction {
    Error,
    Warn,
}

/// Specifies default configuration for indexes created on a collection, including the _id index.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexOptionDefaults {
    /// The `storageEngine` document should be in the following form:
    ///
    /// `{ <storage-engine-name>: <options> }`
    pub storage_engine: Document,
}

/// Specifies the options to a [`Database::drop`](../struct.Database.html#method.drop) operation.
#[derive(Debug, Default, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DropDatabaseOptions {
    /// The write concern for the operation.
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,
}

/// Specifies the options to a
/// [`Database::list_collections`](../struct.Database.html#method.list_collections) operation.
#[derive(Debug, Default, TypedBuilder, Serialize)]
#[non_exhaustive]
pub struct ListCollectionsOptions {
    /// The number of documents the server should return per cursor batch.
    ///
    /// Note that this does not have any affect on the documents that are returned by a cursor,
    /// only the number of documents kept in memory at a given time (and by extension, the
    /// number of round trips needed to return the entire set of documents returned by the
    /// query).
    #[builder(default)]
    #[serde(serialize_with = "bson_util::serialize_batch_size", rename = "cursor")]
    pub batch_size: Option<u32>,
}

/// Specifies the options to a
/// [`Client::list_databases`](../struct.Client.html#method.list_databases) operation.
#[derive(Debug, Default, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ListDatabasesOptions {
    /// Determines which databases to return based on the user's access privileges. This option is
    /// only supported on server versions 4.0.5+.
    #[builder(default)]
    pub authorized_databases: Option<bool>,
}
