use bson::Document;
use serde::Serialize;
use serde_with::skip_serializing_none;

use crate::{
    concern::{ReadConcern, WriteConcern},
    read_preference::ReadPreference,
};

/// These are the valid options for creating a `Database` with `Client::database_with_options`.
#[derive(Debug, Default, TypedBuilder)]
pub struct DatabaseOptions {
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

/// These are the valid options for creating a collection with `Database::create_collection`.
#[skip_serializing_none]
#[derive(Debug, Default, TypedBuilder, Serialize)]
#[serde(rename_all = "camelCase")]
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
    pub collation: Option<Document>,
}

/// Specifies how strictly the database should apply validation rules to existing documents during
/// an update.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ValidationLevel {
    /// Perform no validation for inserts and updates.
    Off,
    /// Perform validation on all inserts and updates.
    Strict,
    /// Perform validation on inserts as well as updates on existing valid documents, but do not
    /// perform validations on updates on existing invalid documents.
    Moderate,
}

// Specifies whether the database should return an error or simply raise a warning if inserted
// documents do not pass the validation.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ValidationAction {
    Error,
    Warn,
}
