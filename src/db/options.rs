use bson::Document;

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
#[derive(Debug, Default, TypedBuilder)]
pub struct CreateCollectionOptions {
    /// Whether the collection should be capped. If true, `size` must also be set.
    #[builder(default)]
    capped: Option<bool>,

    /// The maximum size (in bytes) for a capped collection. This option is ignored if `capped` is
    /// not set to true.
    #[builder(default)]
    size: Option<i64>,

    /// The maximum number of documents in a capped collection. The `size` limit takes precedence
    /// over this option. If a capped collection reaches the size limit before it reaches the
    /// maximum number of documents, MongoDB removes old documents.
    #[builder(default)]
    max: Option<i64>,

    /// The storage engine that the collection should use. The value should take thefollowing
    /// form:
    ///
    /// `{ <storage-engine-name>: <options> }`
    #[builder(default)]
    storage_engine: Option<Document>,

    /// Specifies a validator to restrict the schema of documents which can exist inthe
    /// collection. Expressions can be specified using any query operators except `$near`,
    /// `$nearSphere`, `$text`, and `$where`.
    #[builder(default)]
    validation: Option<Document>,

    /// Specifies how strictly the database should apply the validation rules to existing documents
    /// during an update.
    #[builder(default)]
    validation_level: Option<ValidationLevel>,

    /// Specifies whether the database should return an error or simply raise a warning if inserted
    /// documents do not pass the validation.
    #[builder(default)]
    validation_action: Option<ValidationAction>,

    /// The name of the source collection or view to base this view on. If specified, this will
    /// cause a view to be created rather than a collection.
    #[builder(default)]
    view_on: Option<String>,

    /// An array that consists of the aggregation pipeline stages to run against `view_on` to
    /// determine the contents of the view.
    #[builder(default)]
    pipeline: Option<Vec<Document>>,

    /// The default collation for the collection or view.
    #[builder(default)]
    collation: Option<Document>,
}

/// Specifies how strictly the database should apply validation rules to existing documents during
/// an update.
#[derive(Debug)]
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
#[derive(Debug)]
pub enum ValidationAction {
    Error,
    Warn,
}
