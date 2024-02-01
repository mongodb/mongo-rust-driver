use bson::Document;

use crate::{operation::create::CreateCollectionOptions, ClientSession, Database};

use crate::action::option_setters;

impl Database {
    /// Creates a new collection in the database with the given `name`.
    ///
    /// Note that MongoDB creates collections implicitly when data is inserted, so this method is
    /// not needed if no special options are required.
    ///
    /// `await` will return `Result<()>`.
    pub fn create_collection(&self, name: impl AsRef<str>) -> CreateCollection {
        CreateCollection {
            db: self,
            name: name.as_ref().to_owned(),
            options: None,
            session: None,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl crate::sync::Database {
    /// Creates a new collection in the database with the given `name`.
    ///
    /// Note that MongoDB creates collections implicitly when data is inserted, so this method is
    /// not needed if no special options are required.
    ///
    /// [`run`](CreateCollection::run) will return `Result<()>`.
    pub fn create_collection(&self, name: impl AsRef<str>) -> CreateCollection {
        self.async_database.create_collection(name)
    }
}

/// Creates a new collection.  Create by calling [`Database::create_collection`] and execute with
/// `await` (or [`run`](CreateCollection::run) if using the sync client).
pub struct CreateCollection<'a> {
    pub(crate) db: &'a Database,
    pub(crate) name: String,
    pub(crate) options: Option<CreateCollectionOptions>,
    pub(crate) session: Option<&'a mut ClientSession>,
}

impl<'a> CreateCollection<'a> {
    option_setters!(options: CreateCollectionOptions;
        /// Whether the collection should be capped. If true, `size` must also be set.
        capped: bool,

        /// The maximum size (in bytes) for a capped collection. This option is ignored if `capped` is
        /// not set to true.
        size: u64,

        /// The maximum number of documents in a capped collection. The `size` limit takes precedence
        /// over this option. If a capped collection reaches the size limit before it reaches the
        /// maximum number of documents, MongoDB removes old documents.
        max: u64,

        /// The storage engine that the collection should use. The value should take the following
        /// form:
        ///
        /// `{ <storage-engine-name>: <options> }`
        storage_engine: Document,

        /// Specifies a validator to restrict the schema of documents which can exist in the
        /// collection. Expressions can be specified using any query operators except `$near`,
        /// `$nearSphere`, `$text`, and `$where`.
        validator: Document,

        /// Specifies how strictly the database should apply the validation rules to existing documents
        /// during an update.
        validation_level: crate::db::options::ValidationLevel,

        /// Specifies whether the database should return an error or simply raise a warning if inserted
        /// documents do not pass the validation.
        validation_action: crate::db::options::ValidationAction,

        /// The name of the source collection or view to base this view on. If specified, this will
        /// cause a view to be created rather than a collection.
        view_on: String,

        /// An array that consists of the aggregation pipeline stages to run against `view_on` to
        /// determine the contents of the view.
        pipeline: Vec<Document>,

        /// The default collation for the collection or view.
        collation: crate::collation::Collation,

        /// The write concern for the operation.
        write_concern: crate::options::WriteConcern,

        /// The default configuration for indexes created on this collection, including the _id index.
        index_option_defaults: crate::db::options::IndexOptionDefaults,

        /// An object containing options for creating time series collections. See the [`create`
        /// command documentation](https://www.mongodb.com/docs/manual/reference/command/create/) for
        /// supported options, and the [Time Series Collections documentation](
        /// https://www.mongodb.com/docs/manual/core/timeseries-collections/) for more information.
        ///
        /// This feature is only available on server versions 5.0 and above.
        timeseries: crate::db::options::TimeseriesOptions,

        /// Used to automatically delete documents in time series collections. See the [`create`
        /// command documentation](https://www.mongodb.com/docs/manual/reference/command/create/) for more
        /// information.
        expire_after_seconds: std::time::Duration,

        /// Options for supporting change stream pre- and post-images.
        change_stream_pre_and_post_images: crate::db::options::ChangeStreamPreAndPostImages,

        /// Options for clustered collections.
        clustered_index: crate::db::options::ClusteredIndex,

        /// Tags the query with an arbitrary [`Bson`] value to help trace the operation through the
        /// database profiler, currentOp and logs.
        ///
        /// This option is only available on server versions 4.4+.
        comment: bson::Bson,

        /// Map of encrypted fields for the created collection.
        #[cfg(feature = "in-use-encryption-unstable")]
        encrypted_fields: Document,
    );

    /// Runs the operation using the provided session.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

// Action impl in src/db/action/create_collection.rs
