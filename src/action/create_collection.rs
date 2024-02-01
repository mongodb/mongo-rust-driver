use bson::Document;

use crate::{options::CreateCollectionOptions, ClientSession, Database};

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
        capped: bool,
        size: u64,
        max: u64,
        storage_engine: Document,
        validator: Document,
        validation_level: crate::db::options::ValidationLevel,
        validation_action: crate::db::options::ValidationAction,
        view_on: String,
        pipeline: Vec<Document>,
        collation: crate::collation::Collation,
        write_concern: crate::options::WriteConcern,
        index_option_defaults: crate::db::options::IndexOptionDefaults,
        timeseries: crate::db::options::TimeseriesOptions,
        expire_after_seconds: std::time::Duration,
        change_stream_pre_and_post_images: crate::db::options::ChangeStreamPreAndPostImages,
        clustered_index: crate::db::options::ClusteredIndex,
        comment: bson::Bson,
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
