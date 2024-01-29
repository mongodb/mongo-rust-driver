use std::fmt::Debug;

use super::{gridfs::GridFsBucket, ClientSession, Collection, Cursor, SessionCursor};
use crate::{
    bson::Document,
    error::Result,
    options::{
        AggregateOptions,
        CollectionOptions,
        CreateCollectionOptions,
        GridFsBucketOptions,
        ReadConcern,
        SelectionCriteria,
        WriteConcern,
    },
    runtime,
    Database as AsyncDatabase,
};

/// `Database` is the client-side abstraction of a MongoDB database. It can be used to perform
/// database-level operations or to obtain handles to specific collections within the database. A
/// `Database` can only be obtained through a [`Client`](struct.Client.html) by calling either
/// [`Client::database`](struct.Client.html#method.database) or
/// [`Client::database_with_options`](struct.Client.html#method.database_with_options).
///
/// `Database` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads. For example:
///
/// ```rust
/// # use mongodb::{bson::Document, sync::Client, error::Result};
///
/// # fn start_workers() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com")?;
/// let db = client.database("items");
///
/// for i in 0..5 {
///     let db_ref = db.clone();
///
///     std::thread::spawn(move || {
///         let collection = db_ref.collection::<Document>(&format!("coll{}", i));
///
///         // Do something with the collection
///     });
/// }
/// #
/// # // Technically we should join the threads here, but for the purpose of the example, we'll just
/// # // sleep for a bit.
/// # std::thread::sleep(std::time::Duration::from_secs(3));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Database {
    pub(crate) async_database: AsyncDatabase,
}

impl Database {
    pub(crate) fn new(async_database: AsyncDatabase) -> Self {
        Self { async_database }
    }

    /// Gets the name of the `Database`.
    pub fn name(&self) -> &str {
        self.async_database.name()
    }

    /// Gets the read preference of the `Database`.
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.async_database.selection_criteria()
    }

    /// Gets the read concern of the `Database`.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.async_database.read_concern()
    }

    /// Gets the write concern of the `Database`.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.async_database.write_concern()
    }

    /// Gets a handle to a collection with type `T` specified by `name` of the database. The
    /// `Collection` options (e.g. read preference and write concern) will default to those of the
    /// `Database`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn collection<T>(&self, name: &str) -> Collection<T> {
        Collection::new(self.async_database.collection(name))
    }

    /// Gets a handle to a collection with type `T` specified by `name` in the cluster the `Client`
    /// is connected to. Operations done with this `Collection` will use the options specified by
    /// `options` by default and will otherwise default to those of the `Database`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn collection_with_options<T>(
        &self,
        name: &str,
        options: CollectionOptions,
    ) -> Collection<T> {
        Collection::new(self.async_database.collection_with_options(name, options))
    }

    /// Creates a new collection in the database with the given `name` and `options`.
    ///
    /// Note that MongoDB creates collections implicitly when data is inserted, so this method is
    /// not needed if no special options are required.
    pub fn create_collection(
        &self,
        name: impl AsRef<str>,
        options: impl Into<Option<CreateCollectionOptions>>,
    ) -> Result<()> {
        runtime::block_on(
            self.async_database
                .create_collection(name.as_ref(), options.into()),
        )
    }

    /// Creates a new collection in the database with the given `name` and `options` using the
    /// provided `ClientSession`.
    ///
    /// Note that MongoDB creates collections implicitly when data is inserted, so this method is
    /// not needed if no special options are required.
    pub fn create_collection_with_session(
        &self,
        name: impl AsRef<str>,
        options: impl Into<Option<CreateCollectionOptions>>,
        session: &mut ClientSession,
    ) -> Result<()> {
        runtime::block_on(self.async_database.create_collection_with_session(
            name.as_ref(),
            options.into(),
            &mut session.async_client_session,
        ))
    }

    /// Runs a database-level command.
    ///
    /// Note that no inspection is done on `doc`, so the command will not use the database's default
    /// read concern or write concern. If specific read concern or write concern is desired, it must
    /// be specified manually.
    pub fn run_command(
        &self,
        command: Document,
        selection_criteria: impl Into<Option<SelectionCriteria>>,
    ) -> Result<Document> {
        runtime::block_on(
            self.async_database
                .run_command(command, selection_criteria.into()),
        )
    }

    /// Runs a database-level command using the provided `ClientSession`.
    ///
    /// Note that no inspection is done on `doc`, so the command will not use the database's default
    /// read concern or write concern. If specific read concern or write concern is desired, it must
    /// be specified manually.
    pub fn run_command_with_session(
        &self,
        command: Document,
        selection_criteria: impl Into<Option<SelectionCriteria>>,
        session: &mut ClientSession,
    ) -> Result<Document> {
        runtime::block_on(self.async_database.run_command_with_session(
            command,
            selection_criteria.into(),
            &mut session.async_client_session,
        ))
    }

    /// Runs an aggregation operation.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/aggregation/) for more
    /// information on aggregations.
    pub fn aggregate(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<AggregateOptions>>,
    ) -> Result<Cursor<Document>> {
        let pipeline: Vec<Document> = pipeline.into_iter().collect();
        runtime::block_on(self.async_database.aggregate(pipeline, options.into())).map(Cursor::new)
    }

    /// Runs an aggregation operation using the provided `ClientSession`.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/aggregation/) for more
    /// information on aggregations.
    pub fn aggregate_with_session(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<AggregateOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<Document>> {
        let pipeline: Vec<Document> = pipeline.into_iter().collect();
        runtime::block_on(self.async_database.aggregate_with_session(
            pipeline,
            options.into(),
            &mut session.async_client_session,
        ))
        .map(SessionCursor::new)
    }

    /// Creates a new [`GridFsBucket`] in the database with the given options.
    pub fn gridfs_bucket(&self, options: impl Into<Option<GridFsBucketOptions>>) -> GridFsBucket {
        GridFsBucket::new(self.async_database.gridfs_bucket(options))
    }
}
