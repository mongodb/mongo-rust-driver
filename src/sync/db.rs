use super::{Collection, Cursor};
use crate::{
    error::Result,
    options::{
        AggregateOptions,
        CollectionOptions,
        CreateCollectionOptions,
        DropDatabaseOptions,
        ListCollectionsOptions,
        ReadConcern,
        SelectionCriteria,
        WriteConcern,
    },
    Database as AsyncDatabase,
    RUNTIME,
};
use bson::Document;

pub struct Database {
    async_database: AsyncDatabase,
}

impl Database {
    pub(crate) fn new(async_database: AsyncDatabase) -> Self {
        Self { async_database }
    }

    /// Gets the name of the `Database`.
    pub fn name(&self) -> &str {
        &self.async_database.name()
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

    /// Gets a handle to a collection specified by `name` of the database. The `Collection` options
    /// (e.g. read preference and write concern) will default to those of the `Database`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn collection(&self, name: &str) -> Collection {
        Collection::new(self.async_database.collection(name))
    }

    /// Gets a handle to a collection specified by `name` in the cluster the `Client` is connected
    /// to. Operations done with this `Collection` will use the options specified by `options` by
    /// default and will otherwise default to those of the `Database`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn collection_with_options(&self, name: &str, options: CollectionOptions) -> Collection {
        Collection::new(self.async_database.collection_with_options(name, options))
    }

    /// Drops the database, deleting all data, collections, users, and indexes stored in it.
    pub fn drop(&self, options: impl Into<Option<DropDatabaseOptions>>) -> Result<()> {
        RUNTIME.block_on(self.async_database.drop(options.into()))
    }

    /// Gets information about each of the collections in the database. The cursor will yield a
    /// document pertaining to each collection in the database.
    pub fn list_collections(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<ListCollectionsOptions>>,
    ) -> Result<Cursor> {
        RUNTIME
            .block_on(
                self.async_database
                    .list_collections(filter.into(), options.into()),
            )
            .map(Cursor::new)
    }

    /// Gets the names of the collections in the database.
    pub fn list_collection_names(
        &self,
        filter: impl Into<Option<Document>>,
    ) -> Result<Vec<String>> {
        RUNTIME.block_on(self.async_database.list_collection_names(filter.into()))
    }

    /// Creates a new collection in the database with the given `name` and `options`.
    ///
    /// Note that MongoDB creates collections implicitly when data is inserted, so this method is
    /// not needed if no special options are required.
    pub fn create_collection(
        &self,
        name: &str,
        options: impl Into<Option<CreateCollectionOptions>>,
    ) -> Result<()> {
        RUNTIME.block_on(self.async_database.create_collection(name, options.into()))
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
        RUNTIME.block_on(
            self.async_database
                .run_command(command, selection_criteria.into()),
        )
    }

    /// Runs an aggregation operation.
    ///
    /// See the documentation [here](https://docs.mongodb.com/manual/aggregation/) for more
    /// information on aggregations.
    pub fn aggregate(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<AggregateOptions>>,
    ) -> Result<Cursor> {
        let pipeline: Vec<Document> = pipeline.into_iter().collect();
        RUNTIME
            .block_on(self.async_database.aggregate(pipeline, options.into()))
            .map(Cursor::new)
    }
}
