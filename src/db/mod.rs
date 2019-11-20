pub mod options;

use std::sync::Arc;

use bson::{Bson, Document};

use crate::{
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
    error::{ErrorKind, Result},
    operation::{Create, DropDatabase, ListCollections, RunCommand},
    options::{
        CollectionOptions, CreateCollectionOptions, DatabaseOptions, DropDatabaseOptions,
        ListCollectionsOptions,
    },
    selection_criteria::SelectionCriteria,
    Client, Collection, Namespace,
};

/// `Database` is the client-side abstraction of a MongoDB database. It can be used to perform
/// database-level operations or to obtain handles to specific collections within the database. A
/// `Database` can only be obtained through a `Client` by calling either `Client::database` or
/// `Client::database_with_options`.
///
/// `Database` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads. For example:
///
/// ```rust
/// 
/// # use mongodb::{Client, error::Result};
///
/// # fn start_workers() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com")?;
/// let db = client.database("items");
///
/// for i in 0..5 {
///     let db_ref = db.clone();
///
///     std::thread::spawn(move || {
///         let collection = db_ref.collection(&format!("coll{}", i));
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
#[derive(Clone, Debug)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

#[derive(Debug)]
struct DatabaseInner {
    client: Client,
    name: String,
    selection_criteria: Option<SelectionCriteria>,
    read_concern: Option<ReadConcern>,
    write_concern: Option<WriteConcern>,
}

impl Database {
    pub(crate) fn new(client: Client, name: &str, options: Option<DatabaseOptions>) -> Self {
        let options = options.unwrap_or_default();
        let selection_criteria = options
            .selection_criteria
            .or_else(|| client.selection_criteria().cloned());

        let read_concern = options
            .read_concern
            .or_else(|| client.read_concern().cloned());

        let write_concern = options
            .write_concern
            .or_else(|| client.write_concern().cloned());

        Self {
            inner: Arc::new(DatabaseInner {
                client,
                name: name.to_string(),
                selection_criteria,
                read_concern,
                write_concern,
            }),
        }
    }

    /// Get the `Client` that this collection descended from.
    pub(crate) fn client(&self) -> &Client {
        &self.inner.client
    }

    /// Gets the name of the `Database`.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Gets the read preference of the `Database`.
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.inner.selection_criteria.as_ref()
    }

    /// Gets the read concern of the `Database`.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.read_concern.as_ref()
    }

    /// Gets the write concern of the `Database`.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.write_concern.as_ref()
    }

    /// Gets a handle to a collection specified by `name` of the database. The `Collection` options
    /// (e.g. read preference and write concern) will default to those of the `Database`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn collection(&self, name: &str) -> Collection {
        Collection::new(self.clone(), name, None)
    }

    /// Gets a handle to a collection specified by `name` in the cluster the `Client` is connected
    /// to. Operations done with this `Collection` will use the options specified by `options` by
    /// default and will otherwise default to those of the `Database`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn collection_with_options(&self, name: &str, options: CollectionOptions) -> Collection {
        Collection::new(self.clone(), name, Some(options))
    }

    /// Drops the database, deleting all data, collections, users, and indexes stored in in.
    pub fn drop(&self, mut options: Option<DropDatabaseOptions>) -> Result<()> {
        resolve_options!(self, options, [write_concern]);

        let drop_database = DropDatabase::new(self.name().to_string(), options);
        self.client().execute_operation(&drop_database, None)
    }

    /// Gets information about each of the collections in the database. The cursor will yield a
    /// document pertaining to each collection in the database.
    pub fn list_collections(
        &self,
        filter: Option<Document>,
        options: Option<ListCollectionsOptions>,
    ) -> Result<Cursor> {
        let list_collections =
            ListCollections::new(self.name().to_string(), filter, false, options);
        self.client()
            .execute_operation(&list_collections, None)
            .map(|spec| Cursor::new(self.client().clone(), spec))
    }

    /// Gets the names of the collections in the database.
    pub fn list_collection_names(&self, filter: Option<Document>) -> Result<Vec<String>> {
        let list_collections = ListCollections::new(self.name().to_string(), filter, true, None);
        let cursor = self
            .client()
            .execute_operation(&list_collections, None)
            .map(|spec| Cursor::new(self.client().clone(), spec))?;

        cursor
            .map(|doc| {
                let name = doc?
                    .get("name")
                    .and_then(Bson::as_str)
                    .ok_or_else(|| ErrorKind::ResponseError {
                        message: "Expected name field in server response, but there was none."
                            .to_string(),
                    })?
                    .to_string();
                Ok(name)
            })
            .collect()
    }

    /// Creates a new collection in the database with the given `name` and `options`.
    ///
    /// Note that MongoDB creates collections implicitly when data is inserted, so this method is
    /// not needed if no special options are required.
    pub fn create_collection(
        &self,
        name: &str,
        options: Option<CreateCollectionOptions>,
    ) -> Result<()> {
        let create = Create::new(
            Namespace {
                db: self.name().to_string(),
                coll: name.to_string(),
            },
            self.write_concern().cloned(),
            options,
        );
        self.client().execute_operation(&create, None)
    }

    /// Runs a database-level command.
    ///
    /// Note that no inspection is done on `doc`, so the command will not use the database's default
    /// read concern or write concern. If specific read concern or write concern is desired, it must
    /// be specified manually.
    pub fn run_command(
        &self,
        command: Document,
        selection_criteria: Option<SelectionCriteria>,
    ) -> Result<Document> {
        let operation = RunCommand::new(self.name().into(), command, selection_criteria);
        self.client().execute_operation(&operation, None)
    }
}
