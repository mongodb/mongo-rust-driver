pub mod options;

use std::sync::Arc;

use futures::stream::TryStreamExt;

use crate::{
    bson::{Bson, Document},
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
    error::{ErrorKind, Result},
    operation::{Aggregate, Create, DropDatabase, ListCollections, RunCommand},
    options::{
        AggregateOptions,
        CollectionOptions,
        CreateCollectionOptions,
        DatabaseOptions,
        DropDatabaseOptions,
        ListCollectionsOptions,
    },
    selection_criteria::SelectionCriteria,
    Client,
    Collection,
    Namespace,
};

/// `Database` is the client-side abstraction of a MongoDB database. It can be used to perform
/// database-level operations or to obtain handles to specific collections within the database. A
/// `Database` can only be obtained through a [`Client`](struct.Client.html) by calling either
/// [`Client::database`](struct.Client.html#method.database) or
/// [`Client::database_with_options`](struct.Client.html#method.database_with_options).
///
/// `Database` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads or async tasks. For example:
///
/// ```rust
/// 
/// # #[cfg(not(feature = "sync"))]
/// # use mongodb::{Client, error::Result};
/// # #[cfg(feature = "async-std-runtime")]
/// # use async_std::task;
/// # #[cfg(feature = "tokio-runtime")]
/// # use tokio::task;
/// #
/// #
/// # #[cfg(not(feature = "sync"))]
/// # async fn start_workers() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// let db = client.database("items");
///
/// for i in 0..5 {
///     let db_ref = db.clone();
///
///     task::spawn(async move {
///         let collection = db_ref.collection(&format!("coll{}", i));
///
///         // Do something with the collection
///     });
/// }
/// #
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

    /// Drops the database, deleting all data, collections, and indexes stored in it.
    pub async fn drop(&self, options: impl Into<Option<DropDatabaseOptions>>) -> Result<()> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let drop_database = DropDatabase::new(self.name().to_string(), options);
        self.client().execute_operation(drop_database).await
    }

    /// Gets information about each of the collections in the database. The cursor will yield a
    /// document pertaining to each collection in the database.
    pub async fn list_collections(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<ListCollectionsOptions>>,
    ) -> Result<Cursor> {
        let list_collections = ListCollections::new(
            self.name().to_string(),
            filter.into(),
            false,
            options.into(),
        );
        self.client()
            .execute_cursor_operation(list_collections)
            .await
            .map(|(spec, session)| Cursor::new(self.client().clone(), spec, session))
    }

    /// Gets the names of the collections in the database.
    pub async fn list_collection_names(
        &self,
        filter: impl Into<Option<Document>>,
    ) -> Result<Vec<String>> {
        let list_collections =
            ListCollections::new(self.name().to_string(), filter.into(), true, None);
        let cursor = self
            .client()
            .execute_cursor_operation(list_collections)
            .await
            .map(|(spec, session)| Cursor::new(self.client().clone(), spec, session))?;

        cursor
            .and_then(|doc| match doc.get("name").and_then(Bson::as_str) {
                Some(name) => futures::future::ok(name.into()),
                None => futures::future::err(
                    ErrorKind::ResponseError {
                        message: "Expected name field in server response, but there was none."
                            .to_string(),
                    }
                    .into(),
                ),
            })
            .try_collect()
            .await
    }

    /// Creates a new collection in the database with the given `name` and `options`.
    ///
    /// Note that MongoDB creates collections implicitly when data is inserted, so this method is
    /// not needed if no special options are required.
    pub async fn create_collection(
        &self,
        name: &str,
        options: impl Into<Option<CreateCollectionOptions>>,
    ) -> Result<()> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let create = Create::new(
            Namespace {
                db: self.name().to_string(),
                coll: name.to_string(),
            },
            options,
        );
        self.client().execute_operation(create).await
    }

    /// Runs a database-level command.
    ///
    /// Note that no inspection is done on `doc`, so the command will not use the database's default
    /// read concern or write concern. If specific read concern or write concern is desired, it must
    /// be specified manually.
    pub async fn run_command(
        &self,
        command: Document,
        selection_criteria: impl Into<Option<SelectionCriteria>>,
    ) -> Result<Document> {
        let operation = RunCommand::new(self.name().into(), command, selection_criteria.into())?;
        self.client().execute_operation(operation).await
    }

    /// Runs an aggregation operation.
    ///
    /// See the documentation [here](https://docs.mongodb.com/manual/aggregation/) for more
    /// information on aggregations.
    pub async fn aggregate(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<AggregateOptions>>,
    ) -> Result<Cursor> {
        let mut options = options.into();
        resolve_options!(
            self,
            options,
            [read_concern, write_concern, selection_criteria]
        );

        let aggregate = Aggregate::new(self.name().to_string(), pipeline, options);
        let client = self.client();
        client
            .execute_cursor_operation(aggregate)
            .await
            .map(|(spec, session)| Cursor::new(client.clone(), spec, session))
    }
}
