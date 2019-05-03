pub mod options;

use std::sync::Arc;

use bson::Document;

use crate::{
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
    error::Result,
    options::CollectionOptions,
    read_preference::ReadPreference,
    Client, Collection,
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
/// # let client = Client::with_uri("mongodb://example.com")?;
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
    read_preference: Option<ReadPreference>,
    read_concern: Option<ReadConcern>,
    write_concern: Option<WriteConcern>,
}

impl Database {
    /// Gets a reference to the `Client` the `Database` was created from.
    ///
    /// `Client` uses a `std::sync::Arc` internally, so while the `Client` returned is a different
    /// struct, it shares the same state as the one the `Database` was originally created from.
    /// See the type-level documentation for `Client` for more information.
    pub fn client(&self) -> Client {
        unimplemented!()
    }

    /// Gets the name of the `Database`.
    pub fn name(&self) -> &str {
        unimplemented!()
    }

    /// Gets the read preference of the `Database`.
    pub fn read_preference(&self) -> Option<&ReadPreference> {
        unimplemented!()
    }

    /// Gets the read concern of the `Database`.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        unimplemented!()
    }

    /// Gets the write concern of the `Database`.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        unimplemented!()
    }

    /// Gets a handle to a collection specified by `name` of the database.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn collection(&self, name: &str) -> Collection {
        unimplemented!()
    }

    /// Gets a handle to a collection specified by `name` in the cluster the `Client` is connected
    /// to. Operations done with this `Collection` will use the options specified by `options` by
    /// default.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn collection_with_options(&self, name: &str, options: CollectionOptions) -> Collection {
        unimplemented!()
    }

    /// Drops the database, deleting all data, collections, users, and indexes stored in in.
    pub fn drop(&self) -> Result<()> {
        unimplemented!()
    }

    /// Gets information about each of the collections in the database. The cursor will yield a
    /// document pertaining to each collection in the database.
    pub fn list_collections(&self, filter: Option<Document>) -> Result<Cursor> {
        unimplemented!()
    }

    /// Gets the names of the collections in the database.
    pub fn list_collection_names(&self, filter: Option<Document>) -> Result<Vec<String>> {
        unimplemented!()
    }

    /// Creates a new collection in the database with the given `name` and `options`.
    ///
    /// Note that MongoDB creates collections implicitly when data is inserted, so this method is
    /// not needed if no special options are required.
    pub fn create_collection(&self, name: &str, options: Option<Document>) -> Result<()> {
        unimplemented!()
    }

    /// Runs a database-level command.
    ///
    /// Note that no inspection is done on `doc`, so the command will not use the database's default
    /// read concern or write concern. If specific read concern or write concern is desired, it must
    /// be specified manually.
    pub fn run_command(
        &self,
        doc: Document,
        read_pref: Option<ReadPreference>,
    ) -> Result<Document> {
        unimplemented!()
    }
}
