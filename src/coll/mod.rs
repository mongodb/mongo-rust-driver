pub mod options;

use std::sync::Arc;

use bson::{Bson, Document};

use self::options::*;
use crate::{
    concern::{ReadConcern, WriteConcern},
    error::Result,
    read_preference::ReadPreference,
    results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
    Client, Cursor, Database,
};

/// `Collection` is the client-side abstraction of a MongoDB Collection. It can be used to
/// perform collection-level operations such as CRUD operations. A `Collection` can be obtained
/// through a `Database` by calling either `Database::collection` or
/// `Database::collection_with_options`.
///
/// `Collection` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads. For example:
///
/// ```rust
/// 
/// # #[macro_use]
/// #
/// # use bson::{bson, doc};
/// # use mongodb::{Client, error::Result};
/// #
/// # fn start_workers() -> Result<()> {
/// # let client = Client::with_uri("mongodb://example.com")?;
/// let coll = client.database("items").collection("in_stock");
///
/// for i in 0..5 {
///     let coll_ref = coll.clone();
///
///     std::thread::spawn(move || {
///         // Perform operations with `coll_ref`. For example:
///         coll_ref.insert_one(doc! { "x": i }, None);
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
pub struct Collection {
    inner: Arc<CollectionInner>,
}

#[derive(Debug)]
struct CollectionInner {
    client: Client,
    db: Database,
    name: String,
    read_preference: Option<ReadPreference>,
    read_concern: Option<ReadConcern>,
    write_concern: Option<WriteConcern>,
}

impl Collection {
    /// Gets the name of the `Collection`.
    pub fn name(&self) -> &str {
        unimplemented!()
    }

    /// Gets the namespace of the `Collection`.
    ///
    /// The namespace of a MongoDB collection is the concatenation of the name of the database
    /// containing it, the '.' character, and the name of the collection itself. For example, if a
    /// collection named "bar" is created in a database named "foo", the namespace of the collection
    /// is "foo.bar".
    pub fn namespace(&self) -> String {
        unimplemented!()
    }

    /// Gets the read preference of the `Collection`.
    pub fn read_preference(&self) -> Option<&ReadPreference> {
        unimplemented!()
    }

    /// Gets the read concern of the `Collection`.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        unimplemented!()
    }

    /// Gets the write concern of the `Collection`.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        unimplemented!()
    }

    /// Drops the collection, deleting all data, users, and indexes stored in in.
    pub fn drop(&self) -> Result<()> {
        unimplemented!()
    }

    /// Runs an aggregation operation.
    ///
    /// See the documentation [here](https://docs.mongodb.com/manual/aggregation/) for more
    /// information on aggregations.
    pub fn aggregate(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: Option<AggregateOptions>,
    ) -> Result<Cursor> {
        unimplemented!()
    }

    /// Gets the number of documents matching `filter`.
    pub fn count_documents(
        &self,
        filter: Option<Document>,
        options: Option<CountOptions>,
    ) -> Result<i64> {
        unimplemented!()
    }

    /// Estimates the number of documents in the collection using collection metadata.
    pub fn estimated_document_count(
        &self,
        options: Option<EstimatedDocumentCountOptions>,
    ) -> Result<i64> {
        unimplemented!()
    }

    /// Deletes all documents stored in the collection matching `query`.
    pub fn delete_many(
        &self,
        query: Document,
        options: Option<DeleteOptions>,
    ) -> Result<DeleteResult> {
        unimplemented!()
    }

    /// Deletes up to one document found matching `query`.
    pub fn delete_one(
        &self,
        query: Document,
        options: Option<DeleteOptions>,
    ) -> Result<DeleteResult> {
        unimplemented!()
    }

    /// Finds the distinct values of the field specified by `field_name` across the collection.
    pub fn distinct(
        &self,
        field_name: &str,
        filter: Option<Document>,
        options: Option<DistinctOptions>,
    ) -> Result<Vec<Bson>> {
        unimplemented!()
    }

    /// Finds the documents in the collection matching `filter`.
    pub fn find(&self, filter: Option<Document>, options: Option<FindOptions>) -> Result<Cursor> {
        unimplemented!()
    }

    /// Atomically finds up to one document in the collection matching `filter` and deletes it.
    pub fn find_one_and_delete(
        &self,
        filter: Document,
        options: Option<FindOneAndDeleteOptions>,
    ) -> Result<Option<Document>> {
        unimplemented!()
    }

    /// Atomically finds up to one document in the collection matching `filter` and replaces it with
    /// `replacement`.
    pub fn find_one_and_replace(
        &self,
        filter: Document,
        replacement: Document,
        options: Option<FindOneAndReplaceOptions>,
    ) -> Result<Option<Document>> {
        unimplemented!()
    }

    /// Atomically finds up to one document in the collection matching `filter` and updates it.
    pub fn find_one_and_update(
        &self,
        filter: Document,
        update: Document,
        options: Option<FindOneAndUpdateOptions>,
    ) -> Result<Option<Document>> {
        unimplemented!()
    }

    /// Finds up to one in the collection matching `filter`. This is semantically equivalent to
    /// calling `Collection::find` with the `limit` option set to `1`.
    pub fn find_one(
        &self,
        filter: Option<Document>,
        options: Option<FindOneOptions>,
    ) -> Result<Option<Document>> {
        unimplemented!()
    }

    /// Inserts the documents in `docs` into the collection.
    pub fn insert_many(
        &self,
        docs: impl IntoIterator<Item = Document>,
        options: Option<InsertManyOptions>,
    ) -> Result<InsertManyResult> {
        unimplemented!()
    }

    /// Inserts `doc` into the collection.
    pub fn insert_one(
        &self,
        doc: Document,
        options: Option<InsertOneOptions>,
    ) -> Result<InsertOneResult> {
        unimplemented!()
    }

    /// Replaces up to one document matching `query` in the collection with `replacement`.
    pub fn replace_one(
        &self,
        query: Document,
        replacement: Document,
        options: Option<ReplaceOptions>,
    ) -> Result<UpdateResult> {
        unimplemented!()
    }

    /// Updates all documents matching `query` in the collection.
    pub fn update_many(
        &self,
        query: Document,
        update: Document,
        options: Option<UpdateOptions>,
    ) -> Result<UpdateResult> {
        unimplemented!()
    }

    /// Updates up to one document matching `query` in the collection.
    pub fn update_one(
        &self,
        query: Document,
        update: Document,
        options: Option<UpdateOptions>,
    ) -> Result<UpdateResult> {
        unimplemented!()
    }

    /// Creates the indexes specified by `models`.
    pub fn create_indexes(
        &self,
        models: impl IntoIterator<Item = IndexModel>,
    ) -> Result<Vec<String>> {
        unimplemented!()
    }

    /// Drops the index specified by `name`.
    pub fn drop_index(&self, name: &str) -> Result<Document> {
        unimplemented!()
    }

    /// Drops the index with the given `keys`.
    pub fn drop_index_with_keys(&self, keys: Document) -> Result<Document> {
        unimplemented!()
    }

    /// Drops all indexes in the collection.
    pub fn drop_indexes(&self) -> Result<Document> {
        unimplemented!()
    }
}
