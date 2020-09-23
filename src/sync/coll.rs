use std::marker::{Send, Sync};

use serde::Serialize;

use super::Cursor;
use crate::{
    bson::{Bson, Document},
    error::Result,
    options::{
        AggregateOptions,
        CountOptions,
        DeleteOptions,
        DistinctOptions,
        DropCollectionOptions,
        EstimatedDocumentCountOptions,
        FindOneAndDeleteOptions,
        FindOneAndReplaceOptions,
        FindOneAndUpdateOptions,
        FindOneOptions,
        FindOptions,
        InsertManyOptions,
        InsertOneOptions,
        ReadConcern,
        ReplaceOptions,
        SelectionCriteria,
        UpdateModifications,
        UpdateOptions,
        WriteConcern,
    },
    results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
    Collection as AsyncCollection,
    Namespace,
    RUNTIME,
};

/// `Collection` is the client-side abstraction of a MongoDB Collection. It can be used to
/// perform collection-level operations such as CRUD operations. A `Collection` can be obtained
/// through a [`Database`](struct.Database.html) by calling either
/// [`Database::collection`](struct.Database.html#method.collection) or
/// [`Database::collection_with_options`](struct.Database.html#method.collection_with_options).
///
/// `Collection` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads. For example:
///
/// ```rust
/// # use mongodb::{
/// #     bson::doc,
/// #     error::Result,
/// #     sync::Client,
/// # };
/// #
/// # fn start_workers() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com")?;
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

#[derive(Clone, Debug)]
pub struct Collection<T = Document> where T: Serialize + Send + Sync {
    async_collection: AsyncCollection<T>,
}

impl<T> Collection<T> where T: Serialize + Send + Sync {
    pub(crate) fn new(async_collection: AsyncCollection<T>) -> Self {
        Self { async_collection }
    }

    /// Gets the name of the `Collection`.
    pub fn name(&self) -> &str {
        self.async_collection.name()
    }

    /// Gets the namespace of the `Collection`.
    ///
    /// The namespace of a MongoDB collection is the concatenation of the name of the database
    /// containing it, the '.' character, and the name of the collection itself. For example, if a
    /// collection named "bar" is created in a database named "foo", the namespace of the collection
    /// is "foo.bar".
    pub fn namespace(&self) -> Namespace {
        self.async_collection.namespace()
    }

    /// Gets the selection criteria of the `Collection`.
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.async_collection.selection_criteria()
    }

    /// Gets the read concern of the `Collection`.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.async_collection.read_concern()
    }

    /// Gets the write concern of the `Collection`.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.async_collection.write_concern()
    }

    /// Drops the collection, deleting all data, users, and indexes stored in it.
    pub fn drop(&self, options: impl Into<Option<DropCollectionOptions>>) -> Result<()> {
        RUNTIME.block_on(self.async_collection.drop(options.into()))
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
            .block_on(self.async_collection.aggregate(pipeline, options.into()))
            .map(Cursor::new)
    }

    /// Estimates the number of documents in the collection using collection metadata.
    pub fn estimated_document_count(
        &self,
        options: impl Into<Option<EstimatedDocumentCountOptions>>,
    ) -> Result<i64> {
        RUNTIME.block_on(
            self.async_collection
                .estimated_document_count(options.into()),
        )
    }

    /// Gets the number of documents matching `filter`.
    ///
    /// Note that using [`Collection::estimated_document_count`](#method.estimated_document_count)
    /// is recommended instead of this method is most cases.
    pub fn count_documents(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<CountOptions>>,
    ) -> Result<i64> {
        RUNTIME.block_on(
            self.async_collection
                .count_documents(filter.into(), options.into()),
        )
    }

    /// Deletes all documents stored in the collection matching `query`.
    pub fn delete_many(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
    ) -> Result<DeleteResult> {
        RUNTIME.block_on(self.async_collection.delete_many(query, options.into()))
    }

    /// Deletes up to one document found matching `query`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn delete_one(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
    ) -> Result<DeleteResult> {
        RUNTIME.block_on(self.async_collection.delete_one(query, options.into()))
    }

    /// Finds the distinct values of the field specified by `field_name` across the collection.
    pub fn distinct(
        &self,
        field_name: &str,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<DistinctOptions>>,
    ) -> Result<Vec<Bson>> {
        RUNTIME.block_on(
            self.async_collection
                .distinct(field_name, filter.into(), options.into()),
        )
    }

    /// Finds the documents in the collection matching `filter`.
    pub fn find(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOptions>>,
    ) -> Result<Cursor> {
        RUNTIME
            .block_on(self.async_collection.find(filter.into(), options.into()))
            .map(Cursor::new)
    }

    /// Finds a single document in the collection matching `filter`.
    pub fn find_one(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOneOptions>>,
    ) -> Result<Option<Document>> {
        RUNTIME.block_on(
            self.async_collection
                .find_one(filter.into(), options.into()),
        )
    }

    /// Atomically finds up to one document in the collection matching `filter` and deletes it.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_delete(
        &self,
        filter: Document,
        options: impl Into<Option<FindOneAndDeleteOptions>>,
    ) -> Result<Option<Document>> {
        RUNTIME.block_on(
            self.async_collection
                .find_one_and_delete(filter, options.into()),
        )
    }

    /// Atomically finds up to one document in the collection matching `filter` and replaces it with
    /// `replacement`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_replace(
        &self,
        filter: Document,
        replacement: T,
        options: impl Into<Option<FindOneAndReplaceOptions>>,
    ) -> Result<Option<Document>> {
        RUNTIME.block_on(self.async_collection.find_one_and_replace(
            filter,
            replacement,
            options.into(),
        ))
    }

    /// Atomically finds up to one document in the collection matching `filter` and updates it.
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_update(
        &self,
        filter: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<FindOneAndUpdateOptions>>,
    ) -> Result<Option<Document>> {
        RUNTIME.block_on(self.async_collection.find_one_and_update(
            filter,
            update.into(),
            options.into(),
        ))
    }

    /// Inserts the documents in `docs` into the collection.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn insert_many(
        &self,
        docs: impl IntoIterator<Item = T>,
        options: impl Into<Option<InsertManyOptions>>,
    ) -> Result<InsertManyResult> {
        let docs: Vec<T> = docs.into_iter().collect();
        RUNTIME.block_on(self.async_collection.insert_many(docs, options.into()))
    }

    /// Inserts `doc` into the collection.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn insert_one(
        &self,
        doc: T,
        options: impl Into<Option<InsertOneOptions>>,
    ) -> Result<InsertOneResult> {
        RUNTIME.block_on(self.async_collection.insert_one(doc, options.into()))
    }

    /// Replaces up to one document matching `query` in the collection with `replacement`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn replace_one(
        &self,
        query: Document,
        replacement: T,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> Result<UpdateResult> {
        RUNTIME.block_on(
            self.async_collection
                .replace_one(query, replacement, options.into()),
        )
    }

    /// Updates all documents matching `query` in the collection.
    ///
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://docs.mongodb.com/manual/reference/command/update/#behavior) for more information on specifying updates.
    pub fn update_many(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
    ) -> Result<UpdateResult> {
        RUNTIME.block_on(
            self.async_collection
                .update_many(query, update.into(), options.into()),
        )
    }

    /// Updates up to one document matching `query` in the collection.
    ///
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://docs.mongodb.com/manual/reference/command/update/#behavior) for more information on specifying updates.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn update_one(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
    ) -> Result<UpdateResult> {
        RUNTIME.block_on(
            self.async_collection
                .update_one(query, update.into(), options.into()),
        )
    }
}
