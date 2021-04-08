use std::{
    fmt::Debug,
    marker::{Send, Sync},
};

use serde::{de::DeserializeOwned, Serialize};

use super::{Cursor, SessionCursor};
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
    ClientSession,
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
pub struct Collection<T>
where
    T: Serialize + DeserializeOwned + Unpin + Debug + Send + Sync,
{
    async_collection: AsyncCollection<T>,
}

impl<T> Collection<T>
where
    T: Serialize + DeserializeOwned + Unpin + Debug + Send + Sync,
{
    pub(crate) fn new(async_collection: AsyncCollection<T>) -> Self {
        Self { async_collection }
    }

    /// Gets a clone of the `Collection` with a different type `U`.
    pub fn clone_with_type<U>(&self) -> Collection<U>
    where
        U: Serialize + DeserializeOwned + Unpin + Debug + Send + Sync,
    {
        Collection::new(self.async_collection.clone_with_type())
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

    /// Drops the collection, deleting all data, users, and indexes stored in it using the provided
    /// `ClientSession`.
    pub fn drop_with_session(
        &self,
        options: impl Into<Option<DropCollectionOptions>>,
        session: &mut ClientSession,
    ) -> Result<()> {
        RUNTIME.block_on(
            self.async_collection
                .drop_with_session(options.into(), session),
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
    ) -> Result<Cursor<Document>> {
        let pipeline: Vec<Document> = pipeline.into_iter().collect();
        RUNTIME
            .block_on(self.async_collection.aggregate(pipeline, options.into()))
            .map(Cursor::new)
    }

    /// Runs an aggregation operation using the provided `ClientSession`.
    ///
    /// See the documentation [here](https://docs.mongodb.com/manual/aggregation/) for more
    /// information on aggregations.
    pub fn aggregate_with_session(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<AggregateOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<Document>> {
        let pipeline: Vec<Document> = pipeline.into_iter().collect();
        RUNTIME
            .block_on(self.async_collection.aggregate_with_session(
                pipeline,
                options.into(),
                session,
            ))
            .map(SessionCursor::new)
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

    /// Gets the number of documents matching `filter` using the provided `ClientSession`.
    ///
    /// Note that using [`Collection::estimated_document_count`](#method.estimated_document_count)
    /// is recommended instead of this method is most cases.
    pub fn count_documents_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<CountOptions>>,
        session: &mut ClientSession,
    ) -> Result<i64> {
        RUNTIME.block_on(self.async_collection.count_documents_with_session(
            filter.into(),
            options.into(),
            session,
        ))
    }

    /// Deletes all documents stored in the collection matching `query`.
    pub fn delete_many(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
    ) -> Result<DeleteResult> {
        RUNTIME.block_on(self.async_collection.delete_many(query, options.into()))
    }

    /// Deletes all documents stored in the collection matching `query` using the provided
    /// `ClientSession`.
    pub fn delete_many_with_session(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
        session: &mut ClientSession,
    ) -> Result<DeleteResult> {
        RUNTIME.block_on(self.async_collection.delete_many_with_session(
            query,
            options.into(),
            session,
        ))
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

    /// Deletes up to one document found matching `query` using the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn delete_one_with_session(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
        session: &mut ClientSession,
    ) -> Result<DeleteResult> {
        RUNTIME.block_on(self.async_collection.delete_one_with_session(
            query,
            options.into(),
            session,
        ))
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

    /// Finds the distinct values of the field specified by `field_name` across the collection using
    /// the provided `ClientSession`.
    pub fn distinct_with_session(
        &self,
        field_name: &str,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<DistinctOptions>>,
        session: &mut ClientSession,
    ) -> Result<Vec<Bson>> {
        RUNTIME.block_on(self.async_collection.distinct_with_session(
            field_name,
            filter.into(),
            options.into(),
            session,
        ))
    }

    /// Finds the documents in the collection matching `filter`.
    pub fn find(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOptions>>,
    ) -> Result<Cursor<T>> {
        RUNTIME
            .block_on(self.async_collection.find(filter.into(), options.into()))
            .map(Cursor::new)
    }

    /// Finds the documents in the collection matching `filter` using the provided `ClientSession`.
    pub fn find_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<T>> {
        RUNTIME
            .block_on(self.async_collection.find_with_session(
                filter.into(),
                options.into(),
                session,
            ))
            .map(SessionCursor::new)
    }

    /// Finds a single document in the collection matching `filter`.
    pub fn find_one(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOneOptions>>,
    ) -> Result<Option<T>> {
        RUNTIME.block_on(
            self.async_collection
                .find_one(filter.into(), options.into()),
        )
    }

    /// Finds a single document in the collection matching `filter` using the provided
    /// `ClientSession`.
    pub fn find_one_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOneOptions>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        RUNTIME.block_on(self.async_collection.find_one_with_session(
            filter.into(),
            options.into(),
            session,
        ))
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
    ) -> Result<Option<T>> {
        RUNTIME.block_on(
            self.async_collection
                .find_one_and_delete(filter, options.into()),
        )
    }

    /// Atomically finds up to one document in the collection matching `filter` and deletes it using
    /// the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_delete_with_session(
        &self,
        filter: Document,
        options: impl Into<Option<FindOneAndDeleteOptions>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        RUNTIME.block_on(self.async_collection.find_one_and_delete_with_session(
            filter,
            options.into(),
            session,
        ))
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
    ) -> Result<Option<T>> {
        RUNTIME.block_on(self.async_collection.find_one_and_replace(
            filter,
            replacement,
            options.into(),
        ))
    }

    /// Atomically finds up to one document in the collection matching `filter` and replaces it with
    /// `replacement` using the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_replace_with_session(
        &self,
        filter: Document,
        replacement: T,
        options: impl Into<Option<FindOneAndReplaceOptions>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        RUNTIME.block_on(self.async_collection.find_one_and_replace_with_session(
            filter,
            replacement,
            options.into(),
            session,
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
    ) -> Result<Option<T>> {
        RUNTIME.block_on(self.async_collection.find_one_and_update(
            filter,
            update.into(),
            options.into(),
        ))
    }

    /// Atomically finds up to one document in the collection matching `filter` and updates it using
    /// the provided `ClientSession`. Both `Document` and `Vec<Document>` implement
    /// `Into<UpdateModifications>`, so either can be passed in place of constructing the enum
    /// case. Note: pipeline updates are only supported in MongoDB 4.2+.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_update_with_session(
        &self,
        filter: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<FindOneAndUpdateOptions>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        RUNTIME.block_on(self.async_collection.find_one_and_update_with_session(
            filter,
            update.into(),
            options.into(),
            session,
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

    /// Inserts the documents in `docs` into the collection using the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn insert_many_with_session(
        &self,
        docs: impl IntoIterator<Item = T>,
        options: impl Into<Option<InsertManyOptions>>,
        session: &mut ClientSession,
    ) -> Result<InsertManyResult> {
        let docs: Vec<T> = docs.into_iter().collect();
        RUNTIME.block_on(self.async_collection.insert_many_with_session(
            docs,
            options.into(),
            session,
        ))
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

    /// Inserts `doc` into the collection using the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn insert_one_with_session(
        &self,
        doc: T,
        options: impl Into<Option<InsertOneOptions>>,
        session: &mut ClientSession,
    ) -> Result<InsertOneResult> {
        RUNTIME.block_on(self.async_collection.insert_one_with_session(
            doc,
            options.into(),
            session,
        ))
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

    /// Replaces up to one document matching `query` in the collection with `replacement` using the
    /// provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn replace_one_with_session(
        &self,
        query: Document,
        replacement: T,
        options: impl Into<Option<ReplaceOptions>>,
        session: &mut ClientSession,
    ) -> Result<UpdateResult> {
        RUNTIME.block_on(self.async_collection.replace_one_with_session(
            query,
            replacement,
            options.into(),
            session,
        ))
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

    /// Updates all documents matching `query` in the collection using the provided `ClientSession`.
    ///
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://docs.mongodb.com/manual/reference/command/update/#behavior) for more information on specifying updates.
    pub fn update_many_with_session(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
        session: &mut ClientSession,
    ) -> Result<UpdateResult> {
        RUNTIME.block_on(self.async_collection.update_many_with_session(
            query,
            update.into(),
            options.into(),
            session,
        ))
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

    /// Updates up to one document matching `query` in the collection using the provided
    /// `ClientSession`.
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
    pub fn update_one_with_session(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
        session: &mut ClientSession,
    ) -> Result<UpdateResult> {
        RUNTIME.block_on(self.async_collection.update_one_with_session(
            query,
            update.into(),
            options.into(),
            session,
        ))
    }
}
