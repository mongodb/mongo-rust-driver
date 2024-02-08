use std::{borrow::Borrow, fmt::Debug};

use serde::{de::DeserializeOwned, Serialize};

use super::{ClientSession, Cursor, SessionCursor};
use crate::{
    bson::{Bson, Document},
    error::Result,
    index::IndexModel,
    options::{
        AggregateOptions,
        CountOptions,
        CreateIndexOptions,
        DeleteOptions,
        DistinctOptions,
        DropIndexOptions,
        EstimatedDocumentCountOptions,
        FindOneAndDeleteOptions,
        FindOneAndReplaceOptions,
        FindOneAndUpdateOptions,
        FindOneOptions,
        FindOptions,
        InsertManyOptions,
        InsertOneOptions,
        ListIndexesOptions,
        ReadConcern,
        ReplaceOptions,
        SelectionCriteria,
        UpdateModifications,
        UpdateOptions,
        WriteConcern,
    },
    results::{
        CreateIndexResult,
        CreateIndexesResult,
        DeleteResult,
        InsertManyResult,
        InsertOneResult,
        UpdateResult,
    },
    runtime,
    Collection as AsyncCollection,
    Namespace,
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
pub struct Collection<T> {
    pub(crate) async_collection: AsyncCollection<T>,
}

impl<T> Collection<T> {
    pub(crate) fn new(async_collection: AsyncCollection<T>) -> Self {
        Self { async_collection }
    }

    /// Gets a clone of the `Collection` with a different type `U`.
    pub fn clone_with_type<U>(&self) -> Collection<U> {
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
        runtime::block_on(self.async_collection.aggregate(pipeline, options.into()))
            .map(Cursor::new)
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
        runtime::block_on(self.async_collection.aggregate_with_session(
            pipeline,
            options.into(),
            &mut session.async_client_session,
        ))
        .map(SessionCursor::new)
    }

    /// Estimates the number of documents in the collection using collection metadata.
    ///
    /// Due to an oversight in versions 5.0.0 - 5.0.7 of MongoDB, the `count` server command,
    /// which `estimatedDocumentCount` uses in its implementation, was not included in v1 of the
    /// Stable API. Users of the Stable API with `estimatedDocumentCount` are recommended to
    /// upgrade their cluster to 5.0.8+ or set
    /// [`ServerApi::strict`](crate::options::ServerApi::strict) to false to avoid encountering
    /// errors.
    ///
    /// For more information on the behavior of the `count` server command, see
    /// [Count: Behavior](https://www.mongodb.com/docs/manual/reference/command/count/#behavior).
    pub fn estimated_document_count(
        &self,
        options: impl Into<Option<EstimatedDocumentCountOptions>>,
    ) -> Result<u64> {
        runtime::block_on(
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
    ) -> Result<u64> {
        runtime::block_on(
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
    ) -> Result<u64> {
        runtime::block_on(self.async_collection.count_documents_with_session(
            filter.into(),
            options.into(),
            &mut session.async_client_session,
        ))
    }

    /// Creates the given index on this collection.
    pub fn create_index(
        &self,
        index: IndexModel,
        options: impl Into<Option<CreateIndexOptions>>,
    ) -> Result<CreateIndexResult> {
        runtime::block_on(self.async_collection.create_index(index, options))
    }

    /// Creates the given index on this collection using the provided `ClientSession`.
    pub fn create_index_with_session(
        &self,
        index: IndexModel,
        options: impl Into<Option<CreateIndexOptions>>,
        session: &mut ClientSession,
    ) -> Result<CreateIndexResult> {
        runtime::block_on(self.async_collection.create_index_with_session(
            index,
            options,
            &mut session.async_client_session,
        ))
    }

    /// Creates the given indexes on this collection.
    pub fn create_indexes(
        &self,
        indexes: impl IntoIterator<Item = IndexModel>,
        options: impl Into<Option<CreateIndexOptions>>,
    ) -> Result<CreateIndexesResult> {
        runtime::block_on(self.async_collection.create_indexes(indexes, options))
    }

    /// Creates the given indexes on this collection using the provided `ClientSession`.
    pub fn create_indexes_with_session(
        &self,
        indexes: impl IntoIterator<Item = IndexModel>,
        options: impl Into<Option<CreateIndexOptions>>,
        session: &mut ClientSession,
    ) -> Result<CreateIndexesResult> {
        runtime::block_on(self.async_collection.create_indexes_with_session(
            indexes,
            options,
            &mut session.async_client_session,
        ))
    }

    /// Deletes all documents stored in the collection matching `query`.
    pub fn delete_many(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
    ) -> Result<DeleteResult> {
        runtime::block_on(self.async_collection.delete_many(query, options.into()))
    }

    /// Deletes all documents stored in the collection matching `query` using the provided
    /// `ClientSession`.
    pub fn delete_many_with_session(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
        session: &mut ClientSession,
    ) -> Result<DeleteResult> {
        runtime::block_on(self.async_collection.delete_many_with_session(
            query,
            options.into(),
            &mut session.async_client_session,
        ))
    }

    /// Deletes up to one document found matching `query`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn delete_one(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
    ) -> Result<DeleteResult> {
        runtime::block_on(self.async_collection.delete_one(query, options.into()))
    }

    /// Deletes up to one document found matching `query` using the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn delete_one_with_session(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
        session: &mut ClientSession,
    ) -> Result<DeleteResult> {
        runtime::block_on(self.async_collection.delete_one_with_session(
            query,
            options.into(),
            &mut session.async_client_session,
        ))
    }

    /// Finds the distinct values of the field specified by `field_name` across the collection.
    pub fn distinct(
        &self,
        field_name: impl AsRef<str>,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<DistinctOptions>>,
    ) -> Result<Vec<Bson>> {
        runtime::block_on(self.async_collection.distinct(
            field_name.as_ref(),
            filter.into(),
            options.into(),
        ))
    }

    /// Finds the distinct values of the field specified by `field_name` across the collection using
    /// the provided `ClientSession`.
    pub fn distinct_with_session(
        &self,
        field_name: impl AsRef<str>,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<DistinctOptions>>,
        session: &mut ClientSession,
    ) -> Result<Vec<Bson>> {
        runtime::block_on(self.async_collection.distinct_with_session(
            field_name.as_ref(),
            filter.into(),
            options.into(),
            &mut session.async_client_session,
        ))
    }

    /// Updates all documents matching `query` in the collection.
    ///
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://www.mongodb.com/docs/manual/reference/command/update/#behavior) for more information on specifying updates.
    pub fn update_many(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
    ) -> Result<UpdateResult> {
        runtime::block_on(
            self.async_collection
                .update_many(query, update.into(), options.into()),
        )
    }

    /// Drops the index specified by `name` from this collection.
    pub fn drop_index(
        &self,
        name: impl AsRef<str>,
        options: impl Into<Option<DropIndexOptions>>,
    ) -> Result<()> {
        runtime::block_on(self.async_collection.drop_index(name, options))
    }

    /// Drops the index specified by `name` from this collection using the provided `ClientSession`.
    pub fn drop_index_with_session(
        &self,
        name: impl AsRef<str>,
        options: impl Into<Option<DropIndexOptions>>,
        session: &mut ClientSession,
    ) -> Result<()> {
        runtime::block_on(self.async_collection.drop_index_with_session(
            name,
            options,
            &mut session.async_client_session,
        ))
    }

    /// Drops all indexes associated with this collection.
    pub fn drop_indexes(&self, options: impl Into<Option<DropIndexOptions>>) -> Result<()> {
        runtime::block_on(self.async_collection.drop_indexes(options))
    }

    /// Drops all indexes associated with this collection using the provided `ClientSession`.
    pub fn drop_indexes_with_session(
        &self,
        options: impl Into<Option<DropIndexOptions>>,
        session: &mut ClientSession,
    ) -> Result<()> {
        runtime::block_on(
            self.async_collection
                .drop_indexes_with_session(options, &mut session.async_client_session),
        )
    }

    /// Lists all indexes on this collection.
    pub fn list_indexes(
        &self,
        options: impl Into<Option<ListIndexesOptions>>,
    ) -> Result<Cursor<IndexModel>> {
        runtime::block_on(self.async_collection.list_indexes(options)).map(Cursor::new)
    }

    /// Lists all indexes on this collection using the provided `ClientSession`.
    pub fn list_indexes_with_session(
        &self,
        options: impl Into<Option<ListIndexesOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<IndexModel>> {
        runtime::block_on(
            self.async_collection
                .list_indexes_with_session(options, &mut session.async_client_session),
        )
        .map(SessionCursor::new)
    }

    /// Gets the names of all indexes on the collection.
    pub fn list_index_names(&self) -> Result<Vec<String>> {
        runtime::block_on(self.async_collection.list_index_names())
    }

    /// Gets the names of all indexes on the collection using the provided `ClientSession`.
    pub fn list_index_names_with_session(
        &self,
        session: &mut ClientSession,
    ) -> Result<Vec<String>> {
        runtime::block_on(
            self.async_collection
                .list_index_names_with_session(&mut session.async_client_session),
        )
    }

    /// Updates all documents matching `query` in the collection using the provided `ClientSession`.
    ///
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://www.mongodb.com/docs/manual/reference/command/update/#behavior) for more information on specifying updates.
    pub fn update_many_with_session(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
        session: &mut ClientSession,
    ) -> Result<UpdateResult> {
        runtime::block_on(self.async_collection.update_many_with_session(
            query,
            update.into(),
            options.into(),
            &mut session.async_client_session,
        ))
    }

    /// Updates up to one document matching `query` in the collection.
    ///
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://www.mongodb.com/docs/manual/reference/command/update/#behavior) for more information on specifying updates.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn update_one(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
    ) -> Result<UpdateResult> {
        runtime::block_on(
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
    /// [documentation](https://www.mongodb.com/docs/manual/reference/command/update/#behavior) for more information on specifying updates.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn update_one_with_session(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
        session: &mut ClientSession,
    ) -> Result<UpdateResult> {
        runtime::block_on(self.async_collection.update_one_with_session(
            query,
            update.into(),
            options.into(),
            &mut session.async_client_session,
        ))
    }

    /// Finds the documents in the collection matching `filter`.
    pub fn find(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOptions>>,
    ) -> Result<Cursor<T>> {
        runtime::block_on(self.async_collection.find(filter.into(), options.into()))
            .map(Cursor::new)
    }

    /// Finds the documents in the collection matching `filter` using the provided `ClientSession`.
    pub fn find_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<T>> {
        runtime::block_on(self.async_collection.find_with_session(
            filter.into(),
            options.into(),
            &mut session.async_client_session,
        ))
        .map(SessionCursor::new)
    }
}

impl<T> Collection<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    /// Finds a single document in the collection matching `filter`.
    pub fn find_one(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOneOptions>>,
    ) -> Result<Option<T>> {
        runtime::block_on(
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
        runtime::block_on(self.async_collection.find_one_with_session(
            filter.into(),
            options.into(),
            &mut session.async_client_session,
        ))
    }
}

impl<T> Collection<T>
where
    T: DeserializeOwned,
{
    /// Atomically finds up to one document in the collection matching `filter` and deletes it.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_delete(
        &self,
        filter: Document,
        options: impl Into<Option<FindOneAndDeleteOptions>>,
    ) -> Result<Option<T>> {
        runtime::block_on(
            self.async_collection
                .find_one_and_delete(filter, options.into()),
        )
    }

    /// Atomically finds up to one document in the collection matching `filter` and deletes it using
    /// the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_delete_with_session(
        &self,
        filter: Document,
        options: impl Into<Option<FindOneAndDeleteOptions>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        runtime::block_on(self.async_collection.find_one_and_delete_with_session(
            filter,
            options.into(),
            &mut session.async_client_session,
        ))
    }

    /// Atomically finds up to one document in the collection matching `filter` and updates it.
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_update(
        &self,
        filter: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<FindOneAndUpdateOptions>>,
    ) -> Result<Option<T>> {
        runtime::block_on(self.async_collection.find_one_and_update(
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
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_update_with_session(
        &self,
        filter: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<FindOneAndUpdateOptions>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        runtime::block_on(self.async_collection.find_one_and_update_with_session(
            filter,
            update.into(),
            options.into(),
            &mut session.async_client_session,
        ))
    }
}

impl<T> Collection<T>
where
    T: Serialize + DeserializeOwned,
{
    /// Atomically finds up to one document in the collection matching `filter` and replaces it with
    /// `replacement`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_replace(
        &self,
        filter: Document,
        replacement: T,
        options: impl Into<Option<FindOneAndReplaceOptions>>,
    ) -> Result<Option<T>> {
        runtime::block_on(self.async_collection.find_one_and_replace(
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
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn find_one_and_replace_with_session(
        &self,
        filter: Document,
        replacement: T,
        options: impl Into<Option<FindOneAndReplaceOptions>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        runtime::block_on(self.async_collection.find_one_and_replace_with_session(
            filter,
            replacement,
            options.into(),
            &mut session.async_client_session,
        ))
    }
}

impl<T> Collection<T>
where
    T: Serialize,
{
    /// Inserts the documents in `docs` into the collection.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn insert_many(
        &self,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        options: impl Into<Option<InsertManyOptions>>,
    ) -> Result<InsertManyResult> {
        runtime::block_on(self.async_collection.insert_many(docs, options.into()))
    }

    /// Inserts the documents in `docs` into the collection using the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn insert_many_with_session(
        &self,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        options: impl Into<Option<InsertManyOptions>>,
        session: &mut ClientSession,
    ) -> Result<InsertManyResult> {
        runtime::block_on(self.async_collection.insert_many_with_session(
            docs,
            options.into(),
            &mut session.async_client_session,
        ))
    }

    /// Inserts `doc` into the collection.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn insert_one(
        &self,
        doc: impl Borrow<T>,
        options: impl Into<Option<InsertOneOptions>>,
    ) -> Result<InsertOneResult> {
        runtime::block_on(
            self.async_collection
                .insert_one(doc.borrow(), options.into()),
        )
    }

    /// Inserts `doc` into the collection using the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn insert_one_with_session(
        &self,
        doc: impl Borrow<T>,
        options: impl Into<Option<InsertOneOptions>>,
        session: &mut ClientSession,
    ) -> Result<InsertOneResult> {
        runtime::block_on(self.async_collection.insert_one_with_session(
            doc.borrow(),
            options.into(),
            &mut session.async_client_session,
        ))
    }

    /// Replaces up to one document matching `query` in the collection with `replacement`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn replace_one(
        &self,
        query: Document,
        replacement: impl Borrow<T>,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> Result<UpdateResult> {
        runtime::block_on(self.async_collection.replace_one(
            query,
            replacement.borrow(),
            options.into(),
        ))
    }

    /// Replaces up to one document matching `query` in the collection with `replacement` using the
    /// provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn replace_one_with_session(
        &self,
        query: Document,
        replacement: impl Borrow<T>,
        options: impl Into<Option<ReplaceOptions>>,
        session: &mut ClientSession,
    ) -> Result<UpdateResult> {
        runtime::block_on(self.async_collection.replace_one_with_session(
            query,
            replacement.borrow(),
            options.into(),
            &mut session.async_client_session,
        ))
    }
}
