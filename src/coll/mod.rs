mod batch;
pub mod options;

use std::{fmt, fmt::Debug, sync::Arc};

use futures::StreamExt;
use serde::{
    de::{DeserializeOwned, Error},
    Deserialize,
    Deserializer,
    Serialize,
};

use self::options::*;
use crate::{
    bson::{doc, ser, to_document, Bson, Document},
    bson_util,
    client::session::ClientSession,
    concern::{ReadConcern, WriteConcern},
    error::{convert_bulk_errors, BulkWriteError, BulkWriteFailure, ErrorKind, Result},
    operation::{
        Aggregate,
        Count,
        CountDocuments,
        Delete,
        Distinct,
        DropCollection,
        Find,
        FindAndModify,
        Insert,
        Update,
    },
    results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
    selection_criteria::SelectionCriteria,
    Client,
    Cursor,
    Database,
    SessionCursor,
};

/// Maximum size in bytes of an insert batch.
/// This is intentionally less than the actual max document size, which is 16*1024*1024 bytes, to
/// allow for overhead in the command document.
const MAX_INSERT_DOCS_BYTES: usize = 16 * 1000 * 1000;

/// `Collection` is the client-side abstraction of a MongoDB Collection. It can be used to
/// perform collection-level operations such as CRUD operations. A `Collection` can be obtained
/// through a [`Database`](struct.Database.html) by calling either
/// [`Database::collection`](struct.Database.html#method.collection) or
/// [`Database::collection_with_options`](struct.Database.html#method.collection_with_options).
///
/// `Collection` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads or async tasks. For example:
///
/// ```rust
/// # use mongodb::{
/// #     bson::doc,
/// #     error::Result,
/// # };
/// # #[cfg(feature = "async-std-runtime")]
/// # use async_std::task;
/// # #[cfg(feature = "tokio-runtime")]
/// # use tokio::task;
/// #
/// # #[cfg(not(feature = "sync"))]
/// # async fn start_workers() -> Result<()> {
/// # use mongodb::Client;
/// #
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// let coll = client.database("items").collection("in_stock");
///
/// for i in 0..5 {
///     let coll_ref = coll.clone();
///
///     task::spawn(async move {
///         // Perform operations with `coll_ref`. For example:
///         coll_ref.insert_one(doc! { "x": i }, None).await;
///     });
/// }
/// #
/// # Ok(())
/// # }
/// ```

#[derive(Debug, Clone)]
pub struct Collection<T = Document>
where
    T: Serialize + DeserializeOwned + Unpin + Debug,
{
    inner: Arc<CollectionInner>,
    _phantom: std::marker::PhantomData<T>,
}

#[derive(Debug)]
struct CollectionInner {
    client: Client,
    db: Database,
    name: String,
    selection_criteria: Option<SelectionCriteria>,
    read_concern: Option<ReadConcern>,
    write_concern: Option<WriteConcern>,
}

impl<T> Collection<T>
where
    T: Serialize + DeserializeOwned + Unpin + Debug,
{
    pub(crate) fn new(db: Database, name: &str, options: Option<CollectionOptions>) -> Self {
        let options = options.unwrap_or_default();
        let selection_criteria = options
            .selection_criteria
            .or_else(|| db.selection_criteria().cloned());

        let read_concern = options.read_concern.or_else(|| db.read_concern().cloned());

        let write_concern = options
            .write_concern
            .or_else(|| db.write_concern().cloned());

        Self {
            inner: Arc::new(CollectionInner {
                client: db.client().clone(),
                db,
                name: name.to_string(),
                selection_criteria,
                read_concern,
                write_concern,
            }),
            _phantom: Default::default(),
        }
    }

    /// Gets a clone of the `Collection` with a different type `U`.
    pub fn clone_with_type<U: Serialize + DeserializeOwned + Unpin + Debug>(
        &self,
    ) -> Collection<U> {
        let mut options = CollectionOptions::builder().build();
        options.selection_criteria = self.inner.selection_criteria.clone();
        options.read_concern = self.inner.read_concern.clone();
        options.write_concern = self.inner.write_concern.clone();

        Collection::new(self.inner.db.clone(), &self.inner.name, Some(options))
    }

    /// Get the `Client` that this collection descended from.
    fn client(&self) -> &Client {
        &self.inner.client
    }

    /// Gets the name of the `Collection`.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Gets the namespace of the `Collection`.
    ///
    /// The namespace of a MongoDB collection is the concatenation of the name of the database
    /// containing it, the '.' character, and the name of the collection itself. For example, if a
    /// collection named "bar" is created in a database named "foo", the namespace of the collection
    /// is "foo.bar".
    pub fn namespace(&self) -> Namespace {
        Namespace {
            db: self.inner.db.name().into(),
            coll: self.name().into(),
        }
    }

    /// Gets the selection criteria of the `Collection`.
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.inner.selection_criteria.as_ref()
    }

    /// Gets the read concern of the `Collection`.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.read_concern.as_ref()
    }

    /// Gets the write concern of the `Collection`.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.write_concern.as_ref()
    }

    async fn drop_common(
        &self,
        options: impl Into<Option<DropCollectionOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<()> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let drop = DropCollection::new(self.namespace(), options);
        self.client().execute_operation(drop, session.into()).await
    }

    /// Drops the collection, deleting all data and indexes stored in it.
    pub async fn drop(&self, options: impl Into<Option<DropCollectionOptions>>) -> Result<()> {
        self.drop_common(options, None).await
    }

    /// Drops the collection, deleting all data and indexes stored in it using the provided
    /// `ClientSession`.
    pub async fn drop_with_session(
        &self,
        options: impl Into<Option<DropCollectionOptions>>,
        session: &mut ClientSession,
    ) -> Result<()> {
        self.drop_common(options, session).await
    }

    /// Runs an aggregation operation.
    ///
    /// See the documentation [here](https://docs.mongodb.com/manual/aggregation/) for more
    /// information on aggregations.
    pub async fn aggregate(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<AggregateOptions>>,
    ) -> Result<Cursor<Document>> {
        let mut options = options.into();
        resolve_options!(
            self,
            options,
            [read_concern, write_concern, selection_criteria]
        );

        let aggregate = Aggregate::new(self.namespace(), pipeline, options);
        let client = self.client();
        client
            .execute_cursor_operation(aggregate)
            .await
            .map(|(spec, session)| Cursor::new(client.clone(), spec, session))
    }

    /// Runs an aggregation operation using the provided `ClientSession`.
    ///
    /// See the documentation [here](https://docs.mongodb.com/manual/aggregation/) for more
    /// information on aggregations.
    pub async fn aggregate_with_session(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<AggregateOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<Document>> {
        let mut options = options.into();
        resolve_options!(
            self,
            options,
            [read_concern, write_concern, selection_criteria]
        );

        let aggregate = Aggregate::new(self.namespace(), pipeline, options);
        let client = self.client();
        client
            .execute_operation(aggregate, session)
            .await
            .map(|result| SessionCursor::new(client.clone(), result))
    }

    /// Estimates the number of documents in the collection using collection metadata.
    pub async fn estimated_document_count(
        &self,
        options: impl Into<Option<EstimatedDocumentCountOptions>>,
    ) -> Result<i64> {
        let mut options = options.into();
        resolve_options!(self, options, [read_concern, selection_criteria]);

        let op = Count::new(self.namespace(), options);

        self.client().execute_operation(op, None).await
    }

    async fn count_documents_common(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<CountOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<i64> {
        let mut options = options.into();
        resolve_options!(self, options, [read_concern, selection_criteria]);

        let op = CountDocuments::new(self.namespace(), filter.into(), options);
        self.client().execute_operation(op, session).await
    }

    /// Gets the number of documents matching `filter`.
    ///
    /// Note that using [`Collection::estimated_document_count`](#method.estimated_document_count)
    /// is recommended instead of this method is most cases.
    pub async fn count_documents(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<CountOptions>>,
    ) -> Result<i64> {
        self.count_documents_common(filter, options, None).await
    }

    /// Gets the number of documents matching `filter` using the provided `ClientSession`.
    ///
    /// Note that using [`Collection::estimated_document_count`](#method.estimated_document_count)
    /// is recommended instead of this method is most cases.
    pub async fn count_documents_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<CountOptions>>,
        session: &mut ClientSession,
    ) -> Result<i64> {
        self.count_documents_common(filter, options, session).await
    }

    async fn delete_many_common(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<DeleteResult> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let delete = Delete::new(self.namespace(), query, None, options);
        self.client().execute_operation(delete, session).await
    }

    /// Deletes all documents stored in the collection matching `query`.
    pub async fn delete_many(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
    ) -> Result<DeleteResult> {
        self.delete_many_common(query, options, None).await
    }

    /// Deletes all documents stored in the collection matching `query` using the provided
    /// `ClientSession`.
    pub async fn delete_many_with_session(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
        session: &mut ClientSession,
    ) -> Result<DeleteResult> {
        self.delete_many_common(query, options, session).await
    }

    async fn delete_one_common(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<DeleteResult> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let delete = Delete::new(self.namespace(), query, Some(1), options);
        self.client().execute_operation(delete, session).await
    }

    /// Deletes up to one document found matching `query`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn delete_one(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
    ) -> Result<DeleteResult> {
        self.delete_one_common(query, options, None).await
    }

    /// Deletes up to one document found matching `query` using the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn delete_one_with_session(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
        session: &mut ClientSession,
    ) -> Result<DeleteResult> {
        self.delete_one_common(query, options, session).await
    }

    async fn distinct_common(
        &self,
        field_name: &str,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<DistinctOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<Vec<Bson>> {
        let mut options = options.into();
        resolve_options!(self, options, [read_concern, selection_criteria]);

        let op = Distinct::new(
            self.namespace(),
            field_name.to_string(),
            filter.into(),
            options,
        );
        self.client().execute_operation(op, session).await
    }

    /// Finds the distinct values of the field specified by `field_name` across the collection.
    pub async fn distinct(
        &self,
        field_name: &str,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<DistinctOptions>>,
    ) -> Result<Vec<Bson>> {
        self.distinct_common(field_name, filter, options, None)
            .await
    }

    /// Finds the distinct values of the field specified by `field_name` across the collection using
    /// the provided `ClientSession`.
    pub async fn distinct_with_session(
        &self,
        field_name: &str,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<DistinctOptions>>,
        session: &mut ClientSession,
    ) -> Result<Vec<Bson>> {
        self.distinct_common(field_name, filter, options, session)
            .await
    }

    /// Finds the documents in the collection matching `filter`.
    pub async fn find(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOptions>>,
    ) -> Result<Cursor<T>> {
        let mut options = options.into();
        resolve_options!(self, options, [read_concern, selection_criteria]);

        let find = Find::new(self.namespace(), filter.into(), options);
        let client = self.client();

        client
            .execute_cursor_operation(find)
            .await
            .map(|(result, session)| Cursor::new(client.clone(), result, session))
    }

    /// Finds the documents in the collection matching `filter` using the provided `ClientSession`.
    pub async fn find_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<T>> {
        let find = Find::new(self.namespace(), filter.into(), options.into());
        let client = self.client();

        client
            .execute_operation(find, session)
            .await
            .map(|result| SessionCursor::new(client.clone(), result))
    }

    /// Finds a single document in the collection matching `filter`.
    pub async fn find_one(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOneOptions>>,
    ) -> Result<Option<T>> {
        let mut options = options.into();
        resolve_options!(self, options, [read_concern, selection_criteria]);

        let options: FindOptions = options.map(Into::into).unwrap_or_else(Default::default);
        let mut cursor = self.find(filter, Some(options)).await?;
        cursor.next().await.transpose()
    }

    /// Finds a single document in the collection matching `filter` using the provided
    /// `ClientSession`.
    pub async fn find_one_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOneOptions>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        let mut options = options.into();
        resolve_options!(self, options, [read_concern, selection_criteria]);

        let options: FindOptions = options.map(Into::into).unwrap_or_else(Default::default);
        let mut cursor = self
            .find_with_session(filter, Some(options), session)
            .await?;
        let mut cursor = cursor.with_session(session);
        cursor.next().await.transpose()
    }

    async fn find_one_and_delete_common(
        &self,
        filter: Document,
        options: impl Into<Option<FindOneAndDeleteOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<Option<T>> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let op = FindAndModify::<T>::with_delete(self.namespace(), filter, options);
        self.client().execute_operation(op, session).await
    }

    /// Atomically finds up to one document in the collection matching `filter` and deletes it.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn find_one_and_delete(
        &self,
        filter: Document,
        options: impl Into<Option<FindOneAndDeleteOptions>>,
    ) -> Result<Option<T>> {
        self.find_one_and_delete_common(filter, options, None).await
    }

    /// Atomically finds up to one document in the collection matching `filter` and deletes it using
    /// the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn find_one_and_delete_with_session(
        &self,
        filter: Document,
        options: impl Into<Option<FindOneAndDeleteOptions>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        self.find_one_and_delete_common(filter, options, session)
            .await
    }

    async fn find_one_and_replace_common(
        &self,
        filter: Document,
        replacement: T,
        options: impl Into<Option<FindOneAndReplaceOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<Option<T>> {
        let replacement = to_document(&replacement)?;

        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let op = FindAndModify::<T>::with_replace(self.namespace(), filter, replacement, options)?;
        self.client().execute_operation(op, session).await
    }

    /// Atomically finds up to one document in the collection matching `filter` and replaces it with
    /// `replacement`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn find_one_and_replace(
        &self,
        filter: Document,
        replacement: T,
        options: impl Into<Option<FindOneAndReplaceOptions>>,
    ) -> Result<Option<T>> {
        self.find_one_and_replace_common(filter, replacement, options, None)
            .await
    }

    /// Atomically finds up to one document in the collection matching `filter` and replaces it with
    /// `replacement` using the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn find_one_and_replace_with_session(
        &self,
        filter: Document,
        replacement: T,
        options: impl Into<Option<FindOneAndReplaceOptions>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        self.find_one_and_replace_common(filter, replacement, options, session)
            .await
    }

    async fn find_one_and_update_common(
        &self,
        filter: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<FindOneAndUpdateOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<Option<T>> {
        let update = update.into();
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let op = FindAndModify::<T>::with_update(self.namespace(), filter, update, options)?;
        self.client().execute_operation(op, session).await
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
    pub async fn find_one_and_update(
        &self,
        filter: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<FindOneAndUpdateOptions>>,
    ) -> Result<Option<T>> {
        self.find_one_and_update_common(filter, update, options, None)
            .await
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
    pub async fn find_one_and_update_with_session(
        &self,
        filter: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<FindOneAndUpdateOptions>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        self.find_one_and_update_common(filter, update, options, session)
            .await
    }

    async fn insert_many_common(
        &self,
        docs: impl IntoIterator<Item = T>,
        options: impl Into<Option<InsertManyOptions>>,
        mut session: Option<&mut ClientSession>,
    ) -> Result<InsertManyResult> {
        let docs: ser::Result<Vec<Document>> = docs
            .into_iter()
            .map(|doc| bson::to_document(&doc))
            .collect();
        let mut docs: Vec<Document> = docs?;

        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        if docs.is_empty() {
            return Err(ErrorKind::ArgumentError {
                message: "No documents provided to insert_many".to_string(),
            }
            .into());
        }

        let ordered = options.as_ref().and_then(|o| o.ordered).unwrap_or(true);

        let mut cumulative_failure: Option<BulkWriteFailure> = None;
        let mut cumulative_result: Option<InsertManyResult> = None;

        let mut n_attempted = 0;

        while !docs.is_empty() {
            let mut remaining_docs =
                batch::split_off_batch(&mut docs, MAX_INSERT_DOCS_BYTES, bson_util::doc_size_bytes);
            std::mem::swap(&mut docs, &mut remaining_docs);
            let current_batch = remaining_docs;

            let current_batch_size = current_batch.len();
            n_attempted += current_batch_size;

            let insert = Insert::new(self.namespace(), current_batch, options.clone());
            match self
                .client()
                .execute_operation(insert, session.as_deref_mut())
                .await
            {
                Ok(result) => {
                    if cumulative_failure.is_none() {
                        let cumulative_result =
                            cumulative_result.get_or_insert_with(InsertManyResult::new);
                        for (index, id) in result.inserted_ids {
                            cumulative_result
                                .inserted_ids
                                .insert(index + n_attempted - current_batch_size, id);
                        }
                    }
                }
                Err(e) => match e.kind {
                    ErrorKind::BulkWriteError(failure) => {
                        let failure_ref =
                            cumulative_failure.get_or_insert_with(BulkWriteFailure::new);
                        if let Some(ref write_errors) = failure.write_errors {
                            failure_ref
                                .write_errors
                                .get_or_insert_with(Default::default)
                                .extend(write_errors.iter().map(|error| BulkWriteError {
                                    index: error.index + n_attempted - current_batch_size,
                                    ..error.clone()
                                }));
                        }
                        if let Some(ref write_concern_error) = failure.write_concern_error {
                            failure_ref.write_concern_error = Some(write_concern_error.clone());
                        }

                        if ordered {
                            return Err(ErrorKind::BulkWriteError(
                                cumulative_failure.unwrap_or_else(BulkWriteFailure::new),
                            )
                            .into());
                        }
                    }
                    _ => return Err(e),
                },
            }
        }

        match cumulative_failure {
            Some(failure) => Err(ErrorKind::BulkWriteError(failure).into()),
            None => Ok(cumulative_result.unwrap_or_else(InsertManyResult::new)),
        }
    }

    /// Inserts the data in `docs` into the collection.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn insert_many(
        &self,
        docs: impl IntoIterator<Item = T>,
        options: impl Into<Option<InsertManyOptions>>,
    ) -> Result<InsertManyResult> {
        self.insert_many_common(docs, options, None).await
    }

    /// Inserts the data in `docs` into the collection using the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn insert_many_with_session(
        &self,
        docs: impl IntoIterator<Item = T>,
        options: impl Into<Option<InsertManyOptions>>,
        session: &mut ClientSession,
    ) -> Result<InsertManyResult> {
        self.insert_many_common(docs, options, Some(session)).await
    }

    async fn insert_one_common(
        &self,
        doc: T,
        options: impl Into<Option<InsertOneOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<InsertOneResult> {
        let doc = to_document(&doc)?;

        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let insert = Insert::new(
            self.namespace(),
            vec![doc],
            options.map(InsertManyOptions::from_insert_one_options),
        );
        self.client()
            .execute_operation(insert, session)
            .await
            .map(InsertOneResult::from_insert_many_result)
            .map_err(convert_bulk_errors)
    }

    /// Inserts `doc` into the collection.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn insert_one(
        &self,
        doc: T,
        options: impl Into<Option<InsertOneOptions>>,
    ) -> Result<InsertOneResult> {
        self.insert_one_common(doc, options, None).await
    }

    /// Inserts `doc` into the collection using the provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn insert_one_with_session(
        &self,
        doc: T,
        options: impl Into<Option<InsertOneOptions>>,
        session: &mut ClientSession,
    ) -> Result<InsertOneResult> {
        self.insert_one_common(doc, options, session).await
    }

    async fn replace_one_common(
        &self,
        query: Document,
        replacement: T,
        options: impl Into<Option<ReplaceOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<UpdateResult> {
        let replacement = to_document(&replacement)?;

        bson_util::replacement_document_check(&replacement)?;

        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let update = Update::new(
            self.namespace(),
            query,
            UpdateModifications::Document(replacement),
            false,
            options.map(UpdateOptions::from_replace_options),
        );
        self.client().execute_operation(update, session).await
    }

    /// Replaces up to one document matching `query` in the collection with `replacement`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn replace_one(
        &self,
        query: Document,
        replacement: T,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> Result<UpdateResult> {
        self.replace_one_common(query, replacement, options, None)
            .await
    }

    /// Replaces up to one document matching `query` in the collection with `replacement` using the
    /// provided `ClientSession`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn replace_one_with_session(
        &self,
        query: Document,
        replacement: T,
        options: impl Into<Option<ReplaceOptions>>,
        session: &mut ClientSession,
    ) -> Result<UpdateResult> {
        self.replace_one_common(query, replacement, options, session)
            .await
    }

    async fn update_many_common(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<UpdateResult> {
        let update = update.into();

        let mut options = options.into();

        if let UpdateModifications::Document(ref d) = update {
            bson_util::update_document_check(d)?;
        }

        resolve_options!(self, options, [write_concern]);

        let update = Update::new(self.namespace(), query, update, true, options);
        self.client().execute_operation(update, session).await
    }

    /// Updates all documents matching `query` in the collection.
    ///
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://docs.mongodb.com/manual/reference/command/update/#behavior) for more information on specifying updates.
    pub async fn update_many(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
    ) -> Result<UpdateResult> {
        self.update_many_common(query, update, options, None).await
    }

    /// Updates all documents matching `query` in the collection using the provided `ClientSession`.
    ///
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://docs.mongodb.com/manual/reference/command/update/#behavior) for more information on specifying updates.
    pub async fn update_many_with_session(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
        session: &mut ClientSession,
    ) -> Result<UpdateResult> {
        self.update_many_common(query, update, options, session)
            .await
    }

    async fn update_one_common(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<UpdateResult> {
        let mut options = options.into();
        let update = update.into();

        if let UpdateModifications::Document(ref d) = update {
            bson_util::update_document_check(d)?;
        }

        resolve_options!(self, options, [write_concern]);

        let update = Update::new(self.namespace(), query, update, false, options);
        self.client().execute_operation(update, session).await
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
    pub async fn update_one(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
    ) -> Result<UpdateResult> {
        self.update_one_common(query, update, options, None).await
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
    pub async fn update_one_with_session(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
        session: &mut ClientSession,
    ) -> Result<UpdateResult> {
        self.update_one_common(query, update, options, session)
            .await
    }

    /// Kill the server side cursor that id corresponds to.
    pub(super) async fn kill_cursor(&self, cursor_id: i64) -> Result<()> {
        let ns = self.namespace();

        self.client()
            .database(ns.db.as_str())
            .run_command(
                doc! {
                    "killCursors": ns.coll.as_str(),
                    "cursors": [cursor_id]
                },
                None,
            )
            .await?;
        Ok(())
    }
}

/// A struct modeling the canonical name for a collection in MongoDB.
#[derive(Debug, Clone)]
pub struct Namespace {
    /// The name of the database associated with this namespace.
    pub db: String,

    /// The name of the collection this namespace corresponds to.
    pub coll: String,
}

impl Namespace {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self {
            db: String::new(),
            coll: String::new(),
        }
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}.{}", self.db, self.coll)
    }
}

impl<'de> Deserialize<'de> for Namespace {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        let mut parts = s.split('.');

        let db = parts.next();
        let coll = parts.collect::<Vec<_>>().join(".");

        match (db, coll) {
            (Some(db), coll) if !coll.is_empty() => Ok(Self {
                db: db.to_string(),
                coll,
            }),
            _ => Err(D::Error::custom("Missing one or more fields in namespace")),
        }
    }
}
