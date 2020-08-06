mod batch;
pub mod options;

use std::{fmt, sync::Arc};

use futures::StreamExt;
use serde::{de::Error, Deserialize, Deserializer};

use self::options::*;
use crate::{
    bson::{doc, Bson, Document},
    bson_util,
    change_stream::{options::ChangeStreamOptions, ChangeStream, ChangeStreamTarget},
    concern::{ReadConcern, WriteConcern},
    error::{convert_bulk_errors, BulkWriteError, BulkWriteFailure, ErrorKind, Result},
    operation::{
        Aggregate,
        Count,
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
pub struct Collection {
    inner: Arc<CollectionInner>,
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

impl Collection {
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
        }
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

    /// Drops the collection, deleting all data, users, and indexes stored in it.
    pub async fn drop(&self, options: impl Into<Option<DropCollectionOptions>>) -> Result<()> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let drop = DropCollection::new(self.namespace(), options);
        self.client().execute_operation(drop).await
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

        let aggregate = Aggregate::new(self.namespace(), pipeline, options);
        let client = self.client();
        client
            .execute_cursor_operation(aggregate)
            .await
            .map(|(spec, session)| Cursor::new(client.clone(), spec, session))
    }

    /// Estimates the number of documents in the collection using collection metadata.
    pub async fn estimated_document_count(
        &self,
        options: impl Into<Option<EstimatedDocumentCountOptions>>,
    ) -> Result<i64> {
        let mut options = options.into();
        resolve_options!(self, options, [read_concern, selection_criteria]);

        let op = Count::new(self.namespace(), options);
        self.client().execute_operation(op).await
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
        let options = options.into();

        let mut pipeline = vec![doc! {
            "$match": filter.into().unwrap_or_default(),
        }];

        if let Some(skip) = options.as_ref().and_then(|opts| opts.skip) {
            pipeline.push(doc! {
                "$skip": skip
            });
        }

        if let Some(limit) = options.as_ref().and_then(|opts| opts.limit) {
            pipeline.push(doc! {
                "$limit": limit
            });
        }

        pipeline.push(doc! {
            "$group": {
                "_id": 1,
                "n": { "$sum": 1 },
            }
        });

        let aggregate_options = options.map(|opts| {
            AggregateOptions::builder()
                .hint(opts.hint)
                .max_time(opts.max_time)
                .collation(opts.collation)
                .build()
        });

        let result = match self
            .aggregate(pipeline, aggregate_options)
            .await?
            .next()
            .await
        {
            Some(doc) => doc?,
            None => return Ok(0),
        };

        let n = match result.get("n") {
            Some(n) => n,
            None => {
                return Err(ErrorKind::ResponseError {
                    message: "server response to count_documents aggregate did not contain the \
                              'n' field"
                        .into(),
                }
                .into())
            }
        };

        bson_util::get_int(n).ok_or_else(|| {
            ErrorKind::ResponseError {
                message: format!(
                    "server response to count_documents aggregate should have contained integer \
                     'n', but instead had {:?}",
                    n
                ),
            }
            .into()
        })
    }

    /// Deletes all documents stored in the collection matching `query`.
    pub async fn delete_many(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
    ) -> Result<DeleteResult> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let delete = Delete::new(self.namespace(), query, None, options);
        self.client().execute_operation(delete).await
    }

    /// Deletes up to one document found matching `query`.
    pub async fn delete_one(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
    ) -> Result<DeleteResult> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let delete = Delete::new(self.namespace(), query, Some(1), options);
        self.client().execute_operation(delete).await
    }

    /// Finds the distinct values of the field specified by `field_name` across the collection.
    pub async fn distinct(
        &self,
        field_name: &str,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<DistinctOptions>>,
    ) -> Result<Vec<Bson>> {
        let mut options = options.into();
        resolve_options!(self, options, [read_concern, selection_criteria]);

        let op = Distinct::new(
            self.namespace(),
            field_name.to_string(),
            filter.into(),
            options,
        );
        self.client().execute_operation(op).await
    }

    /// Finds the documents in the collection matching `filter`.
    pub async fn find(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOptions>>,
    ) -> Result<Cursor> {
        let find = Find::new(self.namespace(), filter.into(), options.into());
        let client = self.client();

        client
            .execute_cursor_operation(find)
            .await
            .map(|(result, session)| Cursor::new(client.clone(), result, session))
    }

    /// Finds a single document in the collection matching `filter`.
    pub async fn find_one(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOneOptions>>,
    ) -> Result<Option<Document>> {
        let mut options: FindOptions = options
            .into()
            .map(Into::into)
            .unwrap_or_else(Default::default);
        options.limit = Some(-1);
        let mut cursor = self.find(filter, Some(options)).await?;
        cursor.next().await.transpose()
    }

    /// Atomically finds up to one document in the collection matching `filter` and deletes it.
    pub async fn find_one_and_delete(
        &self,
        filter: Document,
        options: impl Into<Option<FindOneAndDeleteOptions>>,
    ) -> Result<Option<Document>> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let op = FindAndModify::with_delete(self.namespace(), filter, options);
        self.client().execute_operation(op).await
    }

    /// Atomically finds up to one document in the collection matching `filter` and replaces it with
    /// `replacement`.
    pub async fn find_one_and_replace(
        &self,
        filter: Document,
        replacement: Document,
        options: impl Into<Option<FindOneAndReplaceOptions>>,
    ) -> Result<Option<Document>> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let op = FindAndModify::with_replace(self.namespace(), filter, replacement, options)?;
        self.client().execute_operation(op).await
    }

    /// Atomically finds up to one document in the collection matching `filter` and updates it.
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+.
    pub async fn find_one_and_update(
        &self,
        filter: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<FindOneAndUpdateOptions>>,
    ) -> Result<Option<Document>> {
        let update = update.into();
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let op = FindAndModify::with_update(self.namespace(), filter, update, options)?;
        self.client().execute_operation(op).await
    }

    /// Inserts the documents in `docs` into the collection.
    pub async fn insert_many(
        &self,
        docs: impl IntoIterator<Item = Document>,
        options: impl Into<Option<InsertManyOptions>>,
    ) -> Result<InsertManyResult> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let mut docs: Vec<Document> = docs.into_iter().collect();

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
            match self.client().execute_operation(insert).await {
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
                Err(e) => match e.kind.as_ref() {
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

    /// Inserts `doc` into the collection.
    pub async fn insert_one(
        &self,
        doc: Document,
        options: impl Into<Option<InsertOneOptions>>,
    ) -> Result<InsertOneResult> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let insert = Insert::new(
            self.namespace(),
            vec![doc],
            options.map(InsertManyOptions::from_insert_one_options),
        );
        self.client()
            .execute_operation(insert)
            .await
            .map(InsertOneResult::from_insert_many_result)
            .map_err(convert_bulk_errors)
    }

    /// Replaces up to one document matching `query` in the collection with `replacement`.
    pub async fn replace_one(
        &self,
        query: Document,
        replacement: Document,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> Result<UpdateResult> {
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
        self.client().execute_operation(update).await
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
        let update = update.into();
        let mut options = options.into();

        if let UpdateModifications::Document(ref d) = update {
            bson_util::update_document_check(d)?;
        };

        resolve_options!(self, options, [write_concern]);

        let update = Update::new(self.namespace(), query, update, true, options);
        self.client().execute_operation(update).await
    }

    /// Updates up to one document matching `query` in the collection.
    ///
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://docs.mongodb.com/manual/reference/command/update/#behavior) for more information on specifying updates.
    pub async fn update_one(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
    ) -> Result<UpdateResult> {
        let mut options = options.into();
        resolve_options!(self, options, [write_concern]);

        let update = Update::new(self.namespace(), query, update.into(), false, options);
        self.client().execute_operation(update).await
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
    /// Starts a new [`ChangeStream`](change_stream/struct.ChangeStream.html) that receives events
    /// for all changes in this collection. A
    /// [`ChangeStream`](change_stream/struct.ChangeStream.html) cannot be started on system
    /// collections.
    ///
    /// See the documentation [here](https://docs.mongodb.com/manual/changeStreams/) on change
    /// streams.
    ///
    /// Change streams require either a "majority" read concern or no read concern. Anything else
    /// will cause a server error.
    ///
    /// Also note that using a `$project` stage to remove any of the `_id`, `operationType` or `ns`
    /// fields will cause an error. The driver requires these fields to support resumability. For
    /// more information on resumability, see the documentation for
    /// [`ChangeStream`](change_stream/struct.ChangeStream.html)
    #[allow(unused)]
    pub async fn watch(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<ChangeStreamOptions>>,
    ) -> Result<ChangeStream> {
        let mut options = options.into();
        resolve_options!(self, options, [read_concern, selection_criteria]);
        let client = self.client();
        client
            .start_change_stream(
                pipeline,
                options,
                ChangeStreamTarget::Collection(self.namespace()),
            )
            .await
    }
}

/// A struct modeling the canonical name for a collection in MongoDB.
#[derive(Debug, Clone, Deserialize, PartialEq)]
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

    pub(crate) fn deserialize_ns<'de, D>(deserializer: D) -> std::result::Result<Self, D::Error>
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

impl fmt::Display for Namespace {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}.{}", self.db, self.coll)
    }
}
