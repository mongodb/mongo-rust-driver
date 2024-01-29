pub mod options;

use std::{borrow::Borrow, collections::HashSet, fmt, fmt::Debug, str::FromStr, sync::Arc};

use futures_util::{
    future,
    stream::{StreamExt, TryStreamExt},
};
use serde::{
    de::{DeserializeOwned, Error as DeError},
    Deserialize,
    Deserializer,
    Serialize,
};

use self::options::*;
use crate::{
    bson::{doc, Bson, Document},
    bson_util,
    client::options::ServerAddress,
    cmap::conn::PinnedConnectionHandle,
    concern::{ReadConcern, WriteConcern},
    error::{convert_bulk_errors, BulkWriteError, BulkWriteFailure, Error, ErrorKind, Result},
    index::IndexModel,
    operation::{
        Aggregate,
        Count,
        CountDocuments,
        CreateIndexes,
        Delete,
        Distinct,
        DropCollection,
        DropIndexes,
        Find,
        FindAndModify,
        Insert,
        ListIndexes,
        Update,
    },
    results::{
        CreateIndexResult,
        CreateIndexesResult,
        DeleteResult,
        InsertManyResult,
        InsertOneResult,
        UpdateResult,
    },
    selection_criteria::SelectionCriteria,
    Client,
    ClientSession,
    Cursor,
    Database,
    SessionCursor,
};

/// `Collection` is the client-side abstraction of a MongoDB Collection. It can be used to
/// perform collection-level operations such as CRUD operations. A `Collection` can be obtained
/// through a [`Database`](struct.Database.html) by calling either
/// [`Database::collection`](struct.Database.html#method.collection) or
/// [`Database::collection_with_options`](struct.Database.html#method.collection_with_options).
///
/// A [`Collection`] can be parameterized with any type that implements the
/// `Serialize` and `Deserialize` traits from the [`serde`](https://serde.rs/) crate. This includes but
/// is not limited to just `Document`. The various methods that accept or return instances of the
/// documents in the collection will accept/return instances of the generic parameter (e.g.
/// [`Collection::insert_one`] accepts it as an argument, [`Collection::find_one`] returns an
/// `Option` of it). It is recommended to define types that model your data which you can
/// parameterize your [`Collection`]s with instead of `Document`, since doing so eliminates a lot of
/// boilerplate deserialization code and is often more performant.
///
/// `Collection` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads or async tasks.
///
/// # Example
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
/// # #[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
/// # async fn start_workers() -> Result<()> {
/// # use mongodb::Client;
/// #
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// use serde::{Deserialize, Serialize};
///
/// /// Define a type that models our data.
/// #[derive(Clone, Debug, Deserialize, Serialize)]
/// struct Item {
///     id: u32,
/// }
///
/// // Parameterize our collection with the model.
/// let coll = client.database("items").collection::<Item>("in_stock");
///
/// for i in 0..5 {
///     let coll_ref = coll.clone();
///
///     // Spawn several tasks that operate on the same collection concurrently.
///     task::spawn(async move {
///         // Perform operations with `coll_ref` that work with directly our model.
///         coll_ref.insert_one(Item { id: i }, None).await;
///     });
/// }
/// #
/// # Ok(())
/// # }
/// ```

#[derive(Debug)]
pub struct Collection<T> {
    inner: Arc<CollectionInner>,
    _phantom: std::marker::PhantomData<T>,
}

// Because derive is too conservative, derive only implements Clone if T is Clone.
// Collection<T> does not actually store any value of type T (so T does not need to be clone).
impl<T> Clone for Collection<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
struct CollectionInner {
    client: Client,
    db: Database,
    name: String,
    selection_criteria: Option<SelectionCriteria>,
    read_concern: Option<ReadConcern>,
    write_concern: Option<WriteConcern>,
    human_readable_serialization: bool,
}

impl<T> Collection<T> {
    pub(crate) fn new(db: Database, name: &str, options: Option<CollectionOptions>) -> Self {
        let options = options.unwrap_or_default();
        let selection_criteria = options
            .selection_criteria
            .or_else(|| db.selection_criteria().cloned());

        let read_concern = options.read_concern.or_else(|| db.read_concern().cloned());

        let write_concern = options
            .write_concern
            .or_else(|| db.write_concern().cloned());
        #[allow(deprecated)]
        let human_readable_serialization = options.human_readable_serialization.unwrap_or_default();

        Self {
            inner: Arc::new(CollectionInner {
                client: db.client().clone(),
                db,
                name: name.to_string(),
                selection_criteria,
                read_concern,
                write_concern,
                human_readable_serialization,
            }),
            _phantom: Default::default(),
        }
    }

    /// Gets a clone of the `Collection` with a different type `U`.
    pub fn clone_with_type<U>(&self) -> Collection<U> {
        Collection {
            inner: self.inner.clone(),
            _phantom: Default::default(),
        }
    }

    pub(crate) fn clone_unconcerned(&self) -> Self {
        let mut new_inner = CollectionInner::clone(&self.inner);
        new_inner.write_concern = None;
        new_inner.read_concern = None;
        Self {
            inner: Arc::new(new_inner),
            _phantom: Default::default(),
        }
    }

    /// Get the `Client` that this collection descended from.
    pub fn client(&self) -> &Client {
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

    #[allow(clippy::needless_option_as_deref)]
    async fn drop_common(
        &self,
        options: impl Into<Option<DropCollectionOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<()> {
        let mut session = session.into();

        let mut options: Option<DropCollectionOptions> = options.into();
        resolve_options!(self, options, [write_concern]);

        #[cfg(feature = "in-use-encryption-unstable")]
        self.drop_aux_collections(options.as_ref(), session.as_deref_mut())
            .await?;

        let drop = DropCollection::new(self.namespace(), options);
        self.client()
            .execute_operation(drop, session.as_deref_mut())
            .await
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

    #[cfg(feature = "in-use-encryption-unstable")]
    #[allow(clippy::needless_option_as_deref)]
    async fn drop_aux_collections(
        &self,
        options: Option<&DropCollectionOptions>,
        mut session: Option<&mut ClientSession>,
    ) -> Result<()> {
        // Find associated `encrypted_fields`:
        // * from options to this call
        let mut enc_fields = options.and_then(|o| o.encrypted_fields.as_ref());
        let enc_opts = self.client().auto_encryption_opts().await;
        // * from client-wide `encrypted_fields_map`:
        let client_enc_fields = enc_opts
            .as_ref()
            .and_then(|eo| eo.encrypted_fields_map.as_ref());
        if enc_fields.is_none() {
            enc_fields =
                client_enc_fields.and_then(|efm| efm.get(&format!("{}", self.namespace())));
        }
        // * from a `list_collections` call:
        let found;
        if enc_fields.is_none() && client_enc_fields.is_some() {
            let filter = doc! { "name": self.name() };
            let mut specs: Vec<_> = match session.as_deref_mut() {
                Some(s) => {
                    let mut cursor = self
                        .inner
                        .db
                        .list_collections().filter(filter).session(&mut *s)
                        .await?;
                    cursor.stream(s).try_collect().await?
                }
                None => {
                    self.inner
                        .db
                        .list_collections().filter(filter)
                        .await?
                        .try_collect()
                        .await?
                }
            };
            if let Some(spec) = specs.pop() {
                if let Some(enc) = spec.options.encrypted_fields {
                    found = enc;
                    enc_fields = Some(&found);
                }
            }
        }

        // Drop the collections.
        if let Some(enc_fields) = enc_fields {
            for ns in crate::client::csfle::aux_collections(&self.namespace(), enc_fields)? {
                let drop = DropCollection::new(ns, options.cloned());
                self.client()
                    .execute_operation(drop, session.as_deref_mut())
                    .await?;
            }
        }
        Ok(())
    }

    /// Runs an aggregation operation.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/aggregation/) for more
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
        client.execute_cursor_operation(aggregate).await
    }

    /// Runs an aggregation operation using the provided `ClientSession`.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/aggregation/) for more
    /// information on aggregations.
    pub async fn aggregate_with_session(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<AggregateOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<Document>> {
        let mut options = options.into();
        resolve_read_concern_with_session!(self, options, Some(&mut *session))?;
        resolve_write_concern_with_session!(self, options, Some(&mut *session))?;
        resolve_selection_criteria_with_session!(self, options, Some(&mut *session))?;

        let aggregate = Aggregate::new(self.namespace(), pipeline, options);
        let client = self.client();
        client
            .execute_session_cursor_operation(aggregate, session)
            .await
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
    pub async fn estimated_document_count(
        &self,
        options: impl Into<Option<EstimatedDocumentCountOptions>>,
    ) -> Result<u64> {
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
    ) -> Result<u64> {
        let session = session.into();

        let mut options = options.into();
        resolve_read_concern_with_session!(self, options, session.as_ref())?;
        resolve_selection_criteria_with_session!(self, options, session.as_ref())?;

        let op = CountDocuments::new(self.namespace(), filter.into(), options)?;
        self.client().execute_operation(op, session).await
    }

    /// Gets the number of documents matching `filter`.
    ///
    /// Note that this method returns an accurate count.
    pub async fn count_documents(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<CountOptions>>,
    ) -> Result<u64> {
        self.count_documents_common(filter, options, None).await
    }

    /// Gets the number of documents matching `filter` using the provided `ClientSession`.
    ///
    /// Note that this method returns an accurate count.
    pub async fn count_documents_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<CountOptions>>,
        session: &mut ClientSession,
    ) -> Result<u64> {
        self.count_documents_common(filter, options, session).await
    }

    async fn delete_many_common(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<DeleteResult> {
        let session = session.into();

        let mut options = options.into();
        resolve_write_concern_with_session!(self, options, session.as_ref())?;

        let delete = Delete::new(self.namespace(), query, None, options);
        self.client().execute_operation(delete, session).await
    }

    async fn create_indexes_common(
        &self,
        indexes: impl IntoIterator<Item = IndexModel>,
        options: impl Into<Option<CreateIndexOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<CreateIndexesResult> {
        let session = session.into();

        let mut options = options.into();
        resolve_write_concern_with_session!(self, options, session.as_ref())?;

        let indexes: Vec<IndexModel> = indexes.into_iter().collect();

        let create_indexes = CreateIndexes::new(self.namespace(), indexes, options);
        self.client()
            .execute_operation(create_indexes, session)
            .await
    }

    pub(crate) async fn create_index_common(
        &self,
        index: IndexModel,
        options: impl Into<Option<CreateIndexOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<CreateIndexResult> {
        let response = self
            .create_indexes_common(vec![index], options, session)
            .await?;
        Ok(response.into_create_index_result())
    }

    /// Creates the given index on this collection.
    pub async fn create_index(
        &self,
        index: IndexModel,
        options: impl Into<Option<CreateIndexOptions>>,
    ) -> Result<CreateIndexResult> {
        self.create_index_common(index, options, None).await
    }

    /// Creates the given index on this collection using the provided `ClientSession`.
    pub async fn create_index_with_session(
        &self,
        index: IndexModel,
        options: impl Into<Option<CreateIndexOptions>>,
        session: &mut ClientSession,
    ) -> Result<CreateIndexResult> {
        self.create_index_common(index, options, session).await
    }

    /// Creates the given indexes on this collection.
    pub async fn create_indexes(
        &self,
        indexes: impl IntoIterator<Item = IndexModel>,
        options: impl Into<Option<CreateIndexOptions>>,
    ) -> Result<CreateIndexesResult> {
        self.create_indexes_common(indexes, options, None).await
    }

    /// Creates the given indexes on this collection using the provided `ClientSession`.
    pub async fn create_indexes_with_session(
        &self,
        indexes: impl IntoIterator<Item = IndexModel>,
        options: impl Into<Option<CreateIndexOptions>>,
        session: &mut ClientSession,
    ) -> Result<CreateIndexesResult> {
        self.create_indexes_common(indexes, options, session).await
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
        let session = session.into();

        let mut options = options.into();
        resolve_write_concern_with_session!(self, options, session.as_ref())?;

        let delete = Delete::new(self.namespace(), query, Some(1), options);
        self.client().execute_operation(delete, session).await
    }

    /// Deletes up to one document found matching `query`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
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
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
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
        field_name: impl AsRef<str>,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<DistinctOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<Vec<Bson>> {
        let session = session.into();

        let mut options = options.into();
        resolve_read_concern_with_session!(self, options, session.as_ref())?;
        resolve_selection_criteria_with_session!(self, options, session.as_ref())?;

        let op = Distinct::new(
            self.namespace(),
            field_name.as_ref().to_string(),
            filter.into(),
            options,
        );
        self.client().execute_operation(op, session).await
    }

    /// Finds the distinct values of the field specified by `field_name` across the collection.
    pub async fn distinct(
        &self,
        field_name: impl AsRef<str>,
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
        field_name: impl AsRef<str>,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<DistinctOptions>>,
        session: &mut ClientSession,
    ) -> Result<Vec<Bson>> {
        self.distinct_common(field_name, filter, options, session)
            .await
    }

    async fn drop_indexes_common(
        &self,
        name: impl Into<Option<&str>>,
        options: impl Into<Option<DropIndexOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<()> {
        let session = session.into();

        let mut options = options.into();
        resolve_write_concern_with_session!(self, options, session.as_ref())?;

        // If there is no provided name, that means we should drop all indexes.
        let index_name = name.into().unwrap_or("*").to_string();

        let drop_index = DropIndexes::new(self.namespace(), index_name, options);
        self.client().execute_operation(drop_index, session).await
    }

    async fn drop_index_common(
        &self,
        name: impl AsRef<str>,
        options: impl Into<Option<DropIndexOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<()> {
        let name = name.as_ref();
        if name == "*" {
            return Err(ErrorKind::InvalidArgument {
                message: "Cannot pass name \"*\" to drop_index since more than one index would be \
                          dropped."
                    .to_string(),
            }
            .into());
        }
        self.drop_indexes_common(name, options, session).await
    }

    /// Drops the index specified by `name` from this collection.
    pub async fn drop_index(
        &self,
        name: impl AsRef<str>,
        options: impl Into<Option<DropIndexOptions>>,
    ) -> Result<()> {
        self.drop_index_common(name, options, None).await
    }

    /// Drops the index specified by `name` from this collection using the provided `ClientSession`.
    pub async fn drop_index_with_session(
        &self,
        name: impl AsRef<str>,
        options: impl Into<Option<DropIndexOptions>>,
        session: &mut ClientSession,
    ) -> Result<()> {
        self.drop_index_common(name, options, session).await
    }

    /// Drops all indexes associated with this collection.
    pub async fn drop_indexes(&self, options: impl Into<Option<DropIndexOptions>>) -> Result<()> {
        self.drop_indexes_common(None, options, None).await
    }

    /// Drops all indexes associated with this collection using the provided `ClientSession`.
    pub async fn drop_indexes_with_session(
        &self,
        options: impl Into<Option<DropIndexOptions>>,
        session: &mut ClientSession,
    ) -> Result<()> {
        self.drop_indexes_common(None, options, session).await
    }

    /// Lists all indexes on this collection.
    pub async fn list_indexes(
        &self,
        options: impl Into<Option<ListIndexesOptions>>,
    ) -> Result<Cursor<IndexModel>> {
        let list_indexes = ListIndexes::new(self.namespace(), options.into());
        let client = self.client();
        client.execute_cursor_operation(list_indexes).await
    }

    /// Lists all indexes on this collection using the provided `ClientSession`.
    pub async fn list_indexes_with_session(
        &self,
        options: impl Into<Option<ListIndexesOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<IndexModel>> {
        let list_indexes = ListIndexes::new(self.namespace(), options.into());
        let client = self.client();
        client
            .execute_session_cursor_operation(list_indexes, session)
            .await
    }

    async fn list_index_names_common(
        &self,
        cursor: impl TryStreamExt<Ok = IndexModel, Error = Error>,
    ) -> Result<Vec<String>> {
        cursor
            .try_filter_map(|index| future::ok(index.get_name()))
            .try_collect()
            .await
    }

    /// Gets the names of all indexes on the collection.
    pub async fn list_index_names(&self) -> Result<Vec<String>> {
        let cursor = self.list_indexes(None).await?;
        self.list_index_names_common(cursor).await
    }

    /// Gets the names of all indexes on the collection using the provided `ClientSession`.
    pub async fn list_index_names_with_session(
        &self,
        session: &mut ClientSession,
    ) -> Result<Vec<String>> {
        let mut cursor = self.list_indexes_with_session(None, session).await?;
        self.list_index_names_common(cursor.stream(session)).await
    }

    async fn update_many_common(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<UpdateResult> {
        let update = update.into();

        if let UpdateModifications::Document(ref d) = update {
            bson_util::update_document_check(d)?;
        }

        let session = session.into();

        let mut options = options.into();
        resolve_write_concern_with_session!(self, options, session.as_ref())?;

        let update = Update::with_update(
            self.namespace(),
            query,
            update,
            true,
            options,
            self.inner.human_readable_serialization,
        );
        self.client().execute_operation(update, session).await
    }

    /// Updates all documents matching `query` in the collection.
    ///
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://www.mongodb.com/docs/manual/reference/command/update/#behavior) for more information on specifying updates.
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
    /// [documentation](https://www.mongodb.com/docs/manual/reference/command/update/#behavior) for more information on specifying updates.
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
        let update = update.into();
        if let UpdateModifications::Document(ref d) = update {
            bson_util::update_document_check(d)?;
        }

        let session = session.into();

        let mut options = options.into();
        resolve_write_concern_with_session!(self, options, session.as_ref())?;

        let update = Update::with_update(
            self.namespace(),
            query,
            update,
            false,
            options,
            self.inner.human_readable_serialization,
        );
        self.client().execute_operation(update, session).await
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
    /// [documentation](https://www.mongodb.com/docs/manual/reference/command/update/#behavior) for more information on specifying updates.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
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
    pub(super) async fn kill_cursor(
        &self,
        cursor_id: i64,
        pinned_connection: Option<&PinnedConnectionHandle>,
        drop_address: Option<ServerAddress>,
    ) -> Result<()> {
        let ns = self.namespace();

        self.client()
            .database(ns.db.as_str())
            .run_command_common(
                doc! {
                    "killCursors": ns.coll.as_str(),
                    "cursors": [cursor_id]
                },
                drop_address.map(SelectionCriteria::from_address),
                None,
                pinned_connection,
            )
            .await?;
        Ok(())
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

        client.execute_cursor_operation(find).await
    }

    /// Finds the documents in the collection matching `filter` using the provided `ClientSession`.
    pub async fn find_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<T>> {
        let mut options = options.into();
        resolve_read_concern_with_session!(self, options, Some(&mut *session))?;
        resolve_selection_criteria_with_session!(self, options, Some(&mut *session))?;

        let find = Find::new(self.namespace(), filter.into(), options);
        let client = self.client();

        client.execute_session_cursor_operation(find, session).await
    }
}

impl<T> Collection<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
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
        resolve_read_concern_with_session!(self, options, Some(&mut *session))?;
        resolve_selection_criteria_with_session!(self, options, Some(&mut *session))?;

        let options: FindOptions = options.map(Into::into).unwrap_or_else(Default::default);
        let mut cursor = self
            .find_with_session(filter, Some(options), session)
            .await?;
        let mut cursor = cursor.stream(session);
        cursor.next().await.transpose()
    }
}

impl<T> Collection<T>
where
    T: DeserializeOwned,
{
    async fn find_one_and_delete_common(
        &self,
        filter: Document,
        options: impl Into<Option<FindOneAndDeleteOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<Option<T>> {
        let session = session.into();

        let mut options = options.into();
        resolve_write_concern_with_session!(self, options, session.as_ref())?;

        let op = FindAndModify::with_delete(self.namespace(), filter, options);
        self.client().execute_operation(op, session).await
    }

    /// Atomically finds up to one document in the collection matching `filter` and deletes it.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
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
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
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

    async fn find_one_and_update_common(
        &self,
        filter: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<FindOneAndUpdateOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<Option<T>> {
        let update = update.into();

        let session = session.into();

        let mut options = options.into();
        resolve_write_concern_with_session!(self, options, session.as_ref())?;

        let op = FindAndModify::with_update(self.namespace(), filter, update, options)?;
        self.client().execute_operation(op, session).await
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
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
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
}

impl<T> Collection<T>
where
    T: Serialize + DeserializeOwned,
{
    async fn find_one_and_replace_common(
        &self,
        filter: Document,
        replacement: impl Borrow<T>,
        options: impl Into<Option<FindOneAndReplaceOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<Option<T>> {
        let mut options = options.into();
        let session = session.into();
        resolve_write_concern_with_session!(self, options, session.as_ref())?;

        let op = FindAndModify::with_replace(
            self.namespace(),
            filter,
            replacement.borrow(),
            options,
            self.inner.human_readable_serialization,
        )?;
        self.client().execute_operation(op, session).await
    }

    /// Atomically finds up to one document in the collection matching `filter` and replaces it with
    /// `replacement`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn find_one_and_replace(
        &self,
        filter: Document,
        replacement: impl Borrow<T>,
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
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn find_one_and_replace_with_session(
        &self,
        filter: Document,
        replacement: impl Borrow<T>,
        options: impl Into<Option<FindOneAndReplaceOptions>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        self.find_one_and_replace_common(filter, replacement, options, session)
            .await
    }
}

impl<T> Collection<T>
where
    T: Serialize,
{
    #[allow(clippy::needless_option_as_deref)]
    async fn insert_many_common(
        &self,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        options: impl Into<Option<InsertManyOptions>>,
        mut session: Option<&mut ClientSession>,
    ) -> Result<InsertManyResult> {
        let ds: Vec<_> = docs.into_iter().collect();
        let mut options = options.into();
        resolve_write_concern_with_session!(self, options, session.as_ref())?;

        if ds.is_empty() {
            return Err(ErrorKind::InvalidArgument {
                message: "No documents provided to insert_many".to_string(),
            }
            .into());
        }

        let ordered = options.as_ref().and_then(|o| o.ordered).unwrap_or(true);
        #[cfg(feature = "in-use-encryption-unstable")]
        let encrypted = self.client().auto_encryption_opts().await.is_some();
        #[cfg(not(feature = "in-use-encryption-unstable"))]
        let encrypted = false;

        let mut cumulative_failure: Option<BulkWriteFailure> = None;
        let mut error_labels: HashSet<String> = Default::default();
        let mut cumulative_result: Option<InsertManyResult> = None;

        let mut n_attempted = 0;

        while n_attempted < ds.len() {
            let docs: Vec<&T> = ds.iter().skip(n_attempted).map(Borrow::borrow).collect();
            let insert = Insert::new(
                self.namespace(),
                docs,
                options.clone(),
                encrypted,
                self.inner.human_readable_serialization,
            );

            match self
                .client()
                .execute_operation(insert, session.as_deref_mut())
                .await
            {
                Ok(result) => {
                    let current_batch_size = result.inserted_ids.len();

                    let cumulative_result =
                        cumulative_result.get_or_insert_with(InsertManyResult::new);
                    for (index, id) in result.inserted_ids {
                        cumulative_result
                            .inserted_ids
                            .insert(index + n_attempted, id);
                    }

                    n_attempted += current_batch_size;
                }
                Err(e) => {
                    let labels = e.labels().clone();
                    match *e.kind {
                        ErrorKind::BulkWrite(bw) => {
                            // for ordered inserts this size will be incorrect, but knowing the
                            // batch size isn't needed for ordered
                            // failures since we return immediately from
                            // them anyways.
                            let current_batch_size = bw.inserted_ids.len()
                                + bw.write_errors.as_ref().map(|we| we.len()).unwrap_or(0);

                            let failure_ref =
                                cumulative_failure.get_or_insert_with(BulkWriteFailure::new);
                            if let Some(write_errors) = bw.write_errors {
                                for err in write_errors {
                                    let index = n_attempted + err.index;

                                    failure_ref
                                        .write_errors
                                        .get_or_insert_with(Default::default)
                                        .push(BulkWriteError { index, ..err });
                                }
                            }

                            if let Some(wc_error) = bw.write_concern_error {
                                failure_ref.write_concern_error = Some(wc_error);
                            }

                            error_labels.extend(labels);

                            if ordered {
                                // this will always be true since we invoked get_or_insert_with
                                // above.
                                if let Some(failure) = cumulative_failure {
                                    return Err(Error::new(
                                        ErrorKind::BulkWrite(failure),
                                        Some(error_labels),
                                    ));
                                }
                            }
                            n_attempted += current_batch_size;
                        }
                        _ => return Err(e),
                    }
                }
            }
        }

        match cumulative_failure {
            Some(failure) => Err(Error::new(
                ErrorKind::BulkWrite(failure),
                Some(error_labels),
            )),
            None => Ok(cumulative_result.unwrap_or_else(InsertManyResult::new)),
        }
    }

    /// Inserts the data in `docs` into the collection.
    ///
    /// Note that this method accepts both owned and borrowed values, so the input documents
    /// do not need to be cloned in order to be passed in.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn insert_many(
        &self,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        options: impl Into<Option<InsertManyOptions>>,
    ) -> Result<InsertManyResult> {
        self.insert_many_common(docs, options, None).await
    }

    /// Inserts the data in `docs` into the collection using the provided `ClientSession`.
    ///
    /// Note that this method accepts both owned and borrowed values, so the input documents
    /// do not need to be cloned in order to be passed in.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn insert_many_with_session(
        &self,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        options: impl Into<Option<InsertManyOptions>>,
        session: &mut ClientSession,
    ) -> Result<InsertManyResult> {
        self.insert_many_common(docs, options, Some(session)).await
    }

    async fn insert_one_common(
        &self,
        doc: &T,
        options: impl Into<Option<InsertOneOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<InsertOneResult> {
        let session = session.into();

        let mut options = options.into();
        resolve_write_concern_with_session!(self, options, session.as_ref())?;

        #[cfg(feature = "in-use-encryption-unstable")]
        let encrypted = self.client().auto_encryption_opts().await.is_some();
        #[cfg(not(feature = "in-use-encryption-unstable"))]
        let encrypted = false;

        let insert = Insert::new(
            self.namespace(),
            vec![doc],
            options.map(InsertManyOptions::from_insert_one_options),
            encrypted,
            self.inner.human_readable_serialization,
        );
        self.client()
            .execute_operation(insert, session)
            .await
            .map(InsertOneResult::from_insert_many_result)
            .map_err(convert_bulk_errors)
    }

    /// Inserts `doc` into the collection.
    ///
    /// Note that either an owned or borrowed value can be inserted here, so the input document
    /// does not need to be cloned to be passed in.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn insert_one(
        &self,
        doc: impl Borrow<T>,
        options: impl Into<Option<InsertOneOptions>>,
    ) -> Result<InsertOneResult> {
        self.insert_one_common(doc.borrow(), options, None).await
    }

    /// Inserts `doc` into the collection using the provided `ClientSession`.
    ///
    /// Note that either an owned or borrowed value can be inserted here, so the input document
    /// does not need to be cloned to be passed in.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn insert_one_with_session(
        &self,
        doc: impl Borrow<T>,
        options: impl Into<Option<InsertOneOptions>>,
        session: &mut ClientSession,
    ) -> Result<InsertOneResult> {
        self.insert_one_common(doc.borrow(), options, session).await
    }

    async fn replace_one_common(
        &self,
        query: Document,
        replacement: impl Borrow<T>,
        options: impl Into<Option<ReplaceOptions>>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<UpdateResult> {
        let mut options = options.into();

        let session = session.into();

        resolve_write_concern_with_session!(self, options, session.as_ref())?;

        let update = Update::with_replace(
            self.namespace(),
            query,
            replacement.borrow(),
            false,
            options.map(UpdateOptions::from_replace_options),
            self.inner.human_readable_serialization,
        );
        self.client().execute_operation(update, session).await
    }

    /// Replaces up to one document matching `query` in the collection with `replacement`.
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn replace_one(
        &self,
        query: Document,
        replacement: impl Borrow<T>,
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
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn replace_one_with_session(
        &self,
        query: Document,
        replacement: impl Borrow<T>,
        options: impl Into<Option<ReplaceOptions>>,
        session: &mut ClientSession,
    ) -> Result<UpdateResult> {
        self.replace_one_common(query, replacement, options, session)
            .await
    }
}

/// A struct modeling the canonical name for a collection in MongoDB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    /// The name of the database associated with this namespace.
    pub db: String,

    /// The name of the collection this namespace corresponds to.
    pub coll: String,
}

impl Namespace {
    /// Construct a `Namespace` with the given database and collection.
    pub fn new(db: impl Into<String>, coll: impl Into<String>) -> Self {
        Self {
            db: db.into(),
            coll: coll.into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self {
            db: String::new(),
            coll: String::new(),
        }
    }

    pub(crate) fn from_str(s: &str) -> Option<Self> {
        let mut parts = s.split('.');

        let db = parts.next();
        let coll = parts.collect::<Vec<_>>().join(".");

        match (db, coll) {
            (Some(db), coll) if !coll.is_empty() => Some(Self {
                db: db.to_string(),
                coll,
            }),
            _ => None,
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
        Self::from_str(&s)
            .ok_or_else(|| D::Error::custom("Missing one or more fields in namespace"))
    }
}

impl Serialize for Namespace {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&(self.db.clone() + "." + &self.coll))
    }
}

impl FromStr for Namespace {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut parts = s.split('.');

        let db = parts.next();
        let coll = parts.collect::<Vec<_>>().join(".");

        match (db, coll) {
            (Some(db), coll) if !coll.is_empty() => Ok(Self {
                db: db.to_string(),
                coll,
            }),
            _ => Err(Self::Err::invalid_argument(
                "Missing one or more fields in namespace",
            )),
        }
    }
}
