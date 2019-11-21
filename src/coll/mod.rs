pub mod options;

use std::{fmt, sync::Arc};

use bson::{Bson, Document};
use serde::{de::Error, Deserialize, Deserializer};

use self::options::*;
use crate::{
    bson_util,
    concern::{ReadConcern, WriteConcern},
    error::{convert_bulk_errors, Result},
    operation::{Count, Delete, Distinct, Find, DropCollection, Insert, Update},
    results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
    selection_criteria::SelectionCriteria,
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
/// # use bson::{bson, doc};
/// # use mongodb::{Client, error::Result};
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

    /// Drops the collection, deleting all data, users, and indexes stored in in.
    pub fn drop(&self, mut options: Option<DropCollectionOptions>) -> Result<()> {
        resolve_options!(self, options, [write_concern]);

        let drop = DropCollection::new(self.namespace(), options);
        self.client().execute_operation(&drop, None)
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

    /// Estimates the number of documents in the collection using collection metadata.
    pub fn estimated_document_count(
        &self,
        mut options: Option<EstimatedDocumentCountOptions>,
    ) -> Result<i64> {
        resolve_options!(self, options, [read_concern, selection_criteria]);

        let op = Count::new(self.namespace(), options);
        self.client().execute_operation(&op, None)
    }

    /// Gets the number of documents matching `filter`.
    ///
    /// Note that using `Collection::estimated_document_count` is recommended instead of this method
    /// is most cases.
    pub fn count_documents(
        &self,
        filter: Option<Document>,
        options: Option<CountOptions>,
    ) -> Result<i64> {
        unimplemented!()
    }

    /// Deletes all documents stored in the collection matching `query`.
    pub fn delete_many(
        &self,
        query: Document,
        mut options: Option<DeleteOptions>,
    ) -> Result<DeleteResult> {
        resolve_options!(self, options, [write_concern]);

        let delete = Delete::new(self.namespace(), query, None, options);
        self.client().execute_operation(&delete, None)
    }

    /// Deletes up to one document found matching `query`.
    pub fn delete_one(
        &self,
        query: Document,
        mut options: Option<DeleteOptions>,
    ) -> Result<DeleteResult> {
        resolve_options!(self, options, [write_concern]);

        let delete = Delete::new(self.namespace(), query, Some(1), options);
        self.client().execute_operation(&delete, None)
    }

    /// Finds the distinct values of the field specified by `field_name` across the collection.
    pub fn distinct(
        &self,
        field_name: &str,
        filter: Option<Document>,
        mut options: Option<DistinctOptions>,
    ) -> Result<Vec<Bson>> {
        resolve_options!(self, options, [read_concern, selection_criteria]);

        let op = Distinct::new(self.namespace(), field_name.to_string(), filter, options);
        self.client().execute_operation(&op, None)
    }

    /// Finds the documents in the collection matching `filter`.
    pub fn find(&self, filter: Option<Document>, options: Option<FindOptions>) -> Result<Cursor> {
        let find = Find::new(self.namespace(), filter, options);
        let client = self.client();
        client
            .execute_operation(&find, None)
            .map(|spec| Cursor::new(client.clone(), spec))
    }

    /// Finds a single document in the collection matching `filter`.
    pub fn find_one(
        &self,
        filter: Option<Document>,
        options: Option<FindOneOptions>,
    ) -> Result<Option<Document>> {
        let mut options: FindOptions = options.map(Into::into).unwrap_or_else(Default::default);
        options.limit = Some(-1);
        let mut cursor = self.find(filter, Some(options))?;
        cursor.next().transpose()
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

    /// Inserts the documents in `docs` into the collection.
    pub fn insert_many(
        &self,
        docs: impl IntoIterator<Item = Document>,
        mut options: Option<InsertManyOptions>,
    ) -> Result<InsertManyResult> {
        resolve_options!(self, options, [write_concern]);

        let insert = Insert::new(self.namespace(), docs.into_iter().collect(), options);
        self.client().execute_operation(&insert, None)
    }

    /// Inserts `doc` into the collection.
    pub fn insert_one(
        &self,
        doc: Document,
        mut options: Option<InsertOneOptions>,
    ) -> Result<InsertOneResult> {
        resolve_options!(self, options, [write_concern]);

        let insert = Insert::new(
            self.namespace(),
            vec![doc],
            options.map(InsertManyOptions::from_insert_one_options),
        );
        self.client()
            .execute_operation(&insert, None)
            .map(InsertOneResult::from_insert_many_result)
            .map_err(convert_bulk_errors)
    }

    /// Replaces up to one document matching `query` in the collection with `replacement`.
    pub fn replace_one(
        &self,
        query: Document,
        replacement: Document,
        mut options: Option<ReplaceOptions>,
    ) -> Result<UpdateResult> {
        bson_util::replacement_document_check(&replacement)?;
        resolve_options!(self, options, [write_concern]);

        let update = Update::new(
            self.namespace(),
            query,
            UpdateModifications::Document(replacement),
            false,
            options.map(UpdateOptions::from_replace_options),
        );
        self.client().execute_operation(&update, None)
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
        mut options: Option<UpdateOptions>,
    ) -> Result<UpdateResult> {
        let update = update.into();

        if let UpdateModifications::Document(ref d) = update {
            bson_util::update_document_check(d)?;
        };

        resolve_options!(self, options, [write_concern]);

        let update = Update::new(self.namespace(), query, update, true, options);
        self.client().execute_operation(&update, None)
    }

    /// Updates up to one document matching `query` in the collection.
    ///
    /// Both `Document` and `Vec<Document>` implement `Into<UpdateModifications>`, so either can be
    /// passed in place of constructing the enum case. Note: pipeline updates are only supported
    /// in MongoDB 4.2+. See the official MongoDB
    /// [documentation](https://docs.mongodb.com/manual/reference/command/update/#behavior) for more information on specifying updates.
    pub fn update_one(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        mut options: Option<UpdateOptions>,
    ) -> Result<UpdateResult> {
        resolve_options!(self, options, [write_concern]);

        let update = Update::new(self.namespace(), query, update.into(), false, options);
        self.client().execute_operation(&update, None)
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

/// A struct modeling the canonical name for a collection in MongoDB.
#[derive(Debug, Clone)]
pub struct Namespace {
    /// The name of the database associated with this namespace.
    pub db: String,

    /// The name of the collection this namespace corresponds to.
    pub coll: String,
}

impl Namespace {
    #[allow(dead_code)]
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
        let coll = parts.next();

        if parts.count() != 0 {
            return Err(D::Error::custom(
                "Expected a single dot in the namepsace, got more than one",
            ));
        }

        match (db, coll) {
            (Some(db), Some(coll)) => Ok(Self {
                db: db.to_string(),
                coll: coll.to_string(),
            }),
            _ => Err(D::Error::custom("Missing one or more fields in namespace")),
        }
    }
}
