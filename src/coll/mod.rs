mod batch;
pub mod options;

use std::sync::Arc;

use bson::{Bson, Document};

use self::options::*;
use crate::{
    bson_util,
    change_stream::{document::ChangeStreamToken, ChangeStream},
    command_responses::{
        CreateIndexesResponse, DeleteCommandResponse, DistinctCommandResponse,
        FindAndModifyCommandResponse, FindCommandResponse, GetMoreCommandResponse,
        UpdateCommandResponse,
    },
    concern::{ReadConcern, WriteConcern},
    error::{Error, ErrorKind, Result},
    options::ChangeStreamOptions,
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
    db: Database,
    name: String,
    read_preference: Option<ReadPreference>,
    read_concern: Option<ReadConcern>,
    write_concern: Option<WriteConcern>,
}

const MAX_INSERT_DOCS_BYTES: usize = 16 * 1000 * 1000;

impl Collection {
    pub(crate) fn new(db: Database, name: &str, options: Option<CollectionOptions>) -> Self {
        let options = options.unwrap_or_default();
        let read_preference = options
            .read_preference
            .or_else(|| db.read_preference().cloned());

        let read_concern = options.read_concern.or_else(|| db.read_concern().cloned());

        let write_concern = options
            .write_concern
            .or_else(|| db.write_concern().cloned());

        Self {
            inner: Arc::new(CollectionInner {
                db,
                name: name.to_string(),
                read_preference,
                read_concern,
                write_concern,
            }),
        }
    }

    #[allow(unused)]
    pub(crate) fn client(&self) -> Client {
        self.inner.db.client()
    }

    #[allow(unused)]
    pub(crate) fn database(&self) -> Database {
        self.inner.db.clone()
    }

    /// Gets the name of the `Collection`.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Gets the namespace of the `Collection`.
    ///
    /// The namespace of a MongoDB collection is the concatenation of the name of the database
    /// containing it, the '.' character, and the name of the collection itself.inner. For example,
    /// if a collection named "bar" is created in a database named "foo", the namespace of the
    /// collection is "foo.bar".
    pub fn namespace(&self) -> String {
        format!("{}.{}", self.inner.db.name(), self.inner.name)
    }

    /// Gets the read preference of the `Collection`.
    pub fn read_preference(&self) -> Option<&ReadPreference> {
        self.inner.read_preference.as_ref()
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
    pub fn drop(&self) -> Result<()> {
        self.run_write_command(doc! { "drop": &self.inner.name })?;
        Ok(())
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
        self.database()
            .aggregate_helper(self.name(), pipeline, self.read_preference(), options)
    }

    /// Estimates the number of documents in the collection using collection metadata.
    ///
    /// Note that there are restrictions on certain operators for this method. Those operators
    /// (listed below) should be replaced accordingly:
    ///
    /// | Operator | Replacement |
    /// | -------- | ----------- |
    /// | `$where`   | [`$expr`](https://docs.mongodb.com/manual/reference/operator/query/expr/) |
    /// | `$near`    | [`$geoWithin`](https://docs.mongodb.com/manual/reference/operator/query/geoWithin/) with [`$center`](https://docs.mongodb.com/manual/reference/operator/query/center/#op._S_center) |
    /// | `$nearSphere` | [`$geoWithin`](https://docs.mongodb.com/manual/reference/operator/query/geoWithin/) with [`$centerSphere`](https://docs.mongodb.com/manual/reference/operator/query/centerSphere/#op._S_centerSphere) |
    pub fn estimated_document_count(
        &self,
        options: Option<EstimatedDocumentCountOptions>,
    ) -> Result<i64> {
        let mut command_doc = doc! { "count": self.name() };
        if let Some(opts) = options {
            if let Some(max_time) = opts.max_time {
                command_doc.insert("maxTimeMS", max_time.subsec_millis());
            }
        }

        if let Some(ref read_concern) = self.read_concern() {
            command_doc.insert("readConcern", doc! { "level": read_concern.as_str() });
        }

        let (_, response) = self.run_command(command_doc, self.read_preference())?;
        match response.get("n") {
            Some(val) => match bson_util::get_int(val) {
                Some(i) => Ok(i),
                None => bail!(ErrorKind::ResponseError(format!(
                    "expected integer response to count, instead got {}",
                    val
                ))),
            },
            None => bail!(ErrorKind::ResponseError(format!(
                "server response to count command did not include count: {}",
                response
            ))),
        }
    }

    /// Gets the number of documents matching `filter`.
    ///
    /// Note that using `Collection::estimated_document_count` is recommended instead of this method
    /// is most cases.
    ///
    /// Also note that there are restrictions on certain operators for this method. Those operators
    /// (listed below) should be replaced accordingly:
    ///
    /// | Operator | Replacement |
    /// | -------- | ----------- |
    /// | `$where`   | [`$expr`](https://docs.mongodb.com/manual/reference/operator/query/expr/) |
    /// | `$near`    | [`$geoWithin`](https://docs.mongodb.com/manual/reference/operator/query/geoWithin/) with [`$center`](https://docs.mongodb.com/manual/reference/operator/query/center/#op._S_center) |
    /// | `$nearSphere` | [`$geoWithin`](https://docs.mongodb.com/manual/reference/operator/query/geoWithin/) with [`$centerSphere`](https://docs.mongodb.com/manual/reference/operator/query/centerSphere/#op._S_centerSphere) |
    pub fn count_documents(
        &self,
        filter: Option<Document>,
        options: Option<CountOptions>,
    ) -> Result<i64> {
        let mut pipeline = vec![doc! {
            "$match": filter.unwrap_or_default(),
        }];

        let mut aggregate_options: Option<AggregateOptions> = None;

        if let Some(options) = options {
            if let Some(skip) = options.skip {
                pipeline.push(doc! { "$skip": skip });
            }

            if let Some(limit) = options.limit {
                pipeline.push(doc! { "$limit": limit });
            }

            if let Some(hint) = options.hint {
                aggregate_options.get_or_insert_with(Default::default).hint = Some(hint);
            }

            if let Some(max_time) = options.max_time {
                aggregate_options
                    .get_or_insert_with(Default::default)
                    .max_time = Some(max_time);
            }

            if let Some(collation) = options.collation {
                aggregate_options
                    .get_or_insert_with(Default::default)
                    .collation = Some(collation);
            }
        }

        pipeline.push(doc! {
           "$group": {
               "_id": 1,
               "n": { "$sum": 1 }
           }
        });

        let mut cursor = self.aggregate(pipeline, aggregate_options)?;
        let response = match cursor.next() {
            Some(doc) => doc?,
            None => return Ok(0),
        };

        let count = match response.get("n") {
            Some(val) => match bson_util::get_int(val) {
                Some(i) => i,
                None => bail!(ErrorKind::ResponseError(format!(
                    "expected integer response to `count_documents`, instead got {}",
                    val
                ))),
            },
            None => 0,
        };

        Ok(count)
    }

    /// Deletes all documents stored in the collection matching `query`.
    pub fn delete_many(
        &self,
        query: Document,
        options: Option<DeleteOptions>,
    ) -> Result<DeleteResult> {
        self.delete_command(query, options, true)
    }

    /// Deletes up to one document found matching `query`.
    pub fn delete_one(
        &self,
        query: Document,
        options: Option<DeleteOptions>,
    ) -> Result<DeleteResult> {
        self.delete_command(query, options, false)
    }

    /// Finds the distinct values of the field specified by `field_name` across the collection.
    pub fn distinct(
        &self,
        field_name: &str,
        filter: Option<Document>,
        options: Option<DistinctOptions>,
    ) -> Result<Vec<Bson>> {
        let mut command_doc = doc! {
            "distinct": &self.inner.name,
            "key": field_name,
        };

        if let Some(query) = filter {
            command_doc.insert("query", query);
        }

        if let Some(opts) = options {
            if let Some(max_time) = opts.max_time {
                command_doc.insert("maxTimeMS", max_time.subsec_millis());
            }

            if let Some(collation) = opts.collation {
                command_doc.insert("collation", collation.to_bson()?);
            }
        }

        if let Some(ref read_concern) = self.inner.read_concern {
            command_doc.insert("readConcern", doc! { "level": read_concern.as_str() });
        }

        let (_, result) = self.run_command(command_doc, self.inner.read_preference.as_ref())?;

        match bson::from_bson(Bson::Document(result)) {
            Ok(DistinctCommandResponse { values }) => Ok(values),
            Err(_) => bail!(ErrorKind::ResponseError(
                "invalid server response to distinct command".to_string()
            )),
        }
    }

    /// Finds the documents in the collection matching `filter`.
    pub fn find(&self, filter: Option<Document>, options: Option<FindOptions>) -> Result<Cursor> {
        let batch_size = options.as_ref().and_then(|opts| opts.batch_size);
        let (address, result) = self.find_command(filter, options)?;

        Ok(Cursor::new(address, self.clone(), result, batch_size))
    }

    /// Atomically finds up to one document in the collection matching `filter` and deletes it.
    pub fn find_one_and_delete(
        &self,
        filter: Document,
        options: Option<FindOneAndDeleteOptions>,
    ) -> Result<Option<Document>> {
        let mut command_doc = doc! {
            "findAndModify": &self.inner.name,
            "query": filter,
            "remove": true,
        };

        if let Some(opts) = options {
            if let Some(max_time) = opts.max_time {
                command_doc.insert("maxTimeMS", max_time.subsec_millis());
            }

            if let Some(fields) = opts.projection {
                command_doc.insert("fields", fields);
            }

            if let Some(sort) = opts.sort {
                command_doc.insert("sort", sort);
            }

            if let Some(collation) = opts.collation {
                command_doc.insert("collation", collation.to_bson()?);
            }
        }
        self.find_and_modify_command(command_doc, "delete")
    }

    /// Atomically finds up to one document in the collection matching `filter` and replaces it with
    /// `replacement`.
    pub fn find_one_and_replace(
        &self,
        filter: Document,
        replacement: Document,
        options: Option<FindOneAndReplaceOptions>,
    ) -> Result<Option<Document>> {
        bson_util::replacement_document_check(&replacement)?;

        let mut command_doc = doc! {
            "findAndModify": &self.inner.name,
            "query": filter,
            "update": replacement
        };

        if let Some(opts) = options {
            if let Some(bypass_document_validation) = opts.bypass_document_validation {
                command_doc.insert("bypassDocumentValidation", bypass_document_validation);
            }

            if let Some(max_time) = opts.max_time {
                command_doc.insert("maxTimeMS", max_time.subsec_millis());
            }

            if let Some(fields) = opts.projection {
                command_doc.insert("fields", fields);
            }

            if let Some(return_document) = opts.return_document {
                command_doc.insert("new", return_document.is_after());
            }

            if let Some(sort) = opts.sort {
                command_doc.insert("sort", sort);
            }

            if let Some(upsert) = opts.upsert {
                command_doc.insert("upsert", upsert);
            }

            if let Some(collation) = opts.collation {
                command_doc.insert("collation", collation.to_bson()?);
            }
        }

        self.find_and_modify_command(command_doc, "replace")
    }

    /// Atomically finds up to one document in the collection matching `filter` and updates it.
    pub fn find_one_and_update(
        &self,
        filter: Document,
        update: Document,
        options: Option<FindOneAndUpdateOptions>,
    ) -> Result<Option<Document>> {
        bson_util::update_document_check(&update)?;

        let mut command_doc = doc! {
            "findAndModify": &self.inner.name,
            "query": filter,
            "update": update,
        };

        if let Some(opts) = options {
            if let Some(array_filters) = opts.array_filters {
                let filters = Bson::Array(array_filters.into_iter().map(Bson::Document).collect());
                command_doc.insert("arrayFilters", filters);
            }

            if let Some(bypass_document_validation) = opts.bypass_document_validation {
                command_doc.insert("bypassDocumentValidation", bypass_document_validation);
            }

            if let Some(max_time) = opts.max_time {
                command_doc.insert("maxTimeMS", max_time.subsec_millis());
            }

            if let Some(fields) = opts.projection {
                command_doc.insert("fields", fields);
            }

            if let Some(return_document) = opts.return_document {
                command_doc.insert("new", return_document.is_after());
            }

            if let Some(sort) = opts.sort {
                command_doc.insert("sort", sort);
            }

            if let Some(upsert) = opts.upsert {
                command_doc.insert("upsert", upsert);
            }

            if let Some(collation) = opts.collation {
                command_doc.insert("collation", collation.to_bson()?);
            }
        }

        self.find_and_modify_command(command_doc, "update")
    }

    /// Inserts the documents in `docs` into the collection.
    pub fn insert_many(
        &self,
        docs: impl IntoIterator<Item = Document>,
        options: Option<InsertManyOptions>,
    ) -> Result<InsertManyResult> {
        let mut docs: Vec<_> = docs.into_iter().collect();
        let inserted_ids = docs
            .iter_mut()
            .enumerate()
            .map(|(i, d)| (i, bson_util::add_id(d)))
            .collect();
        let options = options.unwrap_or_default();

        self.insert_command(docs, options.bypass_document_validation, options.ordered)?;
        Ok(InsertManyResult { inserted_ids })
    }

    /// Inserts `doc` into the collection.
    pub fn insert_one(
        &self,
        mut doc: Document,
        options: Option<InsertOneOptions>,
    ) -> Result<InsertOneResult> {
        let inserted_id = bson_util::add_id(&mut doc);
        let options = options.unwrap_or_default();

        self.insert_command(vec![doc], options.bypass_document_validation, None)?;
        Ok(InsertOneResult { inserted_id })
    }

    /// Replaces up to one document matching `query` in the collection with `replacement`.
    pub fn replace_one(
        &self,
        query: Document,
        replacement: Document,
        options: Option<ReplaceOptions>,
    ) -> Result<UpdateResult> {
        self.replace_command_helper(query, replacement, options, false)
    }

    /// Updates all documents matching `query` in the collection.
    pub fn update_many(
        &self,
        query: Document,
        update: Document,
        options: Option<UpdateOptions>,
    ) -> Result<UpdateResult> {
        self.update_command_helper(query, update, options, true)
    }

    /// Updates up to one document matching `query` in the collection.
    pub fn update_one(
        &self,
        query: Document,
        update: Document,
        options: Option<UpdateOptions>,
    ) -> Result<UpdateResult> {
        self.update_command_helper(query, update, options, false)
    }

    /// Gets information about each of the indexes in the collection. The cursor will yield a
    /// document pertaining to each index.
    pub fn list_indexes(&self) -> Result<Cursor> {
        let (address, result) = self.run_command(doc! { "listIndexes": self.name() }, None)?;

        if let Some(Bson::I32(26)) = result.get("code") {
            return Ok(Cursor::empty(address, self.clone()));
        }

        match bson::from_bson(Bson::Document(result)) {
            Ok(result) => Ok(Cursor::new(address, self.clone(), result, None)),
            Err(_) => bail!(ErrorKind::ResponseError(
                "invalid server response to find command".to_string()
            )),
        }
    }

    /// Crates a new index specified by the model.
    pub fn create_index(&self, index: IndexModel) -> Result<String> {
        let mut index_names = self.create_indexes(vec![index])?;
        Ok(index_names.remove(0))
    }

    /// Creates the indexes specified by `models`.
    pub fn create_indexes(
        &self,
        models: impl IntoIterator<Item = IndexModel>,
    ) -> Result<Vec<String>> {
        let names_and_indexes: Result<Vec<_>> = models
            .into_iter()
            .map(|IndexModel { keys, options }| {
                let mut options = options.unwrap_or_default();
                let name = {
                    let default_name = || {
                        Bson::String(keys.iter().enumerate().fold(
                            String::new(),
                            |s, (i, (k, v))| {
                                if i == 0 {
                                    format!("{}{}_{}", s, k, v)
                                } else {
                                    format!("{}_{}_{}", s, k, v)
                                }
                            },
                        ))
                    };
                    match options
                        .entry("name".to_string())
                        .or_insert_with(default_name)
                    {
                        Bson::String(ref s) => s.clone(),
                        other => bail!(ErrorKind::ArgumentError(format!(
                            "index name must be string, but instead {:?} was given",
                            other
                        ))),
                    }
                };

                options.insert("key", keys);
                Ok((name, Bson::Document(options)))
            })
            .collect();
        let (names, indexes): (Vec<_>, Vec<_>) = names_and_indexes?.into_iter().unzip();

        let doc = doc! {
            "createIndexes": self.name(),
            "indexes": indexes
        };

        let (_, result) = self.run_command(doc, None)?;

        match bson::from_bson(Bson::Document(result)) {
            Ok(CreateIndexesResponse { ref ok }) if bson_util::get_int(ok) == Some(1) => Ok(names),
            _ => bail!(ErrorKind::ResponseError(
                "invalid server response to find createIndexes command".to_string()
            )),
        }
    }

    /// Drops the index specified by `name`.
    pub fn drop_index(&self, name: &str) -> Result<Document> {
        if name == "*" {
            bail!(ErrorKind::ArgumentError(
                "cannot drop index '*'; use `Collection::drop_indexes` if you want to drop all \
                 indexes on this collection"
                    .to_string()
            ));
        }

        self.drop_index_helper(name)
    }

    /// Drops the index with the given `keys`.
    pub fn drop_index_with_keys(&self, keys: Document) -> Result<Document> {
        self.drop_index_helper(keys)
    }

    /// Drops all indexes in the collection.
    pub fn drop_indexes(&self) -> Result<Document> {
        self.drop_index_helper("*")
    }

    fn drop_index_helper(&self, index: impl Into<Bson>) -> Result<Document> {
        let doc = doc! {
            "dropIndexes": self.name(),
            "index": index.into()
        };
        let (_, result) = self.run_command(doc, None)?;
        Ok(result)
    }

    fn run_command(
        &self,
        doc: Document,
        read_preference: Option<&ReadPreference>,
    ) -> Result<(String, Document)> {
        self.run_command_with_address(doc, read_preference, None)
    }

    fn run_command_with_address(
        &self,
        doc: Document,
        read_preference: Option<&ReadPreference>,
        address: Option<&str>,
    ) -> Result<(String, Document)> {
        self.inner
            .db
            .run_driver_command(doc, read_preference, address)
    }

    fn run_write_command(&self, doc: Document) -> Result<(String, Document)> {
        self.run_command(doc, Some(ReadPreference::Primary).as_ref())
    }

    fn delete_command(
        &self,
        query: Document,
        options: Option<DeleteOptions>,
        multi: bool,
    ) -> Result<DeleteResult> {
        let mut delete_doc = doc! {
            "q": query,
            "limit": if multi { 0 } else { 1 },
        };

        let mut command_doc = doc! {
            "delete": &self.inner.name,
        };

        if let Some(opts) = options {
            if let Some(collation) = opts.collation {
                delete_doc.insert("collation", collation.to_bson()?);
            }
        }

        command_doc.insert(
            "deletes",
            Bson::Array(vec![delete_doc].into_iter().map(Bson::Document).collect()),
        );

        if let Some(ref write_concern) = self.inner.write_concern {
            command_doc.insert("writeConcern", write_concern.clone().into_document()?);
        }

        let (_, result) = self.run_write_command(command_doc)?;

        match bson::from_bson(Bson::Document(result)) {
            Ok(DeleteCommandResponse { n }) => Ok(DeleteResult { deleted_count: n }),
            Err(_) => bail!(ErrorKind::ResponseError(
                "invalid server response to delete command".to_string()
            )),
        }
    }

    fn find_and_modify_command(
        &self,
        command_doc: Document,
        operation: &str,
    ) -> Result<Option<Document>> {
        let (_, result) = self.run_write_command(command_doc)?;
        match bson::from_bson(Bson::Document(result)) {
            Ok(FindAndModifyCommandResponse { value }) => Ok(value),
            Err(_) => bail!(ErrorKind::ResponseError(format!(
                "invalid server response to find_one_and_{} command",
                operation
            ))),
        }
    }

    fn find_command(
        &self,
        filter: Option<Document>,
        options: Option<FindOptions>,
    ) -> Result<(String, FindCommandResponse)> {
        let mut command_doc = doc! { "find" : &self.inner.name };

        if let Some(doc) = filter {
            command_doc.insert("filter", doc);
        }

        if let Some(opts) = options {
            if let Some(allow_partial_results) = opts.allow_partial_results {
                command_doc.insert("allowPartialResults", allow_partial_results);
            }

            if let Some(batch_size) = opts.batch_size {
                command_doc.insert("batchSize", batch_size);
            }

            if let Some(comment) = opts.comment {
                command_doc.insert("comment", comment);
            }

            if let Some(cursor_type) = opts.cursor_type {
                match cursor_type {
                    CursorType::NonTailable => {}
                    CursorType::Tailable => {
                        let _ = command_doc.insert("tailable", true);
                    }
                    CursorType::TailableAwait => {
                        command_doc.insert("tailable", true);
                        command_doc.insert("awaitData", true);
                    }
                }
            }

            if let Some(hint) = opts.hint {
                command_doc.insert("hint", hint.into_bson());
            }

            if let Some(limit) = opts.limit {
                command_doc.insert("limit", limit);
            }

            if let Some(max) = opts.max {
                command_doc.insert("max", max);
            }

            if let Some(max_time) = opts.max_time {
                command_doc.insert("maxTimeMS", max_time.subsec_millis());
            }

            if let Some(min) = opts.min {
                command_doc.insert("min", min);
            }

            if let Some(no_cursor_timeout) = opts.no_cursor_timeout {
                command_doc.insert("noCursorTimeout", no_cursor_timeout);
            }

            if let Some(projection) = opts.projection {
                command_doc.insert("projection", projection);
            }

            if let Some(return_key) = opts.return_key {
                command_doc.insert("returnKey", return_key);
            }

            if let Some(show_record_id) = opts.show_record_id {
                command_doc.insert("showRecordId", show_record_id);
            }

            if let Some(skip) = opts.skip {
                command_doc.insert("skip", skip);
            }

            if let Some(snapshot) = opts.snapshot {
                command_doc.insert("snapshot", snapshot);
            }

            if let Some(sort) = opts.sort {
                command_doc.insert("sort", sort);
            }

            if let (Some(limit), Some(batch_size)) = (opts.limit, opts.batch_size) {
                if limit <= i64::from(batch_size) {
                    command_doc.insert("singleBatch", true);
                }
            }

            if let Some(collation) = opts.collation {
                command_doc.insert("collation", collation.to_bson()?);
            }
        }

        if let Some(ref read_concern) = self.inner.read_concern {
            command_doc.insert("readConcern", doc! { "level": read_concern.as_str() });
        }

        let (address, result) =
            self.run_command(command_doc, self.inner.read_preference.as_ref())?;

        match bson::from_bson(Bson::Document(result)) {
            Ok(result) => Ok((address, result)),
            Err(_) => bail!(ErrorKind::ResponseError(
                "invalid server response to find command".to_string()
            )),
        }
    }

    fn insert_command(
        &self,
        docs: Vec<Document>,
        bypass_document_validation: Option<bool>,
        ordered: Option<bool>,
    ) -> Result<()> {
        let docs: Vec<_> = docs.into_iter().map(Bson::Document).collect();

        if docs.is_empty() {
            return Ok(());
        }

        let mut command_doc = doc! {
            "insert": &self.inner.name,
            "documents": Bson::Array(docs),
        };

        if let Some(val) = bypass_document_validation {
            command_doc.insert("bypassDocumentValidation", val);
        }

        if let Some(val) = ordered {
            command_doc.insert("ordered", val);
        }

        if let Some(ref write_concern) = self.inner.write_concern {
            command_doc.insert("writeConcern", write_concern.clone().into_document()?);
        }

        if bson_util::doc_size_bytes(&command_doc) > MAX_INSERT_DOCS_BYTES {
            return self.insert_in_batches(command_doc);
        }

        self.run_write_command_with_error_check(command_doc)?;
        Ok(())
    }

    fn insert_in_batches(&self, mut command_doc: Document) -> Result<()> {
        let mut remaining_docs = match command_doc.remove("documents") {
            Some(Bson::Array(docs)) => docs,
            _ => unreachable!(),
        };

        while let Some(mut current_batch) = batch::split_off_batch(
            &mut remaining_docs,
            MAX_INSERT_DOCS_BYTES,
            bson_util::size_bytes,
        ) {
            std::mem::swap(&mut remaining_docs, &mut current_batch);

            let mut current_batch_doc = command_doc.clone();
            current_batch_doc.insert("documents", current_batch);
            self.run_write_command_with_error_check(current_batch_doc)?;
        }

        if !remaining_docs.is_empty() {
            command_doc.insert("documents", remaining_docs);
            self.run_write_command_with_error_check(command_doc)?;
        }

        Ok(())
    }

    fn run_write_command_with_error_check(&self, command_doc: Document) -> Result<()> {
        if let Ok((_, document)) = self.run_write_command(command_doc) {
            if let Some(error) = Error::from_command_response(document) {
                return Err(error);
            }
        }

        Ok(())
    }

    fn replace_command_helper(
        &self,
        query: Document,
        replacement: Document,
        options: Option<ReplaceOptions>,
        multi: bool,
    ) -> Result<UpdateResult> {
        bson_util::replacement_document_check(&replacement)?;

        let mut update_doc = doc! {
            "q": query,
            "u": replacement,
        };

        let mut command_doc = doc! {
            "update": &self.inner.name,
        };

        if multi {
            update_doc.insert("multi", multi);
        }

        if let Some(option) = options {
            if let Some(upsert) = option.upsert {
                update_doc.insert("upsert", upsert);
            }

            if let Some(bypass_document_validation) = option.bypass_document_validation {
                command_doc.insert("bypassDocumentValidation", bypass_document_validation);
            }

            if let Some(collation) = option.collation {
                update_doc.insert("collation", collation.to_bson()?);
            }
        }

        command_doc.insert(
            "updates",
            Bson::Array(vec![update_doc].into_iter().map(Bson::Document).collect()),
        );

        self.update_command(command_doc)
    }

    fn update_command_helper(
        &self,
        query: Document,
        update: Document,
        options: Option<UpdateOptions>,
        multi: bool,
    ) -> Result<UpdateResult> {
        bson_util::update_document_check(&update)?;

        let mut update_doc = doc! {
            "q": query,
            "u": update,
        };

        let mut command_doc = doc! {
            "update": &self.inner.name,
        };

        if multi {
            update_doc.insert("multi", multi);
        }

        if let Some(option) = options {
            if let Some(array_filters) = option.array_filters {
                let filters = Bson::Array(array_filters.into_iter().map(Bson::Document).collect());
                update_doc.insert("arrayFilters", filters);
            }

            if let Some(upsert) = option.upsert {
                update_doc.insert("upsert", upsert);
            }

            if let Some(bypass_document_validation) = option.bypass_document_validation {
                command_doc.insert("bypassDocumentValidation", bypass_document_validation);
            }

            if let Some(collation) = option.collation {
                update_doc.insert("collation", collation.to_bson()?);
            }
        }

        command_doc.insert(
            "updates",
            Bson::Array(vec![update_doc].into_iter().map(Bson::Document).collect()),
        );

        self.update_command(command_doc)
    }

    fn update_command(&self, mut command_doc: Document) -> Result<UpdateResult> {
        if let Some(ref write_concern) = self.inner.write_concern {
            command_doc.insert("writeConcern", write_concern.clone().into_document()?);
        }

        let (_, result) = self.run_write_command(command_doc)?;

        match bson::from_bson(Bson::Document(result)) {
            Ok(UpdateCommandResponse {
                n,
                n_modified,
                upserted,
            }) => Ok(UpdateResult {
                matched_count: n - upserted.as_ref().map(|v| v.len() as i64).unwrap_or(0),
                modified_count: n_modified,
                upserted_id: upserted.and_then(|mut v| v.pop().map(|r| r.id)),
            }),
            Err(_) => bail!(ErrorKind::ResponseError(
                "invalid server response to update command".to_string()
            )),
        }
    }

    pub(crate) fn get_more_command(
        &self,
        address: &str,
        cursor_id: i64,
        batch_size: Option<i32>,
    ) -> Result<GetMoreCommandResponse> {
        let mut command_doc = doc! {
            "getMore": cursor_id,
            "collection": self.name(),
        };

        if let Some(batch_size) = batch_size {
            command_doc.insert("batchSize", batch_size);
        }

        let (_, result) = self.run_command_with_address(
            doc! {
                "getMore": cursor_id,
                "collection": self.name(),
            },
            self.read_preference(),
            Some(address),
        )?;

        match bson::from_bson(Bson::Document(result)) {
            Ok(result) => Ok(result),
            Err(_) => bail!(ErrorKind::ResponseError(
                "invalid server response to getmore".to_string()
            )),
        }
    }

    /// Returns a change stream on a specific collection.
    ///
    /// Note that using a `$project` stage to remove any of the `_id`
    /// `operationType` or `ns` fields will cause an error. The driver
    /// requires these fields to support resumability.
    pub(crate) fn watch(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: Option<ChangeStreamOptions>,
    ) -> Result<ChangeStream> {
        let pipeline: Vec<Document> = pipeline.into_iter().collect();

        let mut watch_pipeline = Vec::new();
        let mut aggregate_options: Option<AggregateOptions>;
        let mut resume_token: ChangeStreamToken;
        let stream_options = options.clone();

        if let Some(options) = options {
            watch_pipeline.push(doc! { "$changeStream": options.to_bson()? });
            aggregate_options = Some(
                AggregateOptions::builder()
                    .collation(options.collation)
                    .build(),
            );
            resume_token = ChangeStreamToken::new(options.start_after.or(options.resume_after));
        } else {
            watch_pipeline.push(doc! { "$changeStream": {} });
            aggregate_options = None;
            resume_token = ChangeStreamToken::new(None);
        }
        watch_pipeline.extend(pipeline.clone());

        let cursor = self.aggregate(watch_pipeline, aggregate_options)?;

        let read_preference = self
            .read_preference()
            .or(self.database().read_preference())
            .or(self.client().read_preference())
            .map(|pref: &ReadPreference| pref.clone());

        Ok(ChangeStream {
            cursor,
            resume_token,
            pipeline,
            options: stream_options,
            read_preference,
        })
    }
}
