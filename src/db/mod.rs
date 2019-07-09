pub mod options;

use std::sync::Arc;

use bson::{Bson, Document};

use self::options::{CreateCollectionOptions, DatabaseOptions};
use crate::{
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
    error::{ErrorKind, Result},
    options::{AggregateOptions, CollectionOptions},
    pool::run_command,
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
/// # let client = Client::with_uri_str("mongodb://example.com")?;
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
    pub(crate) fn new(client: Client, name: &str, options: Option<DatabaseOptions>) -> Self {
        let options = options.unwrap_or_default();
        let read_preference = options
            .read_preference
            .or_else(|| client.read_preference().cloned());

        let read_concern = options
            .read_concern
            .or_else(|| client.read_concern().cloned());

        let write_concern = options
            .write_concern
            .or_else(|| client.write_concern().cloned());

        Self {
            inner: Arc::new(DatabaseInner {
                client,
                name: name.to_string(),
                read_preference,
                read_concern,
                write_concern,
            }),
        }
    }

    pub(crate) fn client(&self) -> Client {
        self.inner.client.clone()
    }

    /// Gets the name of the `Database`.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Gets the read preference of the `Database`.
    pub fn read_preference(&self) -> Option<&ReadPreference> {
        self.inner.read_preference.as_ref()
    }

    /// Gets the read concern of the `Database`.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.read_concern.as_ref()
    }

    /// Gets the write concern of the `Database`.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.write_concern.as_ref()
    }

    /// Gets a handle to a collection specified by `name` of the database. The `Collection` options
    /// (e.g. read preference and write concern) will default to those of the `Database`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn collection(&self, name: &str) -> Collection {
        self.collection_with_options(name, Default::default())
    }

    /// Gets a handle to a collection specified by `name` in the cluster the `Client` is connected
    /// to. Operations done with this `Collection` will use the options specified by `options` by
    /// default and will otherwise default to those of the `Database`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn collection_with_options(&self, name: &str, options: CollectionOptions) -> Collection {
        Collection::new(self.clone(), name, Some(options))
    }

    /// Drops the database, deleting all data, collections, users, and indexes stored in it.
    pub fn drop(&self) -> Result<()> {
        self.run_driver_command(
            doc! { "dropDatabase": 1 },
            Some(ReadPreference::Primary).as_ref(),
            None,
        )?;
        Ok(())
    }

    /// Gets information about each of the collections in the database. The cursor will yield a
    /// document pertaining to each collection in the database.
    pub fn list_collections(&self, filter: Option<Document>) -> Result<Cursor> {
        let mut cmd = doc! { "listCollections": 1 };
        if let Some(filter) = filter {
            cmd.insert("filter", filter);
        }

        let (address, result) = self.run_driver_command(cmd, None, None)?;

        match bson::from_bson(Bson::Document(result)) {
            Ok(result) => Ok(Cursor::new(
                address,
                self.collection("$cmd.listCollections"),
                result,
                None,
            )),
            Err(_) => bail!(ErrorKind::ResponseError(
                "invalid server response to find command".to_string()
            )),
        }
    }

    /// Gets the names of the collections stored in the database.
    pub fn list_collection_names(&self, filter: Option<Document>) -> Result<Vec<String>> {
        let mut cmd = doc! {
            "listCollections": 1,
            "nameOnly": true
        };
        if let Some(filter) = filter {
            cmd.insert("filter", filter);
        }

        self.list_collections_command(cmd)?
            .map(|doc| match doc?.get("name") {
                Some(Bson::String(name)) => Ok(name.to_string()),
                _ => bail!(ErrorKind::ResponseError(
                    "invalid document returned by listCollections cursor".to_string(),
                )),
            })
            .collect()
    }

    fn list_collections_command(&self, cmd: Document) -> Result<Cursor> {
        let (address, result) = self.run_driver_command(cmd, None, None)?;

        match bson::from_bson(Bson::Document(result)) {
            Ok(result) => Ok(Cursor::new(
                address,
                self.collection("$cmd.listCollections"),
                result,
                None,
            )),
            Err(_) => bail!(ErrorKind::ResponseError(
                "invalid server response to find command".to_string()
            )),
        }
    }

    /// Creates a new collection in the database with the given `name` and `options`.
    ///
    /// Note that MongoDB creates collections implicitly when data is inserted, so the method is not
    /// needed if no special options are required.
    pub fn create_collection(
        &self,
        name: &str,
        options: Option<CreateCollectionOptions>,
    ) -> Result<()> {
        let mut base = doc! { "create": name };

        let cmd = if let Some(opts) = options {
            if let Some(capped) = opts.capped {
                base.insert("capped", capped);
            }

            if let Some(size) = opts.size {
                base.insert("size", size);
            }

            if let Some(max) = opts.max {
                base.insert("max", max);
            }

            if let Some(storage_engine) = opts.storage_engine {
                base.insert("storageEngine", storage_engine);
            }

            if let Some(validation) = opts.validation {
                base.insert("validator", validation);
            }

            if let Some(validation_level) = opts.validation_level {
                base.insert("validationLevel", validation_level.as_str());
            }

            if let Some(validation_action) = opts.validation_action {
                base.insert("validationAction", validation_action.as_str());
            }

            if let Some(view_on) = opts.view_on {
                base.insert("viewOn", view_on);
            }

            if let Some(pipeline) = opts.pipeline {
                base.insert(
                    "pipeline",
                    Bson::Array(pipeline.into_iter().map(Bson::Document).collect()),
                );
            }

            if let Some(collation) = opts.collation {
                base.insert("collation", collation.to_bson()?);
            }

            base
        } else {
            base
        };

        let (_, result) = self.run_driver_command(cmd, None, None)?;

        if let Some(ref s) = result.get("errmsg") {
            bail!(ErrorKind::ServerError(
                s.to_string(),
                "create collection".to_string(),
            ))
        }

        Ok(())
    }

    /// Runs a database level aggregation operation.
    pub fn aggregate(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: Option<AggregateOptions>,
    ) -> Result<Cursor> {
        self.aggregate_helper(1, pipeline, self.read_preference(), options)
    }

    pub(crate) fn aggregate_helper(
        &self,
        coll_name: impl Into<Bson>,
        pipeline: impl IntoIterator<Item = Document>,
        read_pref: Option<&ReadPreference>,
        options: Option<AggregateOptions>,
    ) -> Result<Cursor> {
        let coll_name_bson = coll_name.into();
        let cursor_coll_name = match coll_name_bson {
            Bson::I32(1) => "$cmd.aggregate".to_string(),
            Bson::String(ref s) => s.clone(),
            _ => unreachable!(),
        };

        let batch_size = options.as_ref().and_then(|opts| opts.batch_size);
        let pipeline: Vec<_> = pipeline.into_iter().collect();

        let has_out = pipeline.iter().any(|d| d.contains_key("$out"));

        let mut command_doc = doc! {
            "aggregate": coll_name_bson,
            "pipeline": Bson::Array(pipeline.into_iter().map(Bson::Document).collect()),
        };

        let mut cursor_option = Document::new();

        if let Some(opts) = options {
            if let Some(allow_disk_use) = opts.allow_disk_use {
                command_doc.insert("allowDiskUse", allow_disk_use);
            }

            if let Some(batch_size) = opts.batch_size {
                if !has_out {
                    cursor_option.insert("batchSize", batch_size);
                }
            }

            if let Some(bypass_document_validation) = opts.bypass_document_validation {
                command_doc.insert("bypassDocumentValidation", bypass_document_validation);
            }

            if let Some(max_time) = opts.max_time {
                command_doc.insert("maxTimeMS", max_time.subsec_millis());
            }

            if let Some(comment) = opts.comment {
                command_doc.insert("comment", comment);
            }

            if let Some(hint) = opts.hint {
                command_doc.insert("hint", hint.into_bson());
            }

            if let Some(collation) = opts.collation {
                command_doc.insert("collation", collation.to_bson()?);
            }
        }

        command_doc.insert("cursor", cursor_option);

        if has_out {
            if let Some(ref write_concern) = self.inner.write_concern {
                command_doc.insert("writeConcern", write_concern.clone().into_document()?);
            }
        } else if let Some(ref read_concern) = self.inner.read_concern {
            if cursor_coll_name != "$cmd.aggregate" {
                command_doc.insert("readConcern", doc! { "level": read_concern.as_str() });
            }
        }

        let (address, result) = if has_out {
            match self.run_driver_command(command_doc, Some(ReadPreference::Primary).as_ref(), None)
            {
                Ok((address, result)) => (address, result),
                Err(_) => bail!(ErrorKind::ResponseError(
                    "invalid server response to aggregate command".to_string()
                )),
            }
        } else {
            match self.run_driver_command(command_doc, read_pref, None) {
                Ok((address, result)) => (address, result),
                Err(_) => bail!(ErrorKind::ResponseError(
                    "invalid server response to aggregate command".to_string()
                )),
            }
        };

        match bson::from_bson(Bson::Document(result)) {
            Ok(result) => Ok(Cursor::new(
                address,
                self.collection(&cursor_coll_name),
                result,
                batch_size,
            )),
            Err(_) => bail!(ErrorKind::ResponseError(
                "invalid server response to aggregate command".to_string()
            )),
        }
    }

    /// Runs a database-level command.
    ///
    /// Note that no inspection is done on `doc`, so the command will not use the database's default
    /// read concern or write concern. If a specific read concern or write concern is desired, it
    /// must be specified manually.
    pub fn run_command(
        &self,
        doc: Document,
        read_pref: Option<ReadPreference>,
    ) -> Result<Document> {
        let (_, doc) = self.run_command_helper(
            doc,
            read_pref.or_else(|| Some(ReadPreference::Primary)).as_ref(),
            None,
        )?;
        Ok(doc)
    }

    pub(crate) fn run_driver_command(
        &self,
        doc: Document,
        read_pref: Option<&ReadPreference>,
        address: Option<&str>,
    ) -> Result<(String, Document)> {
        self.run_command_helper(doc, read_pref.or_else(|| self.read_preference()), address)
    }

    fn run_command_helper(
        &self,
        doc: Document,
        read_pref: Option<&ReadPreference>,
        address: Option<&str>,
    ) -> Result<(String, Document)> {
        let (address, mut stream) = self.inner.client.acquire_stream(read_pref, address)?;
        let slave_ok = self.client().slave_ok(&address, read_pref);
        Ok((
            address,
            run_command(&mut stream, &self.inner.name, doc, slave_ok)?,
        ))
    }
}
