use std::{marker::Unpin, sync::atomic::Ordering};

use futures_util::{
    io::{AsyncRead, AsyncReadExt},
    stream::TryStreamExt,
};

use super::{options::GridFsUploadOptions, Chunk, FilesCollectionDocument, GridFsBucket};
use crate::{
    bson::{doc, oid::ObjectId, spec::BinarySubtype, Bson, DateTime, Document, RawBinaryRef},
    bson_util::get_int,
    error::{ErrorKind, Result},
    index::IndexModel,
    options::{CreateCollectionOptions, FindOneOptions, ReadPreference, SelectionCriteria},
    Collection,
};

impl GridFsBucket {
    /// Uploads a user file to a GridFS bucket. Bytes are read from `source` and stored in chunks in
    /// the bucket's chunks collection. After all the chunks have been uploaded, a corresponding
    /// [`FilesCollectionDocument`] is stored in the bucket's files collection.
    ///
    /// This method generates an [`ObjectId`] for the `files_id` field of the
    /// [`FilesCollectionDocument`] and returns it.
    pub async fn upload_from_futures_0_3_reader<T>(
        &self,
        filename: impl AsRef<str>,
        source: T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<ObjectId>
    where
        T: AsyncRead + Unpin,
    {
        let id = ObjectId::new();
        self.upload_from_futures_0_3_reader_with_id(id.into(), filename, source, options)
            .await?;
        Ok(id)
    }

    /// Uploads a user file to a GridFS bucket with the given `files_id`. Bytes are read from
    /// `source` and stored in chunks in the bucket's chunks collection. After all the chunks have
    /// been uploaded, a corresponding [`FilesCollectionDocument`] is stored in the bucket's files
    /// collection.
    pub async fn upload_from_futures_0_3_reader_with_id<T>(
        &self,
        files_id: Bson,
        filename: impl AsRef<str>,
        mut source: T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<()>
    where
        T: AsyncRead + Unpin,
    {
        let options = options.into();

        self.create_indexes().await?;

        let chunk_size = options
            .as_ref()
            .and_then(|opts| opts.chunk_size_bytes)
            .unwrap_or_else(|| self.chunk_size_bytes());
        let mut length = 0u64;
        let mut n = 0;

        let mut buf = vec![0u8; chunk_size as usize];
        loop {
            let bytes_read = match source.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(error) => {
                    self.chunks()
                        .delete_many(doc! { "files_id": &files_id }, None)
                        .await?;
                    return Err(ErrorKind::Io(error.into()).into());
                }
            };

            let chunk = Chunk {
                id: ObjectId::new(),
                files_id: files_id.clone(),
                n,
                data: RawBinaryRef {
                    subtype: BinarySubtype::Generic,
                    bytes: &buf[..bytes_read],
                },
            };
            self.chunks().insert_one(chunk, None).await?;

            length += bytes_read as u64;
            n += 1;
        }

        let file = FilesCollectionDocument {
            id: files_id,
            length,
            chunk_size,
            upload_date: DateTime::now(),
            filename: Some(filename.as_ref().to_string()),
            metadata: options.and_then(|opts| opts.metadata),
        };
        self.files().insert_one(file, None).await?;

        Ok(())
    }

    async fn create_indexes(&self) -> Result<()> {
        if !self.inner.created_indexes.load(Ordering::SeqCst) {
            let find_options = FindOneOptions::builder()
                .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary))
                .projection(doc! { "_id": 1 })
                .build();
            if self
                .files()
                .clone_with_type::<Document>()
                .find_one(None, find_options)
                .await?
                .is_none()
            {
                self.create_index(self.files(), doc! { "filename": 1, "uploadDate": 1 })
                    .await?;
                self.create_index(self.chunks(), doc! { "files_id": 1, "n": 1 })
                    .await?;
            }
            self.inner.created_indexes.store(true, Ordering::SeqCst);
        }

        Ok(())
    }

    async fn create_index<T>(&self, coll: &Collection<T>, keys: Document) -> Result<()> {
        // listIndexes returns an error if the collection has not yet been created.
        let options = CreateCollectionOptions::builder()
            .write_concern(self.write_concern().cloned())
            .build();
        // Ignore NamespaceExists errors if the collection has already been created.
        if let Err(error) = self.inner.db.create_collection(coll.name(), options).await {
            if error.code() != Some(48) {
                return Err(error);
            }
        }

        // From the spec: Drivers MUST check whether the indexes already exist before attempting to
        // create them.
        let mut indexes = coll.list_indexes(None).await?;
        'outer: while let Some(index_model) = indexes.try_next().await? {
            if index_model.keys.len() != keys.len() {
                continue;
            }
            // Indexes should be considered equivalent regardless of numeric value type.
            // e.g. { "filename": 1, "uploadDate": 1 } is equivalent to
            // { "filename": 1.0, "uploadDate": 1.0 }
            let number_matches = |key: &str, value: &Bson| {
                if let Some(model_value) = index_model.keys.get(key) {
                    match get_int(value) {
                        Some(num) => get_int(model_value) == Some(num),
                        None => model_value == value,
                    }
                } else {
                    false
                }
            };
            for (key, value) in keys.iter() {
                if !number_matches(key, value) {
                    continue 'outer;
                }
            }
            return Ok(());
        }

        let index_model = IndexModel::builder().keys(keys).build();
        coll.create_index(index_model, None).await?;

        Ok(())
    }
}
