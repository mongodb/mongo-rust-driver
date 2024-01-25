//! Contains the functionality for GridFS operations.

mod download;
pub(crate) mod options;
mod upload;

use std::sync::{atomic::AtomicBool, Arc};

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::{
    bson::{doc, oid::ObjectId, Bson, DateTime, Document, RawBinaryRef},
    cursor::Cursor,
    error::{Error, ErrorKind, GridFsErrorKind, GridFsFileIdentifier, Result},
    options::{
        CollectionOptions,
        FindOneOptions,
        FindOptions,
        ReadConcern,
        SelectionCriteria,
        WriteConcern,
    },
    Collection,
    Database,
};

pub use download::GridFsDownloadStream;
pub(crate) use options::*;
pub use upload::GridFsUploadStream;

const DEFAULT_BUCKET_NAME: &str = "fs";
const DEFAULT_CHUNK_SIZE_BYTES: u32 = 255 * 1024;

/// A model for the documents stored in the chunks collection.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Chunk<'a> {
    #[serde(rename = "_id")]
    id: ObjectId,
    files_id: Bson,
    #[serde(serialize_with = "bson::serde_helpers::serialize_u32_as_i32")]
    n: u32,
    #[serde(borrow)]
    data: RawBinaryRef<'a>,
}

/// A model for the documents stored in a GridFS bucket's files
/// collection.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[skip_serializing_none]
#[non_exhaustive]
pub struct FilesCollectionDocument {
    /// The file's unique identifier.
    #[serde(rename = "_id")]
    pub id: Bson,

    /// The length of the file in bytes.
    pub length: u64,

    /// The size of the file's chunks in bytes.
    #[serde(
        rename = "chunkSize",
        serialize_with = "bson::serde_helpers::serialize_u32_as_i32"
    )]
    pub chunk_size_bytes: u32,

    /// The time at which the file was uploaded.
    pub upload_date: DateTime,

    /// The name of the file.
    pub filename: Option<String>,

    /// User-provided metadata associated with the file.
    pub metadata: Option<Document>,
}

impl FilesCollectionDocument {
    /// Returns the total number of chunks expected to be in the file.
    fn n(&self) -> u32 {
        Self::n_from_vals(self.length, self.chunk_size_bytes)
    }

    fn n_from_vals(length: u64, chunk_size_bytes: u32) -> u32 {
        let chunk_size_bytes = chunk_size_bytes as u64;
        let n = length / chunk_size_bytes + u64::from(length % chunk_size_bytes != 0);
        n as u32
    }

    /// Returns the expected length of a chunk given its index.
    fn expected_chunk_length(&self, n: u32) -> u32 {
        Self::expected_chunk_length_from_vals(self.length, self.chunk_size_bytes, n)
    }

    fn expected_chunk_length_from_vals(length: u64, chunk_size_bytes: u32, n: u32) -> u32 {
        let remainder = length % (chunk_size_bytes as u64);
        if n == Self::n_from_vals(length, chunk_size_bytes) - 1 && remainder != 0 {
            remainder as u32
        } else {
            chunk_size_bytes
        }
    }
}

#[derive(Debug)]
struct GridFsBucketInner {
    db: Database,
    options: GridFsBucketOptions,
    files: Collection<FilesCollectionDocument>,
    chunks: Collection<Chunk<'static>>,
    created_indexes: AtomicBool,
}

/// A `GridFsBucket` provides the functionality for storing and retrieving binary BSON data that
/// exceeds the 16 MiB size limit of a MongoDB document. Users may upload and download large amounts
/// of data, called files, to the bucket. When a file is uploaded, its contents are divided into
/// chunks and stored in a chunks collection. A corresponding [`FilesCollectionDocument`] is also
/// stored in a files collection. When a user downloads a file, the bucket finds and returns the
/// data stored in its chunks.
///
/// `GridFsBucket` uses [`std::sync::Arc`] internally, so it can be shared safely across threads or
/// async tasks.
#[derive(Debug, Clone)]
pub struct GridFsBucket {
    inner: Arc<GridFsBucketInner>,
}

impl GridFsBucket {
    pub(crate) fn new(db: Database, mut options: GridFsBucketOptions) -> GridFsBucket {
        if options.read_concern.is_none() {
            options.read_concern = db.read_concern().cloned();
        }
        if options.write_concern.is_none() {
            options.write_concern = db.write_concern().cloned();
        }
        if options.selection_criteria.is_none() {
            options.selection_criteria = db.selection_criteria().cloned();
        }

        let bucket_name = options
            .bucket_name
            .as_deref()
            .unwrap_or(DEFAULT_BUCKET_NAME);

        let collection_options = CollectionOptions::builder()
            .read_concern(options.read_concern.clone())
            .write_concern(options.write_concern.clone())
            .selection_criteria(options.selection_criteria.clone())
            .build();
        let files = db.collection_with_options::<FilesCollectionDocument>(
            &format!("{}.files", bucket_name),
            collection_options.clone(),
        );
        let chunks = db.collection_with_options::<Chunk>(
            &format!("{}.chunks", bucket_name),
            collection_options,
        );

        GridFsBucket {
            inner: Arc::new(GridFsBucketInner {
                db: db.clone(),
                options,
                files,
                chunks,
                created_indexes: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn client(&self) -> &crate::Client {
        self.inner.files.client()
    }

    /// Gets the read concern of the bucket.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.options.read_concern.as_ref()
    }

    /// Gets the write concern of the bucket.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.options.write_concern.as_ref()
    }

    /// Gets the selection criteria of the bucket.
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.inner.options.selection_criteria.as_ref()
    }

    /// Gets the chunk size in bytes for the bucket.
    fn chunk_size_bytes(&self) -> u32 {
        self.inner
            .options
            .chunk_size_bytes
            .unwrap_or(DEFAULT_CHUNK_SIZE_BYTES)
    }

    /// Gets a handle to the files collection for the bucket.
    pub(crate) fn files(&self) -> &Collection<FilesCollectionDocument> {
        &self.inner.files
    }

    /// Gets a handle to the chunks collection for the bucket.
    pub(crate) fn chunks(&self) -> &Collection<Chunk<'static>> {
        &self.inner.chunks
    }

    /// Deletes the [`FilesCollectionDocument`] with the given `id` and its associated chunks from
    /// this bucket. This method returns an error if the `id` does not match any files in the
    /// bucket.
    pub async fn delete(&self, id: Bson) -> Result<()> {
        let delete_result = self
            .files()
            .delete_one(doc! { "_id": id.clone() }, None)
            .await?;
        // Delete chunks regardless of whether a file was found. This will remove any possibly
        // orphaned chunks.
        self.chunks()
            .delete_many(doc! { "files_id": id.clone() }, None)
            .await?;

        if delete_result.deleted_count == 0 {
            return Err(ErrorKind::GridFs(GridFsErrorKind::FileNotFound {
                identifier: GridFsFileIdentifier::Id(id),
            })
            .into());
        }

        Ok(())
    }

    /// Finds and returns the [`FilesCollectionDocument`]s within this bucket that match the given
    /// filter.
    pub async fn find(
        &self,
        filter: Document,
        options: impl Into<Option<GridFsFindOptions>>,
    ) -> Result<Cursor<FilesCollectionDocument>> {
        let find_options = options.into().map(FindOptions::from);
        self.files().find(filter, find_options).await
    }

    /// Finds and returns a single [`FilesCollectionDocument`] within this bucket that matches the
    /// given filter.
    pub async fn find_one(
        &self,
        filter: Document,
        options: impl Into<Option<GridFsFindOneOptions>>,
    ) -> Result<Option<FilesCollectionDocument>> {
        let find_options = options.into().map(FindOneOptions::from);
        self.files().find_one(filter, find_options).await
    }

    /// Renames the file with the given 'id' to the provided `new_filename`. This method returns an
    /// error if the `id` does not match any files in the bucket.
    pub async fn rename(&self, id: Bson, new_filename: impl AsRef<str>) -> Result<()> {
        self.files()
            .update_one(
                doc! { "_id": id },
                doc! { "$set": { "filename": new_filename.as_ref() } },
                None,
            )
            .await?;

        Ok(())
    }

    /// Removes all of the files and their associated chunks from this bucket.
    pub async fn drop(&self) -> Result<()> {
        self.files().drop(None).await?;
        self.chunks().drop(None).await?;

        Ok(())
    }
}

impl Error {
    fn into_futures_io_error(self) -> futures_io::Error {
        futures_io::Error::new(futures_io::ErrorKind::Other, self)
    }
}
