// TODO(RUST-1395) Remove these allows.
#![allow(dead_code, unused_variables)]

mod download;
pub mod options;

use core::task::{Context, Poll};
use std::{
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
};

use serde::{Deserialize, Serialize};
use tokio::io::ReadBuf;

use crate::{
    bson::{doc, oid::ObjectId, Bson, DateTime, Document, RawBinaryRef},
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
    error::{ErrorKind, Result},
    options::{
        DeleteOptions,
        DropCollectionOptions,
        FindOptions,
        SelectionCriteria,
        UpdateOptions,
    },
    Collection,
    Database,
};

use options::*;

pub const DEFAULT_BUCKET_NAME: &str = "fs";
pub const DEFAULT_CHUNK_SIZE_BYTES: u32 = 255 * 1024;

// Contained in a "chunks" collection for each user file
#[derive(Debug, Deserialize, Serialize)]
struct Chunk<'a> {
    #[serde(rename = "_id")]
    id: ObjectId,
    files_id: Bson,
    n: u32,
    #[serde(borrow)]
    data: RawBinaryRef<'a>,
}

/// A collection in which information about stored files is stored. There will be one files
/// collection document per stored file.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct FilesCollectionDocument {
    #[serde(rename = "_id")]
    pub id: Bson,
    pub length: u64,
    pub chunk_size: u32,
    pub upload_date: DateTime,
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Document>,
}

#[derive(Debug)]
struct GridFsBucketInner {
    options: GridFsBucketOptions,
    files: Collection<FilesCollectionDocument>,
    chunks: Collection<Chunk<'static>>,
    created_indexes: AtomicBool,
}

/// Struct for storing GridFS managed files within a [`Database`].
#[derive(Debug, Clone)]
pub struct GridFsBucket {
    inner: Arc<GridFsBucketInner>,
}

// TODO: RUST-1395 Add documentation and example code for this struct.
pub struct GridFsUploadStream {
    files_id: Bson,
}

impl GridFsUploadStream {
    /// Gets the file `id` for the stream.
    pub fn files_id(&self) -> &Bson {
        &self.files_id
    }

    /// Consumes the stream and uploads data in the stream to the server.
    pub async fn finish(self) {
        todo!()
    }

    /// Aborts the upload and discards the upload stream.
    pub async fn abort(self) {
        todo!()
    }
}

impl tokio::io::AsyncWrite for GridFsUploadStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<tokio::io::Result<usize>> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
        todo!()
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
        todo!()
    }
}

impl futures_util::AsyncWrite for GridFsUploadStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<core::result::Result<usize, futures_util::io::Error>> {
        todo!()
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<core::result::Result<(), futures_util::io::Error>> {
        todo!()
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<core::result::Result<(), futures_util::io::Error>> {
        todo!()
    }
}

pub struct GridFsDownloadStream {
    files_id: Bson,
}

impl GridFsDownloadStream {
    /// Gets the file `id` for the stream.
    pub fn files_id(&self) -> &Bson {
        &self.files_id
    }
}

impl tokio::io::AsyncRead for GridFsDownloadStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<tokio::io::Result<()>> {
        todo!()
    }
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

        let files = db.collection::<FilesCollectionDocument>(&format!("{}.files", bucket_name));
        let chunks = db.collection::<Chunk>(&format!("{}.chunks", bucket_name));

        GridFsBucket {
            inner: Arc::new(GridFsBucketInner {
                options,
                files,
                chunks,
                created_indexes: AtomicBool::new(false),
            }),
        }
    }

    /// Gets the read concern of the [`GridFsBucket`].
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.options.read_concern.as_ref()
    }

    /// Gets the write concern of the [`GridFsBucket`].
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.options.write_concern.as_ref()
    }

    /// Gets the selection criteria of the [`GridFsBucket`].
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.inner.options.selection_criteria.as_ref()
    }

    /// Gets the chunk size in bytes for the [`GridFsBucket`].
    fn chunk_size_bytes(&self) -> u32 {
        self.inner
            .options
            .chunk_size_bytes
            .unwrap_or(DEFAULT_CHUNK_SIZE_BYTES)
    }

    /// Gets a handle to the files collection for the [`GridFsBucket`].
    fn files(&self) -> &Collection<FilesCollectionDocument> {
        &self.inner.files
    }

    /// Gets a handle to the chunks collection for the [`GridFsBucket`].
    fn chunks(&self) -> &Collection<Chunk> {
        &self.inner.chunks
    }

    /// Opens a [`GridFsUploadStream`] that the application can write the contents of the file to.
    /// The application provides a custom file id.
    ///
    /// Returns a [`GridFsUploadStream`] to which the application will write the contents.
    pub async fn open_upload_stream_with_id(
        &self,
        id: Bson,
        filename: String,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<GridFsUploadStream> {
        todo!()
    }

    /// Uploads a user file to a GridFS bucket. The application supplies a custom file id. Uses the
    /// `tokio` crate's `AsyncRead` trait for the `source`.
    pub async fn upload_from_tokio_reader_with_id(
        &self,
        id: Bson,
        filename: String,
        source: impl tokio::io::AsyncRead,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) {
        todo!()
    }

    /// Uploads a user file to a GridFS bucket. The application supplies a custom file id. Uses the
    /// `futures-0.3` crate's `AsyncRead` trait for the `source`.
    pub async fn upload_from_futures_0_3_reader_with_id(
        &self,
        id: Bson,
        filename: String,
        source: impl futures_util::AsyncRead,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) {
        todo!()
    }

    /// Uploads a user file to a GridFS bucket. The driver generates a unique [`Bson::ObjectId`] for
    /// the file id. Uses the `tokio` crate's `AsyncRead` trait for the `source`.
    pub async fn upload_from_tokio_reader(
        &self,
        filename: String,
        source: impl tokio::io::AsyncRead,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) {
        self.upload_from_tokio_reader_with_id(
            Bson::ObjectId(ObjectId::new()),
            filename,
            source,
            options,
        )
        .await
    }

    /// Uploads a user file to a GridFS bucket. The driver generates a unique [`Bson::ObjectId`] for
    /// the file id. Uses the `futures-0.3` crate's `AsyncRead` trait for the `source`.
    pub async fn upload_from_futures_0_3_reader(
        &self,
        filename: String,
        source: impl futures_util::AsyncRead,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) {
        self.upload_from_futures_0_3_reader_with_id(
            Bson::ObjectId(ObjectId::new()),
            filename,
            source,
            options,
        )
        .await
    }

    /// Opens a [`GridFsUploadStream`] that the application can write the contents of the file to.
    /// The driver generates a unique [`Bson::ObjectId`] for the file id.
    ///
    /// Returns a [`GridFsUploadStream`] to which the application will write the contents.
    pub async fn open_upload_stream(
        &self,
        filename: String,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<GridFsUploadStream> {
        self.open_upload_stream_with_id(Bson::ObjectId(ObjectId::new()), filename, options)
            .await
    }

    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `id`.
    pub async fn open_download_stream(&self, id: Bson) -> Result<GridFsDownloadStream> {
        todo!()
    }

    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `filename` and the revision
    /// in `options`.
    pub async fn open_download_stream_by_name(
        &self,
        filename: String,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) -> Result<GridFsDownloadStream> {
        todo!()
    }

    /// Deletes the [`FilesCollectionDocument`] with the given `id `and its associated chunks from
    /// this bucket.
    pub async fn delete(&self, id: Bson) -> Result<()> {
        let delete_options = DeleteOptions::builder()
            .write_concern(self.write_concern().cloned())
            .build();

        let delete_result = self
            .files()
            .delete_one(doc! { "_id": id.clone() }, delete_options.clone())
            .await?;
        // Delete chunks regardless of whether a file was found. This will remove any possibly
        // orphaned chunks.
        self.chunks()
            .delete_many(doc! { "files_id": id.clone() }, delete_options)
            .await?;

        if delete_result.deleted_count == 0 {
            return Err(ErrorKind::GridFS {
                message: format!("no file matching id {} was found", id),
            }
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
        let mut find_options = options.into().map(FindOptions::from);
        resolve_options!(self, find_options, [read_concern, selection_criteria]);
        self.files().find(filter, find_options).await
    }

    /// Renames the file with the given 'id' to the provided `new_filename`.
    pub async fn rename(&self, id: Bson, new_filename: impl AsRef<str>) -> Result<()> {
        let update_options = UpdateOptions::builder()
            .write_concern(self.write_concern().cloned())
            .build();

        self.files()
            .update_one(
                doc! { "_id": id },
                doc! { "$set": { "filename": new_filename.as_ref() } },
                update_options,
            )
            .await?;

        Ok(())
    }

    /// Drops all of the files and their associated chunks in this bucket.
    pub async fn drop(&self) -> Result<()> {
        let drop_options = DropCollectionOptions::builder()
            .write_concern(self.write_concern().cloned())
            .build();

        self.files().drop(drop_options.clone()).await?;
        self.chunks().drop(drop_options).await?;

        Ok(())
    }
}
