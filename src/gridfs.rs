pub mod options;

use core::task::{Context, Poll};
use std::pin::Pin;

use crate::{
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
    selection_criteria::SelectionCriteria,
    Database,
};

use bson::{oid::ObjectId, DateTime, Document};
use serde::Deserialize;
use typed_builder::TypedBuilder;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Contains the options for creating a [`GridFsBucket`].
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct GridFsBucketOptions {
    /// The bucket name. Defaults to 'fs'.
    pub bucket_name: Option<String>,

    /// The chunk size in bytes. Defaults to 255 KiB.
    pub chunk_size_bytes: Option<i32>,

    /// The write concern. Defaults to the write concern of the database.
    pub write_concern: Option<WriteConcern>,

    /// The read concern. Defaults to the read concern of the database.
    pub read_concern: Option<ReadConcern>,

    /// The read preference. Defaults to the read preference of the database.
    #[serde(rename = "read_preference")]
    pub selection_criteria: Option<SelectionCriteria>,
}

/// Contains the options for creating a [`GridFsStream`] to upload a file to a
/// [`GridFsBucket`].
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct GridFsUploadOptions {
    /// The number of bytes per chunk of this file. Defaults to the chunk_size_bytes in the
    /// GridFsBucketOptions.
    pub chunk_size_bytes: Option<i32>,

    /// User data for the 'metadata' field of the files collection document.
    pub metadata: Option<Document>,
}

/// Contains the options for creating [`GridFsStream`] to retrieve a stored file
/// from a [`GridFsBucket`].
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct GridFsDownloadByNameOptions {
    /// Which revision (documents with the same filename and different `upload_date`)
    /// of the file to retrieve. Defaults to -1 (the most recent revision).
    ///
    /// Revision numbers are defined as follows:
    /// 0 = the original stored file
    /// 1 = the first revision
    /// 2 = the second revision
    /// etc...
    /// -2 = the second most recent revision
    /// -1 = the most recent revision
    pub revision: Option<i32>,
}

/// Contains the options for performing a find operation on a files collection.  
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct GridFsFindOptions {
    /// Enables writing to temporary files on the server. When set to true, the
    /// server can write temporary data to disk while executing the find operation
    /// on the files collection.
    pub allow_disk_use: Option<bool>,

    /// The number of documents to return per batch.
    pub batch_size: Option<i32>,

    /// The maximum number of documents to return.
    pub limit: Option<i32>,

    /// The maximum amount of time to allow the query to run.
    pub max_time_ms: Option<i64>,

    /// The server normally times out idle cursors after an inactivity period
    /// (10 minutes) to prevent excess memory use. Set this option to prevent that.
    pub no_cursor_timeout: Option<bool>,

    /// The number of documents to skip before returning.
    pub skip: i32,

    /// The order by which to sort results. Defaults to not sorting.
    pub sort: Option<Document>,
}

// Contained in a "chunks" collection for each user file
struct Chunk<T: Eq + Copy> {
    id: ObjectId,
    files_id: T,
    n: i32,
    // default size is 255 KiB
    data: Vec<u8>,
}

/// A collection in which information about stored files is stored. There will be one files
/// collection document per stored file.
#[derive(Serialize, Deserialize)]
pub struct FilesCollectionDocument {
    id: Bson,
    length: i64,
    chunk_size: u32,
    upload_date: DateTime,
    filename: String,
    metadata: Document,
}

/// Struct for storing GridFS managed files within a [`Database`].
pub struct GridFsBucket {
    // Contains a "chunks" collection
    pub(crate) db: Database,
    pub(crate) options: GridFsBucketOptions,
}

// TODO: RUST-1399 Add documentation and example code for this struct.
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

impl futures_util::io::AsyncRead for GridFsDownloadStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<core::result::Result<usize, futures_util::io::Error>> {
        todo!()
    }
}

impl GridFsBucket {
    /// Gets the read concern of the [`GridFsBucket`].
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.options.read_concern.as_ref()
    }

    /// Gets the write concern of the [`GridFsBucket`].
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.options.write_concern.as_ref()
    }

    /// Gets the selection criteria of the [`GridFsBucket`].
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.options.selection_criteria.as_ref()
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

    /// Downloads the contents of the stored file specified by `id` and writes
    /// the contents to the `destination`. Uses the `tokio` crate's `AsyncWrite`
    /// trait for the `destination`.
    pub async fn download_to_tokio_writer(
        &self,
        id: Bson,
        destination: impl tokio::io::AsyncWrite,
    ) {
        todo!()
    }

    /// Downloads the contents of the stored file specified by `id` and writes
    /// the contents to the `destination`. Uses the `futures-0.3` crate's `AsyncWrite`
    /// trait for the `destination`.
    pub async fn download_to_futures_0_3_writer(
        &self,
        id: Bson,
        destination: impl futures_util::AsyncWrite,
    ) {
        todo!()
    }

    /// Downloads the contents of the stored file specified by `filename` and by
    /// the revision in `options` and writes the contents to the `destination`. Uses the
    /// `tokio` crate's `AsyncWrite` trait for the `destination`.
    pub async fn download_to_tokio_writer_by_name(
        &self,
        filename: String,
        destination: impl tokio::io::AsyncWrite,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) {
        todo!()
    }

    /// Downloads the contents of the stored file specified by `filename` and by
    /// the revision in `options` and writes the contents to the `destination`. Uses the
    /// `futures-0.3` crate's `AsyncWrite` trait for the `destination`.
    pub async fn download_to_futures_0_3_writer_by_name(
        &self,
        filename: String,
        destination: impl futures_util::AsyncWrite,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) {
        todo!()
    }

    /// Given an `id`, deletes the stored file's files collection document and
    /// associated chunks from a [`GridFsBucket`].
    pub async fn delete(&self, id: Bson) {
        todo!()
    }

    /// Finds and returns the files collection documents that match the filter.
    pub async fn find(
        &self,
        filter: Document,
        options: impl Into<Option<GridFsBucketOptions>>,
    ) -> Result<Cursor<FilesCollectionDocument>> {
        todo!()
    }

    /// Renames the stored file with the specified `id`.
    pub async fn rename(&self, id: Bson, new_filename: String) {
        todo!()
    }

    /// Drops the files associated with this bucket.
    pub async fn drop(&self) {
        todo!()
    }
}
