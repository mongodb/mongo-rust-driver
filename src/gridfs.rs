<<<<<<< HEAD
pub mod options;

use core::task::{Context, Poll};
use std::pin::Pin;
=======
use core::task::{Context, Poll};
use std::{
    io::{self, Result},
    marker::PhantomPinned,
    pin::Pin,
};
>>>>>>> 5780428 (push public api skeleton)

use crate::{
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
<<<<<<< HEAD
    error::Result,
    selection_criteria::SelectionCriteria,
    Database,
};
use bson::{oid::ObjectId, Bson, DateTime, Document};
use options::*;
use serde::{Deserialize, Serialize};
use tokio::io::ReadBuf;

pub const DEFAULT_BUCKET_NAME: &str = "fs";
pub const DEFAULT_CHUNK_SIZE_BYTES: u32 = 255 * 1024;

// Contained in a "chunks" collection for each user file
struct Chunk {
    id: ObjectId,
    files_id: Bson,
    n: u32,
=======
    selection_criteria::ReadPreference,
    Database,
};

use bson::{oid::ObjectId, DateTime, Document};
use serde::Deserialize;
use typed_builder::TypedBuilder;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct GridFSBucketOptions {
    bucket_name: Option<String>,
    chunk_size_bytes: Option<i32>,
    write_concern: Option<WriteConcern>,
    read_concern: Option<ReadConcern>,
    read_preference: Option<ReadPreference>,
}

pub struct GridFSUploadOptions {
    chunk_size_bytes: Option<i32>,
    metadata: Option<Document>,
}

pub struct GridFSDownloadByNameOptions {
    revision: Option<i32>,
}

#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct GridFSFindOptions {
    allow_disk_use: Option<bool>,
    batch_size: Option<i32>,
    limit: Option<i32>,
    max_time_ms: Option<i64>,
    no_cursor_timeout: Option<bool>,
    skip: i32,
    sort: Option<Document>,
}

// Contained in a "chunks" collection for each user file
pub struct Chunk<T: Eq + Copy> {
    id: ObjectId,
    files_id: T,
    n: i32,
>>>>>>> 5780428 (push public api skeleton)
    // default size is 255 KiB
    data: Vec<u8>,
}

<<<<<<< HEAD
/// A collection in which information about stored files is stored. There will be one files
/// collection document per stored file.
#[derive(Serialize, Deserialize)]
pub struct FilesCollectionDocument {
    id: Bson,
    length: i64,
    chunk_size: u32,
=======
// A collection in which information about stored files is stored. There will be one files
// collection document per stored file.
pub struct FilesCollectionDocument<T: Eq + PartialEq + Copy> {
    id: T,
    length: i64,
    chunk_size: i32,
>>>>>>> 5780428 (push public api skeleton)
    upload_date: DateTime,
    filename: String,
    metadata: Document,
}

<<<<<<< HEAD
/// Struct for storing GridFS managed files within a [`Database`].
pub struct GridFsBucket {
    // Contains a "chunks" collection
    pub(crate) db: Database,
    pub(crate) options: GridFsBucketOptions,
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
=======
pub struct GridFSBucket {
    // Contains a "chunks" collection
    pub db: Database,
    pub options: Option<GridFSBucketOptions>,
}

pub struct GridFSStream {
    _pin: PhantomPinned,
}

impl AsyncRead for GridFSStream {
>>>>>>> 5780428 (push public api skeleton)
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
<<<<<<< HEAD
    ) -> Poll<tokio::io::Result<()>> {
=======
    ) -> Poll<io::Result<()>> {
>>>>>>> 5780428 (push public api skeleton)
        todo!()
    }
}

<<<<<<< HEAD
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
=======
impl AsyncWrite for GridFSStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        todo!()
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        todo!()
    }
}

impl GridFSBucket {
    pub fn open_upload_stream_with_id<T>(
        &self,
        id: T,
        filename: String,
        options: GridFSUploadOptions,
    ) -> GridFSStream {
        todo!()
    }

    pub fn upload_from_stream_with_id<T>(
        &self,
        id: T,
        filename: String,
        source: GridFSStream,
        option: GridFSUploadOptions,
    ) {
        todo!()
    }

    pub fn open_download_stream<T>(&self, id: T) {
        todo!()
    }

    pub fn download_to_stream<T>(&self, id: T, destination: GridFSStream) {
        todo!()
    }

    pub fn delete<T>(&self, id: T) {
        todo!()
    }

    pub fn find<T>(&self, filter: Document, options: GridFSBucketOptions) -> Result<Cursor<T>> {
        todo!()
    }

    pub fn open_download_stream_by_name(
        &self,
        filename: String,
        options: GridFSDownloadByNameOptions,
    ) -> GridFSStream {
        todo!()
    }

    pub fn download_to_stream_by_name(
        &self,
        filename: String,
        destination: GridFSStream,
        options: GridFSDownloadByNameOptions,
>>>>>>> 5780428 (push public api skeleton)
    ) {
        todo!()
    }

<<<<<<< HEAD
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
=======
    pub fn rename<T>(&self, id: T, new_filename: String) {
        todo!()
    }

    pub fn drop(&self) {
>>>>>>> 5780428 (push public api skeleton)
        todo!()
    }
}
