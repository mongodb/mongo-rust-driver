pub mod options;

use core::task::{Context, Poll};
use std::{io, pin::Pin};

use crate::{
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
    error::{Error, Result},
    selection_criteria::SelectionCriteria,
    Database,
};

use options::*;

use bson::{oid::ObjectId, Bson, DateTime, Document};
use futures_util;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

// Contained in a "chunks" collection for each user file
struct Chunk {
    id: ObjectId,
    files_id: Bson,
    n: i32,
    // default size is 255 KiB
    data: Vec<u8>,
}

// A collection in which information about stored files is stored. There will be one files
// collection document per stored file.
pub struct FilesCollectionDocument {
    id: Bson,
    length: i64,
    chunk_size: i32,
    upload_date: DateTime,
    filename: String,
    metadata: Document,
}

/// Struct for storing GridFS managed files within a [`Database`].
pub struct GridFsBucket {
    // Contains a "chunks" collection
    pub(crate) bucket_name: String,
    pub(crate) db: Database,
    pub(crate) chunk_size_bytes: i32,
    pub(crate) read_concern: Option<ReadConcern>,
    pub(crate) write_concern: Option<WriteConcern>,
    pub(crate) read_preference: Option<SelectionCriteria>,
}

// TODO: RUST-1399 Add documentation and example code for this struct.
pub struct GridFsUploadStream {
    pub id: Bson,
}

impl GridFsUploadStream {
    /// Consumes the stream and uploads data in the stream to the server.
    pub fn finish(self) {
        todo!()
    }

    /// Aborts the upload and discards the upload stream.
    pub fn abort(self) {
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

pub struct GridFsDownloadStream {
    pub id: Bson,
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
    /// Gets the read concern of the [`GridFsBucket`].
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.read_concern.as_ref()
    }

    /// Gets the write concern of the [`GridFsBucket`].
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.write_concern.as_ref()
    }

    /// Gets the read preference of the [`GridFsBucket`].
    pub fn read_preference(&self) -> Option<&SelectionCriteria> {
        self.read_preference.as_ref()
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
    /// `tokio` runtime.
    pub async fn upload_from_stream_with_id_tokio(
        &self,
        id: Bson,
        filename: String,
        source: impl tokio::io::AsyncRead,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) {
        todo!()
    }

    /// Uploads a user file to a GridFS bucket. The application supplies a custom file id. Uses the
    /// `futures` crate.
    pub async fn upload_from_stream_with_id_futures(
        &self,
        id: Bson,
        filename: String,
        source: impl futures_util::io::AsyncRead,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) {
        todo!()
    }

    /// Uploads a user file to a GridFS bucket. The driver generates a unique [`Bson::ObjectId`] for
    /// the file id. Uses the `tokio` runtime.
    pub async fn upload_from_stream_tokio(
        &self,
        filename: String,
        source: impl AsyncRead,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) {
        self.upload_from_stream_with_id_tokio(
            Bson::ObjectId(ObjectId::new()),
            filename,
            source,
            options,
        )
        .await
    }

    /// Uploads a user file to a GridFS bucket. The driver generates a unique [`Bson::ObjectId`] for
    /// the file id. Uses the `futures` crate.
    pub async fn upload_from_stream_futures(
        &self,
        filename: String,
        source: impl futures_util::io::AsyncRead,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) {
        self.upload_from_stream_with_id_futures(
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
    /// the contents to the destination [`GridFsDownloadStream`]. Uses the `tokio` runtime.
    pub async fn download_to_stream_tokio(
        &self,
        id: Bson,
        destination: impl tokio::io::AsyncWrite,
    ) {
        todo!()
    }

    /// Downloads the contents of the stored file specified by `id` and writes
    /// the contents to the destination [`GridFsDownloadStream`]. Uses the `futures` crate.
    pub async fn download_to_stream_futures(
        &self,
        id: Bson,
        destination: impl futures_util::io::AsyncWrite,
    ) {
        todo!()
    }

    /// Downloads the contents of the stored file specified by `filename` and by
    /// the revision in `options` and writes the contents to the destination
    /// [`GridFsStream`]. Uses the `tokio` runtime.
    pub async fn download_to_stream_by_name_tokio(
        &self,
        filename: String,
        destination: impl tokio::io::AsyncWrite,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) {
        todo!()
    }

    /// Downloads the contents of the stored file specified by `filename` and by
    /// the revision in `options` and writes the contents to the destination
    /// [`GridFsStream`]. Uses the `futures` crate.
    pub async fn download_to_stream_by_name_futures(
        &self,
        filename: String,
        destination: impl futures_util::io::AsyncWrite,
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
    ) -> io::Result<Cursor<FilesCollectionDocument>> {
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
