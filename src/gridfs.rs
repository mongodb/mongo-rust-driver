pub mod options;

use core::task::{Context, Poll};
use std::{
    pin::Pin,
    io::{self, Error}
};

use crate::{
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
    selection_criteria::SelectionCriteria,
    Database,
};

use options::*;

use bson::{oid::ObjectId, Bson, DateTime, Document};
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
    pub(crate) db: Database,
    pub(crate) options: Option<GridFsBucketOptions>,
}

// TODO: RUST-1399 Add documentation and example code for this struct.
pub struct GridFsUploadStream {
    pub id: Bson,
}

impl AsyncWrite for GridFsUploadStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8]
    ) -> Poll<Result<usize, Error>> {
        todo!()
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Error>> {
        todo!()
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<Result<(), Error>> {
        todo!()
    }
}

pub struct GridFsDownloadStream {
    pub id: Bson,
}

impl AsyncRead for GridFsDownloadStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>
    ) -> Poll<io::Result<()>> {
        todo!()
    }
}

impl GridFsBucket {
    /// Gets the read concern of the [`GridFsBucket`].
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        if let Some(options) = &self.options {
            if let Some(ref rc) = options.read_concern {
                return Some(rc);
            }
        }
        self.db.read_concern()
    }

    /// Gets the write concern of the [`GridFsBucket`].
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        if let Some(options) = &self.options {
            if let Some(ref wc) = options.write_concern {
                return Some(wc);
            }
        }
        self.db.write_concern()
    }

    /// Gets the read preference of the [`GridFsBucket`].
    pub fn read_preference(&self) -> Option<&SelectionCriteria> {
        if let Some(options) = &self.options {
            if options.read_preference.is_some() {
                return options.read_preference.as_ref();
            }
        }
        self.db.selection_criteria()
    }

    /// Opens a [`GridFsUploadStream`] that the application can write the contents of the file to.
    /// The application provides a custom file id.
    ///
    /// Returns a [`GridFsUploadStream`] to which the application will write the contents.
    pub fn open_upload_stream_with_id(
        &self,
        id: Bson,
        filename: String,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> GridFsUploadStream {
        todo!()
    }

    /// Opens a [`GridFsUploadStream`] that the application can write the contents of the file to.
    /// The driver generates a unique [`Bson::ObjectId`] for the file id.
    ///
    /// Returns a [`GridFsUploadStream`] to which the application will write the contents.
    pub fn open_upload_stream(
        &self,
        filename: String,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> GridFsUploadStream {
        self.open_upload_stream_with_id(Bson::ObjectId(ObjectId::new()), filename, options)
    }

    /// Uploads a user file to a GridFS bucket. The application supplies a custom file id.
    pub fn upload_from_stream_with_id(
        &self,
        id: Bson,
        filename: String,
        source: impl AsyncRead,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) {
        todo!()
    }

    /// Uploads a user file to a GridFS bucket. The driver generates a unique [`Bson::ObjectId`] for
    /// the file id.
    pub fn upload_from_stream(
        &self,
        filename: String,
        source: impl AsyncRead,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) {
        self.upload_from_stream_with_id(Bson::ObjectId(ObjectId::new()), filename, source, options)
    }

    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `id`.
    pub fn open_download_stream(&self, id: Bson) -> GridFsDownloadStream {
        todo!()
    }

    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `filename` and the revision
    /// in `options`.
    pub fn open_download_stream_by_name(
        &self,
        filename: String,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) -> GridFsDownloadStream {
        todo!()
    }

    /// Downloads the contents of the stored file specified by `id` and writes
    /// the contents to the destination [`GridFsDownloadStream`].
    pub fn download_to_stream<T>(&self, id: Bson, destination: impl AsyncWrite) {
        todo!()
    }

    /// Downloads the contents of the stored file specified by `filename` and by
    /// the revision in `options` and writes the contents to the destination
    /// [`GridFsStream`].
    pub fn download_to_stream_by_name(
        &self,
        filename: String,
        destination: impl AsyncWrite,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) {
        todo!()
    }

    /// Given an `id`, deletes the stored file's files collection document and
    /// associated chunks from a [`GridFsBucket`].
    pub fn delete(&self, id: Bson) {
        todo!()
    }

    /// Finds and returns the files collection documents that match the filter.
    pub fn find(
        &self,
        filter: Document,
        options: impl Into<Option<GridFsBucketOptions>>,
    ) -> io::Result<Cursor<FilesCollectionDocument>> {
        todo!()
    }

    /// Renames the stored file with the specified `id`.
    pub fn rename(&self, id: Bson, new_filename: String) {
        todo!()
    }

    /// Drops the files associated with this bucket.
    pub fn drop(&self) {
        todo!()
    }
}
