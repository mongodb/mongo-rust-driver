use core::task::{Context, Poll};
use std::{
    io::{self, Result},
    marker::PhantomPinned,
    pin::Pin,
};

use crate::{
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
    selection_criteria::{ReadPreference, SelectionCriteria},
    Database,
};

use bson::{oid::ObjectId, DateTime, Document};
use serde::Deserialize;
use typed_builder::TypedBuilder;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Contains the options for creating a [`GridFsBucket`].
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
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
    pub read_preference: Option<ReadPreference>,
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

// A collection in which information about stored files is stored. There will be one files
// collection document per stored file.
struct FilesCollectionDocument<T: Eq + PartialEq + Copy> {
    id: T,
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

pub struct GridFsStream {
    _pin: PhantomPinned,
}

impl AsyncRead for GridFsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        todo!()
    }
}

impl AsyncWrite for GridFsStream {
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
    pub fn read_preference(&self) -> Option<&ReadPreference> {
        if let Some(options) = &self.options {
            if let Some(ref rp) = options.read_preference {
                return Some(rp);
            }
        }
        if let Some(SelectionCriteria::ReadPreference(ref rp)) = self.db.selection_criteria() {
            Some(rp)
        } else {
            None
        }
    }

    /// Opens a [`GridFsStream`] that the application can write the contents of the file to.
    /// The application provides a custom file id.
    ///
    /// Returns a [`GridFsStream`] to which the application will write the contents.
    pub fn open_upload_stream_with_id<T>(
        &self,
        id: T,
        filename: String,
        options: GridFsUploadOptions,
    ) -> GridFsStream {
        todo!()
    }

    /// Uploads a user file to a GridFS bucket. The application supplies a custom file id.
    pub fn upload_from_stream_with_id<T>(
        &self,
        id: T,
        filename: String,
        source: GridFsStream,
        option: GridFsUploadOptions,
    ) {
        todo!()
    }

    /// Opens and returns a [`GridFsStream`] from which the application can read
    /// the contents of the stored file specified by `id`.
    pub fn open_download_stream<T>(&self, id: T) -> GridFsStream {
        todo!()
    }

    /// Opens and returns a [`GridFsStream`] from which the application can read
    /// the contents of the stored file specified by `filename` and the revision
    /// in `options`.
    pub fn open_download_stream_by_name(
        &self,
        filename: String,
        options: GridFsDownloadByNameOptions,
    ) -> GridFsStream {
        todo!()
    }

    /// Downloads the contents of the stored file specified by `id` and writes
    /// the contents to the destination [`GridFsStream`].
    pub fn download_to_stream<T>(&self, id: T, destination: GridFsStream) {
        todo!()
    }

    /// Downloads the contents of the stored file specified by `filename` and by
    /// the revision in `options` and writes the contents to the destination
    /// [`GridFsStream`].
    pub fn download_to_stream_by_name(
        &self,
        filename: String,
        destination: GridFsStream,
        options: GridFsDownloadByNameOptions,
    ) {
        todo!()
    }

    /// Given an `id`, deletes the stored file's files collection document and
    /// associated chunks from a [`GridFsBucket`].
    pub fn delete<T>(&self, id: T) {
        todo!()
    }

    /// Finds and returns the files collection documents that match the filter.
    pub fn find<T>(&self, filter: Document, options: GridFsBucketOptions) -> Result<Cursor<T>> {
        todo!()
    }

    /// Renames the stored file with the specified `id`.
    pub fn rename<T>(&self, id: T, new_filename: String) {
        todo!()
    }

    /// Drops the files associated with this bucket.
    pub fn drop(&self) {
        todo!()
    }
}
