//! Contains the functionality for GridFS operations.

use std::io::{Read, Write};

use futures_util::{AsyncReadExt, AsyncWriteExt};

use crate::{
    bson::Bson,
    error::Result,
    gridfs::{
        GridFsBucket as AsyncGridFsBucket,
        GridFsDownloadStream as AsyncGridFsDownloadStream,
        GridFsUploadStream as AsyncGridFsUploadStream,
    },
    options::{
        GridFsDownloadByNameOptions,
        GridFsUploadOptions,
        ReadConcern,
        SelectionCriteria,
        WriteConcern,
    },
};

pub use crate::gridfs::FilesCollectionDocument;

/// A `GridFsBucket` provides the functionality for storing and retrieving binary BSON data that
/// exceeds the 16 MiB size limit of a MongoDB document. Users may upload and download large amounts
/// of data, called files, to the bucket. When a file is uploaded, its contents are divided into
/// chunks and stored in a chunks collection. A corresponding [`FilesCollectionDocument`] is also
/// stored in a files collection. When a user downloads a file, the bucket finds and returns the
/// data stored in its chunks.
///
/// `GridFsBucket` uses [`std::sync::Arc`] internally, so it can be shared safely across threads or
/// async tasks.
pub struct GridFsBucket {
    pub(crate) async_bucket: AsyncGridFsBucket,
}

impl GridFsBucket {
    pub(crate) fn new(async_bucket: AsyncGridFsBucket) -> Self {
        Self { async_bucket }
    }

    /// Gets the read concern of the `GridFsBucket`.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.async_bucket.read_concern()
    }

    /// Gets the write concern of the `GridFsBucket`.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.async_bucket.write_concern()
    }

    /// Gets the read preference of the `GridFsBucket`.
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.async_bucket.selection_criteria()
    }

    /// Renames the file with the given `id` to `new_filename`. This method returns an error if the
    /// `id` does not match any files in the bucket.
    pub fn rename(&self, id: Bson, new_filename: impl AsRef<str>) -> Result<()> {
        crate::sync::TOKIO_RUNTIME.block_on(self.async_bucket.rename(id, new_filename))
    }

    /// Removes all of the files and their associated chunks from this bucket.
    pub fn drop(&self) -> Result<()> {
        crate::sync::TOKIO_RUNTIME.block_on(self.async_bucket.drop())
    }
}

/// A stream from which a file stored in a GridFS bucket can be downloaded.
///
/// # Downloading from the Stream
/// The `GridFsDownloadStream` type implements [`std::io::Read`].
///
/// ```rust
/// # use mongodb::{bson::Bson, error::Result, sync::gridfs::{GridFsBucket, GridFsDownloadStream}};
/// # fn download_example(bucket: GridFsBucket, id: Bson) -> Result<()> {
/// use std::io::Read;
///
/// let mut buf = Vec::new();
/// let mut download_stream = bucket.open_download_stream(id)?;
/// download_stream.read_to_end(&mut buf)?;
/// # Ok(())
/// # }
/// ```
pub struct GridFsDownloadStream {
    async_stream: AsyncGridFsDownloadStream,
}

impl Read for GridFsDownloadStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        crate::sync::TOKIO_RUNTIME.block_on(self.async_stream.read(buf))
    }
}

impl GridFsDownloadStream {
    fn new(async_stream: AsyncGridFsDownloadStream) -> Self {
        Self { async_stream }
    }
}

// Download API
impl GridFsBucket {
    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `id`.
    pub fn open_download_stream(&self, id: Bson) -> Result<GridFsDownloadStream> {
        crate::sync::TOKIO_RUNTIME
            .block_on(self.async_bucket.open_download_stream(id))
            .map(GridFsDownloadStream::new)
    }

    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `filename`.
    ///
    /// If there are multiple files in the bucket with the given filename, the `revision` in the
    /// options provided is used to determine which one to download. See the documentation for
    /// [`GridFsDownloadByNameOptions`] for details on how to specify a revision. If no revision is
    /// provided, the file with `filename` most recently uploaded will be downloaded.
    pub fn open_download_stream_by_name(
        &self,
        filename: impl AsRef<str>,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) -> Result<GridFsDownloadStream> {
        crate::sync::TOKIO_RUNTIME
            .block_on(
                self.async_bucket
                    .open_download_stream_by_name(filename, options),
            )
            .map(GridFsDownloadStream::new)
    }
}

/// A stream to which bytes can be written to be uploaded to a GridFS bucket.
///
/// # Uploading to the Stream
///  The `GridFsUploadStream` type implements [`std::io::Write`].
///
/// Bytes can be written to the stream using the write methods in the `Write` trait. When
/// `close` is invoked on the stream, any remaining bytes in the buffer are written to the chunks
/// collection and a corresponding [`FilesCollectionDocument`] is written to the files collection.
/// It is an error to write to, abort, or close the stream after `close` has been called.
///
/// ```rust
/// # use mongodb::{error::Result, sync::gridfs::{GridFsBucket, GridFsUploadStream}};
/// # fn upload_example(bucket: GridFsBucket) -> Result<()> {
/// use std::io::Write;
///
/// let bytes = vec![0u8; 100];
/// let mut upload_stream = bucket.open_upload_stream("example_file", None);
/// upload_stream.write_all(&bytes[..])?;
/// upload_stream.close()?;
/// # Ok(())
/// # }
/// ```
///
/// # Aborting the Stream
/// A stream can be aborted by calling the `abort` method. This will remove any chunks associated
/// with the stream from the chunks collection. It is an error to write to, abort, or close the
/// stream after `abort` has been called.
///
/// ```rust
/// # use mongodb::{error::Result, sync::gridfs::{GridFsBucket, GridFsUploadStream}};
/// # fn abort_example(bucket: GridFsBucket) -> Result<()> {
/// use std::io::Write;
///
/// let bytes = vec![0u8; 100];
/// let mut upload_stream = bucket.open_upload_stream("example_file", None);
/// upload_stream.write_all(&bytes[..])?;
/// upload_stream.abort()?;
/// # Ok(())
/// # }
/// ```
///
/// In the event of an error during any operation on the `GridFsUploadStream`, any chunks associated
/// with the stream will be removed from the chunks collection. Any subsequent attempts to write to,
/// abort, or close the stream will return an error.
///
/// If a `GridFsUploadStream` is dropped prior to `abort` or `close` being called, its [`Drop`]
/// implementation will remove any chunks associated with the stream from the chunks collection.
/// Users should prefer calling `abort` explicitly to relying on the `Drop` implementation in order
/// to inspect the result of the delete operation.
///
/// # Flushing the Stream
/// Because all chunks besides the final chunk of a file must be exactly `chunk_size_bytes`, calling
/// [`flush`](std::io::Write::flush) is not guaranteed to flush all bytes to the chunks collection.
/// Any remaining buffered bytes will be written to the chunks collection upon a call to `close`.
pub struct GridFsUploadStream {
    async_stream: AsyncGridFsUploadStream,
}

impl GridFsUploadStream {
    /// Gets the stream's unique [`Bson`] identifier. This value will be the `id` field for the
    /// [`FilesCollectionDocument`] uploaded to the files collection when the stream is closed.
    pub fn id(&self) -> &Bson {
        self.async_stream.id()
    }

    /// Closes the stream, writing any buffered bytes to the chunks collection and a corresponding
    /// [`FilesCollectionDocument`] to the files collection. If an error occurs during either of
    /// these steps, the chunks associated with this stream are deleted. It is an error to write to,
    /// abort, or close the stream after this method has been called.
    pub fn close(&mut self) -> std::io::Result<()> {
        crate::sync::TOKIO_RUNTIME.block_on(self.async_stream.close())
    }

    /// Aborts the stream, discarding any chunks that have already been written to the chunks
    /// collection. Once this method has been called, it is an error to attempt to write to, abort,
    /// or close the stream.
    pub fn abort(&mut self) -> Result<()> {
        crate::sync::TOKIO_RUNTIME.block_on(self.async_stream.abort())
    }
}

impl Write for GridFsUploadStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        crate::sync::TOKIO_RUNTIME.block_on(self.async_stream.write(buf))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        crate::sync::TOKIO_RUNTIME.block_on(self.async_stream.flush())
    }
}

// Upload API
impl GridFsBucket {
    /// Creates and returns a [`GridFsUploadStream`] that the application can write the contents of
    /// the file to. This method generates a unique [`ObjectId`](crate::bson::oid::ObjectId) for the
    /// corresponding [`FilesCollectionDocument`]'s `id` field that can be accessed via the
    /// stream's `id` method.
    pub fn open_upload_stream(
        &self,
        filename: impl AsRef<str>,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> GridFsUploadStream {
        let async_stream = self.async_bucket.open_upload_stream(filename, options);
        GridFsUploadStream { async_stream }
    }

    /// Opens a [`GridFsUploadStream`] that the application can write the contents of the file to.
    /// The provided `id` will be used for the corresponding [`FilesCollectionDocument`]'s `id`
    /// field.
    pub fn open_upload_stream_with_id(
        &self,
        id: Bson,
        filename: impl AsRef<str>,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> GridFsUploadStream {
        let async_stream = self
            .async_bucket
            .open_upload_stream_with_id(id, filename, options);
        GridFsUploadStream { async_stream }
    }
}
