pub mod options;

use core::task::{Context, Poll};
use std::{io, pin::Pin, sync::Arc};

use crate::{
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
    error::Result,
    selection_criteria::SelectionCriteria,
    Database,
    error::{Error, ErrorKind, Result},
    options::FindOneOptions,
    selection_criteria::SelectionCriteria,
    Collection,
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
    // default size is 255 KiB
    pub data: Vec<u8>,
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
    ) -> GridFsUploadStream {
        GridFsUploadStream {
            id,
            length: 0,
            filename,
            options: options.into(),
        }
    }

    /// Opens a [`GridFsUploadStream`] that the application can write the contents of the file to.
    /// The driver generates a unique [`Bson::ObjectId`] for the file id.
    ///
    /// Returns a [`GridFsUploadStream`] to which the application will write the contents.
    pub async fn open_upload_stream(
        &self,
        filename: String,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> GridFsUploadStream {
        self.open_upload_stream_with_id(Bson::ObjectId(ObjectId::new()), filename, options)
            .await
    }

    /// Uploads a user file to a GridFS bucket. The application supplies a custom file id. Uses the
    /// `tokio` crate's `AsyncRead` trait for the `source`.
    pub async fn upload_from_tokio_reader_with_id(
        &self,
        files_id: Bson,
        filename: String,
        source: &mut T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<()> {
        let options: GridFsUploadOptions = options
            .into()
            .map(Into::into)
            .unwrap_or_else(Default::default);
        let chunk_size = options.chunk_size_bytes.unwrap_or(self.chunk_size_bytes);
        let mut length = 0;
        let mut n = 0;
        // Get chunks collection
        let chunks: Collection<Chunk> = self
            .db
            .collection(&(format!("{}.chunks", self.bucket_name)));
        // Read data in, chunk_size_bytes at a time.
        let mut eof = false;
        while !eof {
            let mut buf = vec![0u8; chunk_size as usize];
            let mut curr_length = 0usize;
            while curr_length < chunk_size as usize {
                let bytes_read = match source.read(&mut buf[curr_length..]).await {
                    Ok(num) => num,
                    Err(e) => {
                        // clean up any uploaded chunks
                        self.abort_upload().await?;
                        let labels: Option<Vec<_>> = None;
                        return Err(Error::new(ErrorKind::Io(Arc::new(e)), labels));
                    }
                };
                curr_length += bytes_read;
                if bytes_read == 0 {
                    eof = true;
                    break;
                }
            }
            if curr_length == 0 {
                break;
            }
            let chunk = Chunk {
                id: ObjectId::new(),
                files_id: files_id.clone(),
                n,
                data: buf,
            };
            // Put chunk in chunks collection.
            chunks.insert_one(chunk, None).await?;
            length += curr_length;
            n += 1;
        }
        let files_collection: Collection<FilesCollectionDocument> =
            self.db.collection(&(format!("{}.files", self.bucket_name)));
        let file = FilesCollectionDocument {
            id: Bson::ObjectId(ObjectId::new()),
            length: length as i64,
            chunk_size,
            upload_date: DateTime::now(),
            filename,
            metadata: options.metadata,
        };
        files_collection.insert_one(file, None).await?;
        Ok(())
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
        use futures_util::{AsyncReadExt, AsyncWriteExt};
        sourc
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
        let file = match self
            .db
            .collection::<FilesCollectionDocument>(&(format!("{}.files", self.bucket_name)))
            .find_one(doc! { "_id": &id }, None)
            .await?
        {
            Some(fcd) => fcd,
            None => {
                let labels: Option<Vec<_>> = None;
                return Err(Error::new(
                    ErrorKind::InvalidArgument {
                        message: format!("couldn't find file with id {}", &id),
                    },
                    labels,
                ));
            }
        };

        let chunks = self
            .db
            .collection::<Chunk>(&(format!("{}.chunks", self.bucket_name)));

        Ok(GridFsDownloadStream {
            id,
            file,
            chunks,
            cursor: None,
        })
    }

    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `filename` and the revision
    /// in `options`.
    pub async fn open_download_stream_by_name(
        &self,
        filename: String,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) -> Result<GridFsDownloadStream> {
        let mut sort = doc! { "uploadDate": -1 };
        let mut skip: i32 = 0;
        if let Some(opts) = options.into() {
            if let Some(rev) = opts.revision {
                if rev >= 0 {
                    sort = doc! { "uploadDate": 1 };
                    skip = rev;
                } else {
                    skip = -rev - 1;
                }
            }
        }
        let options = FindOneOptions::builder()
            .sort(sort)
            .skip(skip as u64)
            .build();

        let file = match self
            .db
            .collection::<FilesCollectionDocument>(&(format!("{}.files", self.bucket_name)))
            .find_one(doc! { "filename": &filename }, options)
            .await?
        {
            Some(fcd) => fcd,
            None => {
                let labels: Option<Vec<_>> = None;
                return Err(Error::new(
                    ErrorKind::InvalidArgument {
                        message: format!("couldn't find file with name {}", &filename),
                    },
                    labels,
                ));
            }
        };

        let chunks = self
            .db
            .collection::<Chunk>(&(format!("{}.chunks", self.bucket_name)));
        let id = file.id.clone();

        Ok(GridFsDownloadStream {
            id,
            file,
            chunks,
            cursor: None,
        })
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

    async fn abort_upload(&self) -> Result<()> {
        todo!()
    }
}
