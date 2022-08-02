pub mod options;

use core::task::{Context, Poll};
use std::{pin::Pin, sync::Arc};

use crate::{
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
    error::{Error, ErrorKind, Result},
    options::{FindOneOptions, FindOptions},
    selection_criteria::SelectionCriteria,
    Collection,
    Database,
};

use tokio::io::ReadBuf;

use futures_util::stream::StreamExt;
use options::*;

use bson::{doc, oid::ObjectId, Bson, DateTime, Document};
use futures_util;
use serde::{Deserialize, Serialize};

// Contained in a "chunks" collection for each user file
#[derive(Deserialize, Serialize)]
pub struct Chunk {
    pub id: ObjectId,
    pub files_id: Bson,
    pub n: i32,
    // default size is 255 KiB
    pub data: Vec<u8>,
}

// A collection in which information about stored files is stored. There will be one files
// collection document per stored file.
#[derive(Deserialize, Serialize)]
pub struct FilesCollectionDocument {
    pub id: Bson,
    pub length: i64,
    pub chunk_size: u32,
    pub upload_date: DateTime,
    pub filename: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Document>,
}

/// Struct for storing GridFS managed files within a [`Database`].
pub struct GridFsBucket {
    // Contains a "chunks" collection
    pub(crate) bucket_name: String,
    pub(crate) db: Database,
    pub(crate) chunk_size_bytes: u32,
    pub(crate) read_concern: Option<ReadConcern>,
    pub(crate) write_concern: Option<WriteConcern>,
    pub(crate) read_preference: Option<SelectionCriteria>,
}

// TODO: RUST-1399 Add documentation and example code for this struct.
pub struct GridFsUploadStream {
    pub id: Bson,
    pub files_id: Bson,
    pub length: i64,
    pub filename: String,
    pub chunk_size: u32,
    pub metadata: Option<Document>,
    pub files: Collection<FilesCollectionDocument>,
}

impl GridFsUploadStream {
    /// Consumes the stream and inserts the FilesCollectionDocument into the files collection. No further writes to the stream are allowed after this function call.
    pub async fn finish(self) -> Result<()> {
        let file = FilesCollectionDocument {
            id: self.files_id,
            length: self.length,
            chunk_size: self.chunk_size,
            upload_date: DateTime::now(),
            filename: self.filename,
            metadata: self.metadata,
        };
        self.files.insert_one(file, None).await?;
        Ok(())
    }

    /// Aborts the upload and discards any uploaded chunks.
    pub async fn abort(self) -> Result<()> {
        self.files.delete_many(doc! {"_id": self.files_id}, None).await?;
        Ok(())
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
    pub file: FilesCollectionDocument,
    pub chunks: Collection<Chunk>,
    pub cursor: Cursor<Chunk>,
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
        files_id: Bson,
        filename: String,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> GridFsUploadStream {
        let files: Collection<FilesCollectionDocument> =
            self.db.collection(&(format!("{}.files", self.bucket_name)));
        let options: Option<GridFsUploadOptions> = options.into();
        let chunk_size = if let Some(ref opts) = options {
            opts.chunk_size_bytes.unwrap_or(self.chunk_size_bytes)
        } else {
            self.chunk_size_bytes
        };
        let metadata = if let Some(ref opts) = options {
            opts.metadata.clone()
        } else {
            None
        };
        GridFsUploadStream {
            id: Bson::ObjectId(ObjectId::new()),
            files_id,
            length: 0,
            filename,
            chunk_size,
            files,
            metadata,
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
    /// `tokio` runtime.
    pub async fn upload_from_stream_with_id_tokio<T: tokio::io::AsyncRead + std::marker::Unpin>(
        &self,
        files_id: Bson,
        filename: String,
        source: &mut T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<()> {
        use tokio::io::AsyncReadExt;
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
                        chunks.delete_many(doc! { "files_id": &files_id }, None).await?;
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
    /// `futures` crate.
    pub async fn upload_from_stream_with_id_futures<T: futures_util::io::AsyncRead + std::marker::Unpin>(
        &self,
        files_id: Bson,
        filename: String,
        source: &mut T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<()> {
        use futures_util::AsyncReadExt;
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
                        chunks.delete_many(doc! { "files_id": &files_id }, None).await?;
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

    /// Uploads a user file to a GridFS bucket. The driver generates a unique [`Bson::ObjectId`] for
    /// the file id. Uses the `tokio` runtime.
    pub async fn upload_from_stream_tokio<T: tokio::io::AsyncRead + std::marker::Unpin>(
        &self,
        filename: String,
        source: &mut T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<()> {
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
    pub async fn upload_from_stream_futures<T: futures_util::io::AsyncRead + std::marker::Unpin>(
        &self,
        filename: String,
        source: &mut T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<()> {
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

        let options = FindOptions::builder().sort(doc! { "n": -1 }).build();
        let cursor = chunks.find(doc! { "files_id": &id } , options).await?;

        Ok(GridFsDownloadStream {
            id,
            file,
            chunks,
            cursor,
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
        let options = FindOptions::builder().sort(doc! { "n": -1 }).build();
        let cursor = chunks.find(doc! { "files_id": &id } , options).await?;

        Ok(GridFsDownloadStream {
            id,
            file,
            chunks,
            cursor,
        })
    }


    pub async fn download_to_stream_common(
        &self,
        id: Bson,
        destination: impl tokio::io::AsyncWrite,
    ) -> Result<()> {
        let file = match self
            .db
            .collection::<FilesCollectionDocument>(&(format!("{}.files", self.bucket_name)))
            .find_one(doc! { "_id": &id }, None)
            .await? {
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
        
        if file.length == 0 {
            return Ok(())
        }

        let chunks = self
            .db
            .collection::<Chunk>(&(format!("{}.chunks", self.bucket_name)));

        let options = FindOptions::builder().sort(doc! { "n": -1 }).build();
        let mut cursor = chunks.find(doc! { "files_id": &id } , options).await?;
        let mut n = 0;
        while let Some(c) = cursor.next().await {
            let chunk = c?;
            if chunk.n != n {
                let labels: Option<Vec<_>> = None;
                return Err(Error::new(ErrorKind::InvalidResponse { message: "missing chunks in file".to_string() }, labels));
            } else if chunk.data.len() != self.chunk_size_bytes && !cursor.is_exhausted() {
                let labels: Option<Vec<_>> = None;
                return Err(Error::new(ErrorKind::InvalidResponse { message: "received invalid chunk".to_string() }, labels));
            }
            destination.write(chunk.data);
        }
        Ok(())

    }

    /// Downloads the contents of the stored file specified by `id` and writes
    /// the contents to the destination [`GridFsDownloadStream`]. Uses the `tokio` runtime.
    pub async fn download_to_stream_tokio(
        &self,
        id: Bson,
        destination: impl tokio::io::AsyncWrite,
    ) -> Result<()> {
        self.download_to_stream_common(id, destination)
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
    pub async fn delete(&self, id: Bson) -> Result<()> {
        let file = match self
            .db
            .collection::<FilesCollectionDocument>(&(format!("{}.files", self.bucket_name)))
            .find_one(doc! { "_id": &id }, None)
            .await? {
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
        let chunks: Collection<Chunk> = self
        .db
        .collection(&(format!("{}.chunks", self.bucket_name)));
        chunks.delete_many(doc! { "files_id": id }, None).await?;
        Ok(())
    }

    /// Finds and returns the files collection documents that match the filter.
    pub async fn find(
        &self,
        filter: Document,
        options: impl Into<Option<GridFsFindOptions>>,
    ) -> Result<Cursor<FilesCollectionDocument>> {
        self.db.collection::<FilesCollectionDocument>(&(format!("{}.files", self.bucket_name))).find(filter, None).await
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
