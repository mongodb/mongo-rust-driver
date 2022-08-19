pub mod options;

use core::task::{Context, Poll};
use std::{
    cell::RefCell,
    future::Future,
    io::Read,
    pin::Pin,
    sync::Arc,
    thread::sleep,
    time::Duration,
};

use crate::{
    bson::{doc, oid::ObjectId, Binary, Bson, DateTime, Document},
    coll::options::{
        DeleteOptions,
        DropCollectionOptions,
        FindOneOptions,
        FindOptions,
        InsertOneOptions,
    },
    concern::{ReadConcern, WriteConcern},
    cursor::Cursor,
    error::{ErrorKind, Result},
    selection_criteria::SelectionCriteria,
    Collection,
    Database,
    IndexModel,
};
use futures_util;
use options::*;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncWriteExt, ReadBuf};

pub const DEFAULT_BUCKET_NAME: &'static str = "fs";
pub const DEFAULT_CHUNK_SIZE_BYTES: u32 = 255 * 1024;

// Contained in a "chunks" collection for each user file
#[derive(Debug, Deserialize, Serialize)]
pub struct Chunk {
    pub id: ObjectId,
    pub files_id: Bson,
    pub n: u32,
    // default size is 255 KiB
    pub data: Binary,
}

/// A collection in which information about stored files is stored. There will be one files
/// collection document per stored file.
#[derive(Debug, Deserialize, Serialize)]
pub struct FilesCollectionDocument {
    pub id: Bson,
    pub length: u64,
    pub chunk_size: u32,
    pub upload_date: DateTime,
    pub filename: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Document>,
}

#[derive(Debug)]
struct GridFsBucketInner {
    pub(crate) options: GridFsBucketOptions,
    pub(crate) files: Collection<FilesCollectionDocument>,
    pub(crate) chunks: Collection<Chunk>,
    pub(crate) created_indexes: bool,
}

/// Struct for storing GridFS managed files within a [`Database`].
#[derive(Debug, Clone)]
pub struct GridFsBucket {
    inner: Arc<GridFsBucketInner>,
}

// TODO: RUST-1395 Add documentation and example code for this struct.
pub struct GridFsUploadStream {
    bucket: GridFsBucket,
    files_id: Bson,
    length: RefCell<u64>,
    filename: String,
    options: GridFsUploadOptions,
    buffer: RefCell<Vec<u8>>,
    chunk_size: u32,
    n: RefCell<u32>,
}

impl GridFsUploadStream {
    /// Gets the file `id` for the stream.
    pub fn files_id(&self) -> &Bson {
        &self.files_id
    }

    /// Consumes the stream and inserts the `FilesCollectionDocument` into the files collection. No
    /// further writes to the stream are allowed following this function call.
    pub async fn finish(mut self) -> Result<()> {
        let options = InsertOneOptions::builder()
            .write_concern(self.bucket.write_concern().cloned())
            .build();
        let file = FilesCollectionDocument {
            id: self.files_id.clone(),
            length: *self.length.borrow(),
            chunk_size: self.options.chunk_size_bytes.unwrap(),
            upload_date: DateTime::now(),
            filename: self.filename.clone(),
            metadata: self.options.metadata.take(),
        };
        self.bucket.inner.files.insert_one(file, options).await?;
        self.shutdown().await?;
        Ok(())
    }

    /// Aborts the upload and discards any uploaded chunks.
    pub async fn abort(mut self) -> Result<()> {
        let options = DeleteOptions::builder()
            .write_concern(self.bucket.write_concern().cloned())
            .build();
        self.bucket
            .inner
            .chunks
            .delete_many(doc! { "files_id": self.files_id.clone() }, options)
            .await?;
        self.shutdown().await?;
        Ok(())
    }
}

impl tokio::io::AsyncWrite for GridFsUploadStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<tokio::io::Result<usize>> {
        println!("buf: {:?}", buf);
        self.buffer.borrow_mut().append(&mut Vec::from(buf));
        if self.buffer.borrow().len() < self.chunk_size as usize {
            println!("I'm ready!");
            return Poll::Ready(Ok(buf.len()));
        }

        let mut chunks = Vec::new();

        while let Some(data) = self
            .buffer
            .borrow()
            .chunks_exact(self.chunk_size as usize)
            .next()
        {
            let chunk = Chunk {
                id: ObjectId::new(),
                files_id: self.files_id.clone(),
                n: *self.n.borrow(),
                data: data.to_vec(),
            };
            chunks.push(chunk);
            *self.n.borrow_mut() += 1;
            *self.length.borrow_mut() += self.chunk_size as u64;
        }
        println!("chunks = {:?}", chunks);
        self.buffer
            .borrow_mut()
            .drain(..(chunks.len() * self.chunk_size as usize));
        let mut fut = Box::pin(self.chunks.insert_many(chunks.into_iter(), None));
        match fut.as_mut().poll(cx) {
            Poll::Ready(result) => match result {
                Ok(_) => return Poll::Ready(Ok(buf.len())),
                Err(e) => {
                    return Poll::Ready(std::io::Result::Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        e,
                    )))
                }
            },
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
        let chunk = Chunk {
            id: ObjectId::new(),
            files_id: self.files_id.clone(),
            n: *self.n.borrow(),
            data: self.buffer.borrow().to_vec(),
        };
        let mut fut = Box::pin(self.chunks.insert_one(chunk, None));

        loop {
            match fut.as_mut().poll(cx) {
                Poll::Ready(result) => match result {
                    Ok(_) => {
                        assert_eq!(4, 5);

                        *self.n.borrow_mut() += 1;
                        *self.length.borrow_mut() += self.buffer.borrow().len() as u64;
                        self.buffer.borrow_mut().drain(..);
                        return Poll::Ready(Ok(()));
                    }
                    Err(e) => {
                        return Poll::Ready(std::io::Result::Err(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            e,
                        )))
                    }
                },
                Poll::Pending => {
                    sleep(Duration::from_millis(100));
                    println!("x");
                    continue;
                }
            }
        }
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
    bucket: GridFsBucket,
    files_id: Bson,
    file: FilesCollectionDocument,
    cursor: Cursor<Chunk>,
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
        if self.cursor.borrow().is_exhausted() {
            return Poll::Ready(Ok(()));
        }
        let mut cursor = self.cursor.borrow_mut();
        let mut fut: Pin<Box<dyn Future<Output = Result<bool>>>> = Box::pin(cursor.advance());
        match fut.as_mut().poll(cx) {
            Poll::Ready(result) => match result {
                Ok(success) => {
                    if success {
                        buf.put_slice(self.cursor.borrow().current().as_bytes());
                    }
                    return Poll::Ready(Ok(()));
                }
                Err(e) => {
                    return Poll::Ready(std::io::Result::Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        e,
                    )))
                }
            },
            Poll::Pending => Poll::Pending,
        }
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
    pub(crate) fn new(db: Database, options: GridFsBucketOptions) -> GridFsBucket {
        let options = GridFsBucketOptions::builder()
            .read_concern(options.read_concern.or_else(|| db.read_concern().cloned()))
            .write_concern(
                options
                    .write_concern
                    .or_else(|| db.write_concern().cloned()),
            )
            .selection_criteria(
                options
                    .selection_criteria
                    .or_else(|| db.selection_criteria().cloned()),
            )
            .chunk_size_bytes(options.chunk_size_bytes.or(Some(DEFAULT_CHUNK_SIZE_BYTES)))
            .bucket_name(
                options
                    .bucket_name
                    .or_else(|| Some(DEFAULT_BUCKET_NAME.to_string())),
            )
            .build();

        let files = db.collection::<FilesCollectionDocument>(&format!(
            "{}.files",
            &options.bucket_name.as_ref().unwrap()
        ));
        let chunks =
            db.collection::<Chunk>(&format!("{}.chunks", options.bucket_name.as_ref().unwrap()));

        GridFsBucket {
            inner: Arc::new(GridFsBucketInner {
                options,
                files,
                chunks,
                created_indexes: false,
            }),
        }
    }

    fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.options.read_concern.as_ref()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.options.write_concern.as_ref()
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.inner.options.selection_criteria.as_ref()
    }

    fn files(&self) -> &Collection<FilesCollectionDocument> {
        &self.inner.files
    }

    fn chunks(&self) -> &Collection<Chunk> {
        &self.inner.chunks
    }

    fn chunk_size_bytes(&self) -> u32 {
        self.inner.options.chunk_size_bytes.unwrap()
    }

    fn bucket_name(&self) -> &str {
        self.inner.options.bucket_name.as_ref().unwrap()
    }

    /// Opens a [`GridFsUploadStream`] that the application can write the contents of the file to.
    /// The application provides a custom file id.
    ///
    /// Returns a [`GridFsUploadStream`] to which the application will write the contents.
    pub async fn open_upload_stream_with_id(
        &self,
        files_id: Bson,
        filename: impl AsRef<str>,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<GridFsUploadStream> {
        self.check_or_create_indexes().await?;
        let mut options: GridFsUploadOptions = options.into().unwrap_or_default();
        options.chunk_size_bytes = options
            .chunk_size_bytes
            .or_else(|| Some(self.chunk_size_bytes()));
        Ok(GridFsUploadStream {
            bucket: self.clone(),
            files_id,
            length: 0,
            filename: filename.as_ref().to_string(),
            options,
        })
    }

    /// Opens a [`GridFsUploadStream`] that the application can write the contents of the file to.
    /// The driver generates a unique [`Bson::ObjectId`] for the file id.
    ///
    /// Returns a [`GridFsUploadStream`] to which the application will write the contents.
    pub async fn open_upload_stream(
        &self,
        filename: impl AsRef<str>,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<GridFsUploadStream> {
        self.open_upload_stream_with_id(Bson::ObjectId(ObjectId::new()), filename, options)
            .await
    }

    /// Uploads a user file to a GridFS bucket. The application supplies a custom file id. Uses the
    /// `tokio` crate's `AsyncRead` trait for the `source`.
    pub async fn upload_from_tokio_reader_with_id<T>(
        &self,
        files_id: Bson,
        filename: impl AsRef<str>,
        source: &mut T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<()>
    where
        T: tokio::io::AsyncRead + std::marker::Unpin,
    {
        use tokio::io::AsyncReadExt;

        self.check_or_create_indexes().await?;

        let mut options: GridFsUploadOptions = options.into().unwrap_or_default();
        options.chunk_size_bytes = options
            .chunk_size_bytes
            .or_else(|| Some(self.chunk_size_bytes()));

        let chunk_size = options.chunk_size_bytes.unwrap();
        let mut length = 0;
        let mut n = 0;
        // Get chunks collection
        let chunks = self.chunks();

        let insert_options = InsertOneOptions::builder()
            .write_concern(self.write_concern().cloned())
            .build();

        loop {
            let mut buf = vec![0u8; chunk_size as usize];
            let mut curr_length = 0usize;
            while curr_length < chunk_size as usize {
                let bytes_read = match source.read(&mut buf[curr_length..]).await {
                    Ok(num) => num,
                    Err(e) => {
                        // abort the upload by cleaning up any uploaded chunks
                        let delete_options = DeleteOptions::builder()
                            .write_concern(self.write_concern().cloned())
                            .build();
                        chunks
                            .delete_many(doc! { "files_id": &files_id }, delete_options)
                            .await?;
                        return Err(ErrorKind::Io(Arc::new(e)).into());
                    }
                };
                curr_length += bytes_read;
                if bytes_read == 0 {
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
                data: Binary {
                    bytes: buf[..curr_length].to_vec(),
                    subtype: BinarySubtype::Generic,
                },
            };
            // Put chunk in chunks collection.
            chunks.insert_one(chunk, insert_options.clone()).await?;
            length += curr_length;
            n += 1;
        }

        let file = FilesCollectionDocument {
            id: Bson::ObjectId(ObjectId::new()),
            length: length as u64,
            chunk_size,
            upload_date: DateTime::now(),
            filename: filename.as_ref().to_string(),
            metadata: options.metadata,
        };
        self.files().insert_one(file, insert_options).await?;
        Ok(())
    }

    /// Uploads a user file to a GridFS bucket. The application supplies a custom file id. Uses the
    /// `futures-0.3` crate's `AsyncRead` trait for the `source`.
    pub async fn upload_from_futures_0_3_reader_with_id<T>(
        &self,
        files_id: Bson,
        filename: impl AsRef<str>,
        source: &mut T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<()>
    where
        T: futures_util::io::AsyncRead + std::marker::Unpin,
    {
        self.upload_from_tokio_reader_with_id(files_id, filename, &mut source.compat(), options)
            .await
    }

    /// Uploads a user file to a GridFS bucket. The driver generates a unique [`Bson::ObjectId`] for
    /// the file id. Uses the `tokio` crate's `AsyncRead` trait for the `source`.
    pub async fn upload_from_tokio_reader<T>(
        &self,
        filename: impl AsRef<str>,
        source: &mut T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<()>
    where
        T: tokio::io::AsyncRead + std::marker::Unpin,
    {
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
    pub async fn upload_from_futures_0_3_reader<T>(
        &self,
        filename: impl AsRef<str>,
        source: &mut T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<()>
    where
        T: futures_util::io::AsyncRead + std::marker::Unpin,
    {
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
        let find_one_options = FindOneOptions::builder()
            .selection_criteria(self.selection_criteria().cloned())
            .build();
        let file = match self
            .files()
            .find_one(doc! { "_id": &id }, find_one_options)
            .await?
        {
            Some(fcd) => fcd,
            None => {
                return Err(ErrorKind::InvalidArgument {
                    message: format!("couldn't find file with id {}", &id),
                }
                .into());
            }
        };

        let options = FindOptions::builder().sort(doc! { "n": -1 }).build();
        let cursor = self
            .chunks()
            .find(doc! { "files_id": &id }, options)
            .await?;

        Ok(GridFsDownloadStream {
            bucket: self.clone(),
            files_id: id,
            file,
            cursor,
        })
    }

    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `filename` and the revision
    /// in `options`.
    pub async fn open_download_stream_by_name(
        &self,
        filename: impl AsRef<str>,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) -> Result<GridFsDownloadStream> {
        let mut sort = doc! { "uploadDate": -1 };
        let mut skip = 0;
        if let Some(opts) = options.into() {
            if let Some(rev) = opts.revision {
                if rev >= 0 {
                    sort = doc! { "uploadDate": 1 };
                    skip = rev as u64;
                } else {
                    skip = (-rev - 1) as u64;
                }
            }
        }
        let options = FindOneOptions::builder().sort(sort).skip(skip).build();

        let file = match self
            .files()
            .find_one(doc! { "filename": filename.as_ref() }, options)
            .await?
        {
            Some(fcd) => fcd,
            None => {
                return Err(ErrorKind::InvalidArgument {
                    message: format!("couldn't find file with name {}", filename.as_ref()),
                }
                .into());
            }
        };

        let options = FindOptions::builder().sort(doc! { "n": -1 }).build();
        let cursor = self
            .chunks()
            .find(doc! { "files_id": file.id.clone() }, options)
            .await?;

        Ok(GridFsDownloadStream {
            bucket: self.clone(),
            files_id: file.id.clone(),
            file,
            cursor,
        })
    }

    async fn download_to_tokio_writer_common<T>(
        &self,
        file: FilesCollectionDocument,
        destination: &mut T,
    ) -> Result<()>
    where
        T: tokio::io::AsyncWrite + std::marker::Unpin,
    {
        if file.length == 0 {
            return Ok(());
        }

        let options = FindOptions::builder()
            .sort(doc! { "n": -1 })
            .read_concern(self.read_concern().cloned())
            .selection_criteria(self.selection_criteria().cloned())
            .build();

        let mut cursor = self
            .chunks()
            .find(doc! { "files_id": &file.id }, options)
            .await?;

        let mut n = 0;
        while cursor.advance().await? {
            let chunk = cursor.deserialize_current()?;
            if chunk.n != n {
                return Err(ErrorKind::InvalidResponse {
                    message: "missing chunks in file".to_string(),
                }
                .into());
            } else if chunk.data.bytes.len() as u32 != self.chunk_size_bytes()
                && !cursor.is_exhausted()
            {
                return Err(ErrorKind::InvalidResponse {
                    message: "received invalid chunk".to_string(),
                }
                .into());
            }
            destination.write_all(&chunk.data.bytes).await?;
            n += 1;
        }
        Ok(())
    }

    /// Downloads the contents of the stored file specified by `id` and writes
    /// the contents to the `destination`. Uses the `tokio` crate's `AsyncWrite`
    /// trait for the `destination`.
    pub async fn download_to_tokio_writer<T>(&self, id: Bson, destination: &mut T) -> Result<()>
    where
        T: tokio::io::AsyncWrite + std::marker::Unpin,
    {
        let options = FindOneOptions::builder()
            .read_concern(self.read_concern().cloned())
            .selection_criteria(self.selection_criteria().cloned())
            .build();

        let file = match self.files().find_one(doc! { "_id": &id }, options).await? {
            Some(fcd) => fcd,
            None => {
                return Err(ErrorKind::InvalidArgument {
                    message: format!("couldn't find file with id {}", &id),
                }
                .into());
            }
        };
        self.download_to_tokio_writer_common(file, destination)
            .await
    }

    /// Downloads the contents of the stored file specified by `id` and writes
    /// the contents to the `destination`. Uses the `futures-0.3` crate's `AsyncWrite`
    /// trait for the `destination`.
    pub async fn download_to_futures_0_3_writer<T>(
        &self,
        id: Bson,
        destination: &mut T,
    ) -> Result<()>
    where
        T: futures_util::io::AsyncWrite + std::marker::Unpin,
    {
        self.download_to_tokio_writer(id, &mut destination.compat_write())
            .await
    }

    /// Downloads the contents of the stored file specified by `filename` and by
    /// the revision in `options` and writes the contents to the `destination`. Uses the
    /// `tokio` crate's `AsyncWrite` trait for the `destination`.
    pub async fn download_to_tokio_writer_by_name<T>(
        &self,
        filename: impl AsRef<str>,
        destination: &mut T,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) -> Result<()>
    where
        T: tokio::io::AsyncWrite + std::marker::Unpin,
    {
        let mut sort = doc! { "uploadDate": -1 };
        let mut skip = 0;
        if let Some(opts) = options.into() {
            if let Some(rev) = opts.revision {
                if rev >= 0 {
                    sort = doc! { "uploadDate": 1 };
                    skip = rev as u64;
                } else {
                    skip = (-rev - 1) as u64;
                }
            }
        }
        let options = FindOneOptions::builder()
            .sort(sort)
            .skip(skip)
            .read_concern(self.read_concern().cloned())
            .selection_criteria(self.selection_criteria().cloned())
            .build();

        let file = match self
            .files()
            .find_one(doc! { "filename": filename.as_ref() }, options)
            .await?
        {
            Some(fcd) => fcd,
            None => {
                return Err(ErrorKind::InvalidArgument {
                    message: format!("couldn't find file with name {}", &filename.as_ref()),
                }
                .into());
            }
        };
        self.download_to_tokio_writer_common(file, destination)
            .await
    }

    /// Downloads the contents of the stored file specified by `filename` and by
    /// the revision in `options` and writes the contents to the `destination`. Uses the
    /// `futures-0.3` crate's `AsyncWrite` trait for the `destination`.
    pub async fn download_to_futures_0_3_writer_by_name<T>(
        &self,
        filename: impl AsRef<str>,
        destination: &mut T,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) -> Result<()>
    where
        T: futures_util::io::AsyncWrite + std::marker::Unpin,
    {
        self.download_to_tokio_writer_by_name(filename, &mut destination.compat_write(), options)
            .await
    }

    /// Given an `id`, deletes the stored file's files collection document and
    /// associated chunks from a [`GridFsBucket`].
    pub async fn delete(&self, id: Bson) -> Result<()> {
        let options = DeleteOptions::builder()
            .write_concern(self.write_concern().cloned())
            .build();

        if self
            .files()
            .delete_one(doc! { "_id": &id }, options.clone())
            .await?
            .deleted_count
            != 1
        {
            self.chunks()
                .delete_many(doc! { "files_id": &id }, options)
                .await?;
            return Err(ErrorKind::InvalidArgument {
                message: format!("couldn't find file with id {}", &id),
            }
            .into());
        };

        self.chunks()
            .delete_many(doc! { "files_id": id }, options)
            .await?;

        Ok(())
    }

    /// Finds and returns the files collection documents that match the filter.
    pub async fn find(
        &self,
        filter: Document,
        options: impl Into<Option<GridFsFindOptions>>,
    ) -> Result<Cursor<FilesCollectionDocument>> {
        let options: FindOptions = options.into().unwrap_or_default().into();
        self.files().find(filter, options).await
    }

    /// Renames the stored file with the specified `id`.
    pub async fn rename(&self, id: Bson, new_filename: String) -> Result<()> {
        let options = UpdateOptions::builder()
            .write_concern(self.write_concern().cloned())
            .build();
        let update_result = self
            .files()
            .update_one(
                doc! { "_id": id },
                UpdateModifications::Document(doc! { "filename": { "$set": new_filename } }),
                options,
            )
            .await?;

        if update_result.matched_count == 0 {
            return Err(ErrorKind::InvalidArgument {
                message: "this file was not found in the bucket".to_string(),
            }
            .into());
        }
        Ok(())
    }

    /// Drops the files associated with this bucket.
    pub async fn drop(&self) -> Result<()> {
        let options = DropCollectionOptions::builder()
            .write_concern(self.write_concern().cloned())
            .build();
        self.files().drop(options.clone()).await?;

        self.chunks().drop(options).await?;
        Ok(())
    }

    async fn check_or_create_indexes(&self) -> Result<()> {
        if !self.inner.created_indexes {
            self.files()
                .create_index(
                    IndexModel::builder()
                        .keys(doc! { "filename": 1, "uploadDate": 1 })
                        .build(),
                    None,
                )
                .await?;

            self.chunks()
                .create_index(
                    IndexModel::builder()
                        .keys(doc! { "files_id": 1, "n": 1 })
                        .build(),
                    None,
                )
                .await?;
        }
        Ok(())
    }
}
