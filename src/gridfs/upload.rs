use std::{
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll},
};

use futures_util::{
    future::{BoxFuture, FutureExt},
    io::{AsyncRead, AsyncReadExt, AsyncWrite},
    stream::TryStreamExt,
};

use super::{options::GridFsUploadOptions, Chunk, FilesCollectionDocument, GridFsBucket};
use crate::{
    action::Action,
    bson::{doc, oid::ObjectId, spec::BinarySubtype, Bson, DateTime, Document, RawBinaryRef},
    bson_util::get_int,
    checked::Checked,
    client::AsyncDropToken,
    error::{Error, ErrorKind, GridFsErrorKind, Result},
    index::IndexModel,
    options::{ReadPreference, SelectionCriteria},
    Collection,
};

// User functions for uploading from readers.
impl GridFsBucket {
    /// Uploads a user file to the bucket. Bytes are read from `source`, which may be any type that
    /// implements the [`futures_io::AsyncRead`] trait, and stored in chunks in the bucket's
    /// chunks collection. After all the chunks have been uploaded, a corresponding
    /// [`FilesCollectionDocument`] is stored in the bucket's files collection.
    ///
    /// This method generates an [`ObjectId`] for the `id` field of the
    /// [`FilesCollectionDocument`] and returns it.
    ///
    /// To upload from a type that implements [`tokio::io::AsyncRead`], use the
    /// [`tokio_util::compat`] module to convert between types.
    ///
    /// ```rust
    /// # use mongodb::{error::Result, gridfs::GridFsBucket};
    /// # async fn compat_example(
    /// #     bucket: GridFsBucket,
    /// #     tokio_reader: impl tokio::io::AsyncRead + Unpin)
    /// # -> Result<()> {
    /// use tokio_util::compat::TokioAsyncReadCompatExt;
    ///
    /// let futures_reader = tokio_reader.compat();
    /// bucket.upload_from_futures_0_3_reader("example_file", futures_reader, None).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Note that once an `AsyncRead` trait is stabilized in the standard library, this method will
    /// be deprecated in favor of one that accepts a `std::io::AsyncRead` source.
    pub async fn upload_from_futures_0_3_reader<T>(
        &self,
        filename: impl AsRef<str>,
        source: T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<ObjectId>
    where
        T: AsyncRead + Unpin,
    {
        let id = ObjectId::new();
        self.upload_from_futures_0_3_reader_with_id(id.into(), filename, source, options)
            .await?;
        Ok(id)
    }

    /// Uploads a user file to the bucket. Bytes are read from `source`, which may be any type that
    /// implements the [`futures_io::AsyncRead`] trait, and stored in chunks in the bucket's
    /// chunks collection. After all the chunks have been uploaded, a corresponding
    /// [`FilesCollectionDocument`] with the given `id` is stored in the bucket's files collection.
    ///
    /// To upload from a type that implements [`tokio::io::AsyncRead`], use the
    /// [`tokio_util::compat`] module to convert between types.
    ///
    /// ```rust
    /// # use mongodb::{bson::Bson, error::Result, gridfs::GridFsBucket};
    /// # async fn compat_example(
    /// #     bucket: GridFsBucket,
    /// #     tokio_reader: impl tokio::io::AsyncRead + Unpin,
    /// #     id: Bson,
    /// # ) -> Result<()> {
    /// use tokio_util::compat::TokioAsyncReadCompatExt;
    ///
    /// let futures_reader = tokio_reader.compat();
    /// bucket.upload_from_futures_0_3_reader_with_id(id, "example_file", futures_reader, None).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Note that once an `AsyncRead` trait is stabilized in the standard library, this method will
    /// be deprecated in favor of one that accepts a `std::io::AsyncRead` source.
    pub async fn upload_from_futures_0_3_reader_with_id<T>(
        &self,
        id: Bson,
        filename: impl AsRef<str>,
        mut source: T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<()>
    where
        T: AsyncRead + Unpin,
    {
        let options = options.into();

        self.create_indexes().await?;

        let chunk_size_bytes = options
            .as_ref()
            .and_then(|opts| opts.chunk_size_bytes)
            .unwrap_or_else(|| self.chunk_size_bytes());
        let mut length = 0u64;
        let mut n = 0;

        let mut buf = vec![0u8; chunk_size_bytes as usize];
        loop {
            let bytes_read = match read_exact_or_to_end(&mut buf, &mut source).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(error) => {
                    return clean_up_chunks(id.clone(), self.chunks().clone(), Some(error)).await;
                }
            };

            let chunk = Chunk {
                id: ObjectId::new(),
                files_id: id.clone(),
                n,
                data: RawBinaryRef {
                    subtype: BinarySubtype::Generic,
                    bytes: &buf[..bytes_read],
                },
            };
            self.chunks().insert_one(chunk).await?;

            length += bytes_read as u64;
            n += 1;
        }

        let file = FilesCollectionDocument {
            id,
            length,
            chunk_size_bytes,
            upload_date: DateTime::now(),
            filename: Some(filename.as_ref().to_string()),
            metadata: options.and_then(|opts| opts.metadata),
        };
        self.files().insert_one(file).await?;

        Ok(())
    }

    async fn create_indexes(&self) -> Result<()> {
        if !self.inner.created_indexes.load(Ordering::SeqCst) {
            if self
                .files()
                .clone_with_type::<Document>()
                .find_one(doc! {})
                .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary))
                .projection(doc! { "_id": 1 })
                .await?
                .is_none()
            {
                self.create_index(self.files(), doc! { "filename": 1, "uploadDate": 1 })
                    .await?;
                self.create_index(self.chunks(), doc! { "files_id": 1, "n": 1 })
                    .await?;
            }
            self.inner.created_indexes.store(true, Ordering::SeqCst);
        }

        Ok(())
    }

    async fn create_index<T: Send + Sync>(
        &self,
        coll: &Collection<T>,
        keys: Document,
    ) -> Result<()> {
        // listIndexes returns an error if the collection has not yet been created.
        // Ignore NamespaceExists errors if the collection has already been created.
        if let Err(error) = self
            .inner
            .db
            .create_collection(coll.name())
            .optional(self.write_concern().cloned(), |b, wc| b.write_concern(wc))
            .await
        {
            if error.sdam_code() != Some(48) {
                return Err(error);
            }
        }

        // From the spec: Drivers MUST check whether the indexes already exist before attempting to
        // create them.
        let mut indexes = coll.list_indexes().await?;
        'outer: while let Some(index_model) = indexes.try_next().await? {
            if index_model.keys.len() != keys.len() {
                continue;
            }
            // Indexes should be considered equivalent regardless of numeric value type.
            // e.g. { "filename": 1, "uploadDate": 1 } is equivalent to
            // { "filename": 1.0, "uploadDate": 1.0 }
            let number_matches = |key: &str, value: &Bson| {
                if let Some(model_value) = index_model.keys.get(key) {
                    match get_int(value) {
                        Some(num) => get_int(model_value) == Some(num),
                        None => model_value == value,
                    }
                } else {
                    false
                }
            };
            for (key, value) in keys.iter() {
                if !number_matches(key, value) {
                    continue 'outer;
                }
            }
            return Ok(());
        }

        let index_model = IndexModel::builder().keys(keys).build();
        coll.create_index(index_model).await?;

        Ok(())
    }
}

async fn read_exact_or_to_end<T>(buf: &mut [u8], source: &mut T) -> Result<usize>
where
    T: AsyncRead + Unpin,
{
    let mut total_bytes_read = 0;
    loop {
        let bytes_read = match source.read(&mut buf[total_bytes_read..]).await? {
            0 => break,
            n => n,
        };
        total_bytes_read += bytes_read;
        if total_bytes_read == buf.len() {
            break;
        }
    }

    Ok(total_bytes_read)
}

/// A stream to which bytes can be written to be uploaded to a GridFS bucket.
///
/// # Uploading to the Stream
///  The `GridFsUploadStream` type implements [`futures_io::AsyncWrite`]. It is recommended that
/// users call the utility methods in [`AsyncWriteExt`](futures_util::io::AsyncWriteExt) to interact
/// with the stream.
///
/// Bytes can be written to the stream using the write methods in `AsyncWriteExt`. When
/// [`close`](futures_util::io::AsyncWriteExt::close) is invoked on the stream, any remaining bytes
/// in the buffer are written to the chunks collection and a corresponding
/// [`FilesCollectionDocument`] is written to the files collection. It is an error to write to,
/// abort, or close the stream after `close` has been called.
///
/// ```rust
/// # use mongodb::{error::Result, gridfs::{GridFsBucket, GridFsUploadStream}};
/// # async fn upload_example(bucket: GridFsBucket) -> Result<()> {
/// use futures_util::io::AsyncWriteExt;
///
/// let bytes = vec![0u8; 100];
/// let mut upload_stream = bucket.open_upload_stream("example_file", None);
/// upload_stream.write_all(&bytes[..]).await?;
/// upload_stream.close().await?;
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
/// # use mongodb::{error::Result, gridfs::{GridFsBucket, GridFsUploadStream}};
/// # async fn abort_example(bucket: GridFsBucket) -> Result<()> {
/// use futures_util::io::AsyncWriteExt;
///
/// let bytes = vec![0u8; 100];
/// let mut upload_stream = bucket.open_upload_stream("example_file", None);
/// upload_stream.write_all(&bytes[..]).await?;
/// upload_stream.abort().await?;
/// # Ok(())
/// # }
/// ```
///
/// In the event of an error during any operation on the `GridFsUploadStream`, any chunks associated
/// with the stream will be removed from the chunks collection. Any subsequent attempts to write to,
/// abort, or close the stream will return an error.
///
/// If a `GridFsUploadStream` is dropped prior to `abort` or `close` being called, its [`Drop`]
/// implementation will spawn a task to remove any chunks associated with the stream from the chunks
/// collection. Users should prefer calling `abort` explicitly to relying on the `Drop`
/// implementation in order to `await` the task and inspect the result of the delete operation.
///
/// # Flushing the Stream
/// Because all chunks besides the final chunk of a file must be exactly `chunk_size_bytes`, calling
/// [`flush`](futures_util::io::AsyncWriteExt::flush) is not guaranteed to flush all bytes to the
/// chunks collection. Any remaining buffered bytes will be written to the chunks collection upon a
/// call to `close`.
///
/// # Using [`tokio::io::AsyncWrite`]
/// Users who prefer to use tokio's `AsyncWrite` trait can use the [`tokio_util::compat`] module.
///
/// ```rust
/// # use mongodb::gridfs::{GridFsBucket, GridFsUploadStream};
/// # fn compat_example(bucket: GridFsBucket) {
/// use tokio_util::compat::FuturesAsyncWriteCompatExt;
///
/// let futures_upload_stream = bucket.open_upload_stream("example_file", None);
/// let tokio_upload_stream = futures_upload_stream.compat_write();
/// # }
/// ```
pub struct GridFsUploadStream {
    bucket: GridFsBucket,
    state: State,
    current_n: u32,
    id: Bson,
    chunk_size_bytes: u32,
    // Additional metadata for the file. These values are stored as Options so that they can be
    // taken and inserted into a FilesCollectionDocument when the stream is closed.
    filename: Option<String>,
    metadata: Option<Option<Document>>,
    drop_token: AsyncDropToken,
}

type WriteBytesFuture = BoxFuture<'static, Result<(u32, Vec<u8>)>>;
type CloseFuture = BoxFuture<'static, Result<()>>;

enum State {
    // The buffer is stored as an option so that it can be moved into futures without requiring
    // ownership of the state. It can always be unwrapped safely.
    Idle(Option<Vec<u8>>),
    Writing(WriteBytesFuture),
    Closing(CloseFuture),
    Closed,
}

impl State {
    fn set_writing(&mut self, new_future: WriteBytesFuture) -> &mut WriteBytesFuture {
        *self = Self::Writing(new_future);
        match self {
            Self::Writing(future) => future,
            _ => unreachable!(),
        }
    }

    fn set_closing(&mut self, new_future: CloseFuture) -> &mut CloseFuture {
        *self = Self::Closing(new_future);
        match self {
            Self::Closing(future) => future,
            _ => unreachable!(),
        }
    }
}

impl GridFsUploadStream {
    /// Gets the stream's unique [`Bson`] identifier. This value will be the `id` field for the
    /// [`FilesCollectionDocument`] uploaded to the files collection when the stream is closed.
    pub fn id(&self) -> &Bson {
        &self.id
    }

    /// Aborts the stream, discarding any chunks that have already been written to the chunks
    /// collection. Once this method has been called, it is an error to attempt to write to, abort,
    /// or close the stream.
    pub async fn abort(&mut self) -> Result<()> {
        match self.state {
            State::Closed => Err(ErrorKind::GridFs(GridFsErrorKind::UploadStreamClosed).into()),
            _ => {
                self.state = State::Closed;
                clean_up_chunks(self.id.clone(), self.bucket.chunks().clone(), None).await
            }
        }
    }
}

impl Drop for GridFsUploadStream {
    // TODO RUST-1493: pre-create this task
    fn drop(&mut self) {
        if !matches!(self.state, State::Closed) {
            let chunks = self.bucket.chunks().clone();
            let id = self.id.clone();
            self.drop_token.spawn(async move {
                let _result = chunks.delete_many(doc! { "files_id": id }).await;
            })
        }
    }
}

impl AsyncWrite for GridFsUploadStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, futures_io::Error>> {
        let stream = self.get_mut();

        let future = match &mut stream.state {
            State::Idle(buffer) => {
                let buffer_ref = buffer.as_mut().unwrap();

                buffer_ref.extend_from_slice(buf);
                if buffer_ref.len() < stream.chunk_size_bytes as usize {
                    return Poll::Ready(Ok(buf.len()));
                }

                let new_future = write_bytes(
                    stream.bucket.clone(),
                    buffer.take().unwrap(),
                    stream.current_n,
                    stream.chunk_size_bytes,
                    stream.id.clone(),
                )
                .boxed();
                stream.state.set_writing(new_future)
            }
            State::Writing(future) => future,
            State::Closing(_) | State::Closed => return Poll::Ready(Err(get_closed_error())),
        };

        let result = match future.poll_unpin(cx) {
            Poll::Ready(result) => result,
            Poll::Pending => return Poll::Pending,
        };

        match result {
            Ok((chunks_written, buffer)) => {
                stream.current_n += chunks_written;
                stream.state = State::Idle(Some(buffer));
                Poll::Ready(Ok(buf.len()))
            }
            Err(error) => {
                stream.state = State::Closed;
                Poll::Ready(Err(error.into_futures_io_error()))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
        // The buffer only contains leftover bytes that couldn't fill an entire chunk, so there's
        // nothing to flush.
        match self.state {
            State::Closed => Poll::Ready(Err(get_closed_error())),
            _ => Poll::Ready(Ok(())),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
        let stream = self.get_mut();

        let future = match &mut stream.state {
            State::Idle(buffer) => {
                let buffer = buffer.take().unwrap();

                let file = FilesCollectionDocument {
                    id: stream.id.clone(),
                    length: stream.current_n as u64 * stream.chunk_size_bytes as u64
                        + buffer.len() as u64,
                    chunk_size_bytes: stream.chunk_size_bytes,
                    upload_date: DateTime::now(),
                    filename: stream.filename.take(),
                    metadata: stream.metadata.take().unwrap(),
                };

                let new_future = close(stream.bucket.clone(), buffer, file).boxed();
                stream.state.set_closing(new_future)
            }
            State::Writing(_) => {
                let error: Error = ErrorKind::GridFs(GridFsErrorKind::WriteInProgress).into();
                let new_future = clean_up_chunks(
                    stream.id.clone(),
                    stream.bucket.chunks().clone(),
                    Some(error),
                )
                .boxed();
                stream.state.set_closing(new_future)
            }
            State::Closing(future) => future,
            State::Closed => return Poll::Ready(Err(get_closed_error())),
        };

        let result = match future.poll_unpin(cx) {
            Poll::Ready(result) => result,
            Poll::Pending => return Poll::Pending,
        };

        stream.state = State::Closed;
        match result {
            Ok(()) => Poll::Ready(Ok(())),
            Err(error) => Poll::Ready(Err(error.into_futures_io_error())),
        }
    }
}

// Writes the data in the buffer to the database and returns the number of chunks written and any
// leftover bytes that didn't fill an entire chunk.
async fn write_bytes(
    bucket: GridFsBucket,
    mut buffer: Vec<u8>,
    starting_n: u32,
    chunk_size_bytes: u32,
    files_id: Bson,
) -> Result<(u32, Vec<u8>)> {
    let chunk_size_bytes: usize = Checked::new(chunk_size_bytes).try_into()?;
    bucket.create_indexes().await?;

    let mut n = Checked::new(0);
    let mut chunks = vec![];

    while (Checked::new(buffer.len()) - (n * chunk_size_bytes)).get()? >= chunk_size_bytes {
        let start = n * chunk_size_bytes;
        let end = (n + 1) * chunk_size_bytes;
        let chunk = Chunk {
            id: ObjectId::new(),
            files_id: files_id.clone(),
            n: starting_n + n.try_into::<u32>()?,
            data: RawBinaryRef {
                subtype: BinarySubtype::Generic,
                bytes: &buffer[start.get()?..end.get()?],
            },
        };
        n += 1;
        chunks.push(chunk);
    }

    match bucket.chunks().insert_many(chunks).await {
        Ok(_) => {
            buffer.drain(..(n * chunk_size_bytes).get()?);
            Ok((n.try_into()?, buffer))
        }
        Err(error) => match clean_up_chunks(files_id, bucket.chunks().clone(), Some(error)).await {
            // clean_up_chunks will always return an error if one is passed in, so this case is
            // unreachable
            Ok(()) => unreachable!(),
            Err(error) => Err(error),
        },
    }
}

async fn close(bucket: GridFsBucket, buffer: Vec<u8>, file: FilesCollectionDocument) -> Result<()> {
    let insert_result: Result<()> = async {
        if !buffer.is_empty() {
            debug_assert!(buffer.len() < file.chunk_size_bytes as usize);
            let final_chunk = Chunk {
                id: ObjectId::new(),
                n: file.n() - 1,
                files_id: file.id.clone(),
                data: RawBinaryRef {
                    subtype: BinarySubtype::Generic,
                    bytes: &buffer[..],
                },
            };
            bucket.chunks().insert_one(final_chunk).await?;
        }
        bucket.files().insert_one(&file).await?;
        Ok(())
    }
    .await;

    match insert_result {
        Ok(()) => Ok(()),
        Err(error) => clean_up_chunks(file.id.clone(), bucket.chunks().clone(), Some(error)).await,
    }
}

async fn clean_up_chunks(
    id: Bson,
    chunks: Collection<Chunk<'static>>,
    original_error: Option<Error>,
) -> Result<()> {
    match chunks.delete_many(doc! { "files_id": id }).await {
        Ok(_) => match original_error {
            Some(error) => Err(error),
            None => Ok(()),
        },
        Err(delete_error) => Err(ErrorKind::GridFs(GridFsErrorKind::AbortError {
            original_error,
            delete_error,
        })
        .into()),
    }
}

fn get_closed_error() -> futures_io::Error {
    let error: Error = ErrorKind::GridFs(GridFsErrorKind::UploadStreamClosed).into();
    error.into_futures_io_error()
}

// User functions for creating upload streams.
impl GridFsBucket {
    /// Creates and returns a [`GridFsUploadStream`] that the application can write the contents of
    /// the file to. This method generates a unique [`ObjectId`] for the corresponding
    /// [`FilesCollectionDocument`]'s `id` field that can be accessed via the stream's `id`
    /// method.
    pub fn open_upload_stream(
        &self,
        filename: impl AsRef<str>,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> GridFsUploadStream {
        self.open_upload_stream_with_id(ObjectId::new().into(), filename, options)
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
        let options = options.into();
        GridFsUploadStream {
            bucket: self.clone(),
            state: State::Idle(Some(Vec::new())),
            current_n: 0,
            id,
            filename: Some(filename.as_ref().into()),
            chunk_size_bytes: options
                .as_ref()
                .and_then(|opts| opts.chunk_size_bytes)
                .unwrap_or_else(|| self.chunk_size_bytes()),
            metadata: Some(options.and_then(|opts| opts.metadata)),
            drop_token: self.client().register_async_drop(),
        }
    }
}
