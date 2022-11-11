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
    bson::{doc, oid::ObjectId, spec::BinarySubtype, Bson, DateTime, Document, RawBinaryRef},
    bson_util::get_int,
    error::{Error, ErrorKind, GridFsErrorKind, Result},
    index::IndexModel,
    options::{CreateCollectionOptions, FindOneOptions, ReadPreference, SelectionCriteria},
    runtime,
    Collection,
};

impl GridFsBucket {
    /// Uploads a user file to a GridFS bucket. Bytes are read from `source` and stored in chunks in
    /// the bucket's chunks collection. After all the chunks have been uploaded, a corresponding
    /// [`FilesCollectionDocument`] is stored in the bucket's files collection.
    ///
    /// This method generates an [`ObjectId`] for the `files_id` field of the
    /// [`FilesCollectionDocument`] and returns it.
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

    /// Uploads a user file to a GridFS bucket with the given `files_id`. Bytes are read from
    /// `source` and stored in chunks in the bucket's chunks collection. After all the chunks have
    /// been uploaded, a corresponding [`FilesCollectionDocument`] is stored in the bucket's files
    /// collection.
    pub async fn upload_from_futures_0_3_reader_with_id<T>(
        &self,
        files_id: Bson,
        filename: impl AsRef<str>,
        mut source: T,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> Result<()>
    where
        T: AsyncRead + Unpin,
    {
        let options = options.into();

        self.create_indexes().await?;

        let chunk_size = options
            .as_ref()
            .and_then(|opts| opts.chunk_size_bytes)
            .unwrap_or_else(|| self.chunk_size_bytes());
        let mut length = 0u64;
        let mut n = 0;

        let mut buf = vec![0u8; chunk_size as usize];
        loop {
            let bytes_read = match source.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(error) => {
                    self.chunks()
                        .delete_many(doc! { "files_id": &files_id }, None)
                        .await?;
                    return Err(ErrorKind::Io(error.into()).into());
                }
            };

            let chunk = Chunk {
                id: ObjectId::new(),
                files_id: files_id.clone(),
                n,
                data: RawBinaryRef {
                    subtype: BinarySubtype::Generic,
                    bytes: &buf[..bytes_read],
                },
            };
            self.chunks().insert_one(chunk, None).await?;

            length += bytes_read as u64;
            n += 1;
        }

        let file = FilesCollectionDocument {
            id: files_id,
            length,
            chunk_size,
            upload_date: DateTime::now(),
            filename: Some(filename.as_ref().to_string()),
            metadata: options.and_then(|opts| opts.metadata),
        };
        self.files().insert_one(file, None).await?;

        Ok(())
    }

    async fn create_indexes(&self) -> Result<()> {
        if !self.inner.created_indexes.load(Ordering::SeqCst) {
            let find_options = FindOneOptions::builder()
                .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary))
                .projection(doc! { "_id": 1 })
                .build();
            if self
                .files()
                .clone_with_type::<Document>()
                .find_one(None, find_options)
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

    async fn create_index<T>(&self, coll: &Collection<T>, keys: Document) -> Result<()> {
        // listIndexes returns an error if the collection has not yet been created.
        let options = CreateCollectionOptions::builder()
            .write_concern(self.write_concern().cloned())
            .build();
        // Ignore NamespaceExists errors if the collection has already been created.
        if let Err(error) = self.inner.db.create_collection(coll.name(), options).await {
            if error.code() != Some(48) {
                return Err(error);
            }
        }

        // From the spec: Drivers MUST check whether the indexes already exist before attempting to
        // create them.
        let mut indexes = coll.list_indexes(None).await?;
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
        coll.create_index(index_model, None).await?;

        Ok(())
    }
}

pub struct GridFsUploadStream {
    bucket: GridFsBucket,
    state: State,
    current_n: u32,
    id: Bson,
    chunk_size: u32,
    // Additional metadata for the file. These values are stored as Options so that they can be
    // taken and inserted into a `FilesCollectionDocument` when the stream is closed.
    filename: Option<String>,
    metadata: Option<Option<Document>>,
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
    /// Gets the file `id` for the stream.
    pub fn id(&self) -> &Bson {
        &self.id
    }

    pub async fn abort(&mut self) -> Result<()> {
        match self.state {
            State::Closed => Err(ErrorKind::GridFs(GridFsErrorKind::UploadStreamClosed).into()),
            _ => {
                self.bucket
                    .chunks()
                    .delete_many(doc! { "files_id": &self.id }, None)
                    .await?;
                self.state = State::Closed;
                Ok(())
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
            runtime::execute(async move {
                let _result = chunks.delete_many(doc! { "files_id": id }, None).await;
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
                if buffer_ref.len() < stream.chunk_size as usize {
                    return Poll::Ready(Ok(buf.len()));
                }

                let new_future = write_bytes(
                    stream.bucket.clone(),
                    buffer.take().unwrap(),
                    stream.current_n,
                    stream.chunk_size,
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

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
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
                    length: stream.current_n as u64 * stream.chunk_size as u64
                        + buffer.len() as u64,
                    chunk_size: stream.chunk_size,
                    upload_date: DateTime::now(),
                    filename: stream.filename.take(),
                    metadata: stream.metadata.take().unwrap(),
                };

                let new_future = close(stream.bucket.clone(), buffer, file).boxed();
                stream.state.set_closing(new_future)
            }
            // This case is effectively unreachable, as the AsyncWriteExt methods take &mut self and
            // poll the futures to completion. If a user were to call these polling methods directly
            // and intersperse futures, we should just pend here until the writing future resolves.
            State::Writing(_) => return Poll::Pending,
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

async fn write_bytes(
    bucket: GridFsBucket,
    mut buffer: Vec<u8>,
    starting_n: u32,
    chunk_size: u32,
    files_id: Bson,
) -> Result<(u32, Vec<u8>)> {
    bucket.create_indexes().await?;

    let mut n = 0;
    let mut chunks = vec![];

    while buffer.len() as u32 - (n * chunk_size) >= chunk_size {
        let start = n * chunk_size;
        let end = (n + 1) * chunk_size;
        let chunk = Chunk {
            id: ObjectId::new(),
            files_id: files_id.clone(),
            n: starting_n + n,
            data: RawBinaryRef {
                subtype: BinarySubtype::Generic,
                bytes: &buffer[(start as usize)..(end as usize)],
            },
        };
        n += 1;
        chunks.push(chunk);
    }

    bucket.chunks().insert_many(chunks, None).await?;
    buffer.drain(..(n * chunk_size) as usize);

    Ok((n, buffer))
}

async fn close(bucket: GridFsBucket, buffer: Vec<u8>, file: FilesCollectionDocument) -> Result<()> {
    let insert_result: Result<()> = async {
        if !buffer.is_empty() {
            let final_chunk = Chunk {
                id: ObjectId::new(),
                n: file.n() - 1,
                files_id: file.id.clone(),
                data: RawBinaryRef {
                    subtype: BinarySubtype::Generic,
                    bytes: &buffer[..],
                },
            };
            bucket.chunks().insert_one(final_chunk, None).await?;
        }
        bucket.files().insert_one(&file, None).await?;
        Ok(())
    }
    .await;

    match insert_result {
        Ok(()) => Ok(()),
        Err(error) => {
            bucket
                .chunks()
                .delete_many(doc! { "files_id": file.id }, None)
                .await?;
            Err(error)
        }
    }
}

fn get_closed_error() -> futures_io::Error {
    let error: Error = ErrorKind::GridFs(GridFsErrorKind::UploadStreamClosed).into();
    error.into_futures_io_error()
}

impl GridFsBucket {
    /// Opens a [`GridFsUploadStream`] that the application can write the contents of the file to.
    /// The driver generates a unique [`Bson::ObjectId`] for the file id.
    ///
    /// Returns a [`GridFsUploadStream`] to which the application will write the contents.
    pub fn open_upload_stream(
        &self,
        filename: impl AsRef<str>,
        options: impl Into<Option<GridFsUploadOptions>>,
    ) -> GridFsUploadStream {
        self.open_upload_stream_with_id(ObjectId::new().into(), filename, options)
    }

    /// Opens a [`GridFsUploadStream`] that the application can write the contents of the file to.
    /// The application provides a custom file id.
    ///
    /// Returns a [`GridFsUploadStream`] to which the application will write the contents.
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
            chunk_size: options
                .as_ref()
                .and_then(|opts| opts.chunk_size_bytes)
                .unwrap_or_else(|| self.chunk_size_bytes()),
            metadata: Some(options.and_then(|opts| opts.metadata)),
        }
    }
}
