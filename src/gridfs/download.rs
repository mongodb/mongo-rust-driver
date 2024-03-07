use std::{
    marker::Unpin,
    ops::Range,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{
    future::{BoxFuture, FutureExt},
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
};

use super::{options::GridFsDownloadByNameOptions, Chunk, FilesCollectionDocument, GridFsBucket};
use crate::{
    bson::{doc, Bson},
    error::{ErrorKind, GridFsErrorKind, GridFsFileIdentifier, Result},
    options::{FindOneOptions, FindOptions},
    Collection,
    Cursor,
};

// Utility functions for finding files within the bucket.
impl GridFsBucket {
    async fn find_file_by_id(&self, id: &Bson) -> Result<FilesCollectionDocument> {
        match self.find_one(doc! { "_id": id }, None).await? {
            Some(file) => Ok(file),
            None => Err(ErrorKind::GridFs(GridFsErrorKind::FileNotFound {
                identifier: GridFsFileIdentifier::Id(id.clone()),
            })
            .into()),
        }
    }

    async fn find_file_by_name(
        &self,
        filename: &str,
        options: Option<GridFsDownloadByNameOptions>,
    ) -> Result<FilesCollectionDocument> {
        let revision = options.and_then(|opts| opts.revision).unwrap_or(-1);
        let (sort, skip) = if revision >= 0 {
            (1, revision)
        } else {
            (-1, -revision - 1)
        };
        let options = FindOneOptions::builder()
            .sort(doc! { "uploadDate": sort })
            .skip(skip as u64)
            .build();

        match self
            .files()
            .find_one(doc! { "filename": filename }, options)
            .await?
        {
            Some(fcd) => Ok(fcd),
            None => {
                if self
                    .files()
                    .find_one(doc! { "filename": filename }, None)
                    .await?
                    .is_some()
                {
                    Err(ErrorKind::GridFs(GridFsErrorKind::RevisionNotFound { revision }).into())
                } else {
                    Err(ErrorKind::GridFs(GridFsErrorKind::FileNotFound {
                        identifier: GridFsFileIdentifier::Filename(filename.into()),
                    })
                    .into())
                }
            }
        }
    }
}

// User functions for downloading to writers.
impl GridFsBucket {
    /// Downloads the contents of the stored file specified by `id` and writes the contents to the
    /// `destination`, which may be any type that implements the [`futures_io::AsyncWrite`] trait.
    ///
    /// To download to a type that implements [`tokio::io::AsyncWrite`], use the
    /// [`tokio_util::compat`] module to convert between types.
    ///
    /// ```rust
    /// # use mongodb::{bson::Bson, error::Result, gridfs::GridFsBucket};
    /// # async fn compat_example(
    /// #     bucket: GridFsBucket,
    /// #     tokio_writer: impl tokio::io::AsyncWrite + Unpin,
    /// #     id: Bson,
    /// # ) -> Result<()> {
    /// use tokio_util::compat::TokioAsyncWriteCompatExt;
    ///
    /// let futures_writer = tokio_writer.compat_write();
    /// bucket.download_to_futures_0_3_writer(id, futures_writer).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Note that once an `AsyncWrite` trait is stabilized in the standard library, this method will
    /// be deprecated in favor of one that accepts a `std::io::AsyncWrite` source.
    pub async fn download_to_futures_0_3_writer<T>(&self, id: Bson, destination: T) -> Result<()>
    where
        T: AsyncWrite + Unpin,
    {
        let file = self.find_file_by_id(&id).await?;
        self.download_to_writer_common(file, destination).await
    }

    /// Downloads the contents of the stored file specified by `filename` and writes the contents to
    /// the `destination`, which may be any type that implements the [`futures_io::AsyncWrite`]
    /// trait.
    ///
    /// If there are multiple files in the bucket with the given filename, the `revision` in the
    /// options provided is used to determine which one to download. See the documentation for
    /// [`GridFsDownloadByNameOptions`] for details on how to specify a revision. If no revision is
    /// provided, the file with `filename` most recently uploaded will be downloaded.
    ///
    /// To download to a type that implements [`tokio::io::AsyncWrite`], use the
    /// [`tokio_util::compat`] module to convert between types.
    ///
    /// ```rust
    /// # use mongodb::{bson::Bson, error::Result, gridfs::GridFsBucket};
    /// # async fn compat_example(
    /// #     bucket: GridFsBucket,
    /// #     tokio_writer: impl tokio::io::AsyncWrite + Unpin,
    /// #     id: Bson,
    /// # ) -> Result<()> {
    /// use tokio_util::compat::TokioAsyncWriteCompatExt;
    ///
    /// let futures_writer = tokio_writer.compat_write();
    /// bucket.download_to_futures_0_3_writer_by_name("example_file", futures_writer, None).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Note that once an `AsyncWrite` trait is stabilized in the standard library, this method will
    /// be deprecated in favor of one that accepts a `std::io::AsyncWrite` source.
    pub async fn download_to_futures_0_3_writer_by_name<T>(
        &self,
        filename: impl AsRef<str>,
        destination: T,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) -> Result<()>
    where
        T: AsyncWrite + Unpin,
    {
        let file = self
            .find_file_by_name(filename.as_ref(), options.into())
            .await?;
        self.download_to_writer_common(file, destination).await
    }

    async fn download_to_writer_common<T>(
        &self,
        file: FilesCollectionDocument,
        mut destination: T,
    ) -> Result<()>
    where
        T: AsyncWrite + Unpin,
    {
        if file.length == 0 {
            return Ok(());
        }

        let options = FindOptions::builder().sort(doc! { "n": 1 }).build();
        let mut cursor = self
            .chunks()
            .find(doc! { "files_id": &file.id }, options)
            .await?;

        let mut n = 0;
        while cursor.advance().await? {
            let chunk = cursor.deserialize_current()?;
            if chunk.n != n {
                return Err(ErrorKind::GridFs(GridFsErrorKind::MissingChunk { n }).into());
            }

            let chunk_length = chunk.data.bytes.len();
            let expected_length = file.expected_chunk_length(n);
            if chunk_length != expected_length as usize {
                return Err(ErrorKind::GridFs(GridFsErrorKind::WrongSizeChunk {
                    actual_size: chunk_length,
                    expected_size: expected_length,
                    n,
                })
                .into());
            }

            destination.write_all(chunk.data.bytes).await?;
            n += 1;
        }

        if n != file.n() {
            return Err(ErrorKind::GridFs(GridFsErrorKind::WrongNumberOfChunks {
                actual_number: n,
                expected_number: file.n(),
            })
            .into());
        }

        Ok(())
    }
}

/// A stream from which a file stored in a GridFS bucket can be downloaded.
///
/// # Downloading from the Stream
/// The `GridFsDownloadStream` type implements [`futures_io::AsyncRead`]. It is recommended that
/// users call the utility methods in [`AsyncReadExt`](futures_util::io::AsyncReadExt) to interact
/// with the stream.
///
/// ```rust
/// # use mongodb::{bson::Bson, error::Result, gridfs::{GridFsBucket, GridFsDownloadStream}};
/// # async fn download_example(bucket: GridFsBucket, id: Bson) -> Result<()> {
/// use futures_util::io::AsyncReadExt;
///
/// let mut buf = Vec::new();
/// let mut download_stream = bucket.open_download_stream(id).await?;
/// download_stream.read_to_end(&mut buf).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Using [`tokio::io::AsyncRead`]
/// Users who prefer to use tokio's `AsyncRead` trait can use the [`tokio_util::compat`] module.
///
/// ```rust
/// # use mongodb::{bson::Bson, error::Result, gridfs::{GridFsBucket, GridFsUploadStream}};
/// # async fn compat_example(bucket: GridFsBucket, id: Bson) -> Result<()> {
/// use tokio_util::compat::FuturesAsyncReadCompatExt;
///
/// let futures_upload_stream = bucket.open_download_stream(id).await?;
/// let tokio_upload_stream = futures_upload_stream.compat();
/// # Ok(())
/// # }
/// ```
pub struct GridFsDownloadStream {
    state: State,
    current_n: u32,
    file: FilesCollectionDocument,
}

type GetBytesFuture = BoxFuture<'static, Result<(Vec<u8>, Box<Cursor<Chunk<'static>>>)>>;

enum State {
    // Idle is stored as an option so that its fields can be moved into a GetBytesFuture
    // without requiring ownership of the state. It can always be unwrapped safely.
    Idle(Option<Idle>),
    Busy(GetBytesFuture),
    Done,
}

struct Idle {
    buffer: Vec<u8>,
    cursor: Box<Cursor<Chunk<'static>>>,
}

impl State {
    fn set_busy(&mut self, new_future: GetBytesFuture) -> &mut GetBytesFuture {
        *self = State::Busy(new_future);
        match self {
            Self::Busy(ref mut future) => future,
            _ => unreachable!(),
        }
    }
}

impl GridFsDownloadStream {
    async fn new(
        file: FilesCollectionDocument,
        chunks: &Collection<Chunk<'static>>,
    ) -> Result<Self> {
        let initial_state = if file.length == 0 {
            State::Done
        } else {
            let options = FindOptions::builder().sort(doc! { "n": 1 }).build();
            let cursor = chunks.find(doc! { "files_id": &file.id }, options).await?;
            State::Idle(Some(Idle {
                buffer: Vec::new(),
                cursor: Box::new(cursor),
            }))
        };
        Ok(Self {
            state: initial_state,
            current_n: 0,
            file,
        })
    }
}

impl AsyncRead for GridFsDownloadStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::result::Result<usize, futures_util::io::Error>> {
        let stream = self.get_mut();

        let result = match &mut stream.state {
            State::Idle(idle) => {
                let Idle { buffer, cursor } = idle.take().unwrap();

                if !buffer.is_empty() {
                    Ok((buffer, cursor))
                } else {
                    let chunks_in_buf = FilesCollectionDocument::n_from_vals(
                        buf.len() as u64,
                        stream.file.chunk_size_bytes,
                    );
                    // We should read from current_n to chunks_in_buf + current_n, or, if that would
                    // exceed the total number of chunks in the file, to the last chunk in the file.
                    let final_n = std::cmp::min(chunks_in_buf + stream.current_n, stream.file.n());
                    let n_range = stream.current_n..final_n;

                    stream.current_n = final_n;

                    let new_future = stream.state.set_busy(
                        get_bytes(
                            cursor,
                            buffer,
                            n_range,
                            stream.file.chunk_size_bytes,
                            stream.file.length,
                        )
                        .boxed(),
                    );

                    match new_future.poll_unpin(cx) {
                        Poll::Ready(result) => result,
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }

            State::Busy(future) => match future.poll_unpin(cx) {
                Poll::Ready(result) => result,
                Poll::Pending => return Poll::Pending,
            },
            State::Done => return Poll::Ready(Ok(0)),
        };

        match result {
            Ok((mut buffer, cursor)) => {
                let bytes_to_write = std::cmp::min(buffer.len(), buf.len());
                buf[..bytes_to_write].copy_from_slice(buffer.drain(0..bytes_to_write).as_slice());

                stream.state = if !buffer.is_empty() || cursor.has_next() {
                    State::Idle(Some(Idle { buffer, cursor }))
                } else {
                    State::Done
                };

                Poll::Ready(Ok(bytes_to_write))
            }
            Err(error) => {
                stream.state = State::Done;
                Poll::Ready(Err(error.into_futures_io_error()))
            }
        }
    }
}

async fn get_bytes(
    mut cursor: Box<Cursor<Chunk<'static>>>,
    mut buffer: Vec<u8>,
    n_range: Range<u32>,
    chunk_size_bytes: u32,
    file_len: u64,
) -> Result<(Vec<u8>, Box<Cursor<Chunk<'static>>>)> {
    for n in n_range {
        if !cursor.advance().await? {
            return Err(ErrorKind::GridFs(GridFsErrorKind::MissingChunk { n }).into());
        }

        let chunk = cursor.deserialize_current()?;
        let chunk_bytes = chunk.data.bytes;

        if chunk.n != n {
            return Err(ErrorKind::GridFs(GridFsErrorKind::MissingChunk { n }).into());
        }

        let expected_len =
            FilesCollectionDocument::expected_chunk_length_from_vals(file_len, chunk_size_bytes, n);
        if chunk_bytes.len() != (expected_len as usize) {
            return Err(ErrorKind::GridFs(GridFsErrorKind::WrongSizeChunk {
                actual_size: chunk_bytes.len(),
                expected_size: expected_len,
                n,
            })
            .into());
        }

        buffer.extend_from_slice(chunk_bytes);
    }

    Ok((buffer, cursor))
}

// User functions for creating download streams.
impl GridFsBucket {
    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `id`.
    pub async fn open_download_stream(&self, id: Bson) -> Result<GridFsDownloadStream> {
        let file = self.find_file_by_id(&id).await?;
        GridFsDownloadStream::new(file, self.chunks()).await
    }

    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `filename`.
    ///
    /// If there are multiple files in the bucket with the given filename, the `revision` in the
    /// options provided is used to determine which one to download. See the documentation for
    /// [`GridFsDownloadByNameOptions`] for details on how to specify a revision. If no revision is
    /// provided, the file with `filename` most recently uploaded will be downloaded.
    pub async fn open_download_stream_by_name(
        &self,
        filename: impl AsRef<str>,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) -> Result<GridFsDownloadStream> {
        let file = self
            .find_file_by_name(filename.as_ref(), options.into())
            .await?;
        GridFsDownloadStream::new(file, self.chunks()).await
    }
}
