use std::{
    ops::Range,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{
    future::{BoxFuture, FutureExt},
    io::AsyncRead,
};

use super::{Chunk, FilesCollectionDocument};
use crate::{
    bson::doc,
    error::{ErrorKind, GridFsErrorKind, Result},
    Collection,
    Cursor,
};

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
/// If the destination is a local file (or other `AsyncWrite` byte sink), the contents of the stream
/// can be efficiently written to it with [`futures_util::io::copy`].
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
    pub(crate) async fn new(
        file: FilesCollectionDocument,
        chunks: &Collection<Chunk<'static>>,
    ) -> Result<Self> {
        let initial_state = if file.length == 0 {
            State::Done
        } else {
            let cursor = chunks
                .find(doc! { "files_id": &file.id })
                .sort(doc! { "n": 1 })
                .await?;
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
