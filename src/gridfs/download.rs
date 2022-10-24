use std::{
    marker::Unpin,
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
    Cursor,
};

// Utility functions for finding files within the bucket.
impl GridFsBucket {
    async fn find_file_by_id(&self, id: &Bson) -> Result<FilesCollectionDocument> {
        match self.files().find_one(doc! { "_id": id }, None).await? {
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
    /// Downloads the contents of the stored file specified by `id` and writes
    /// the contents to the `destination`.
    pub async fn download_to_futures_0_3_writer<T>(&self, id: Bson, destination: T) -> Result<()>
    where
        T: AsyncWrite + Unpin,
    {
        let file = self.find_file_by_id(&id).await?;
        self.download_to_writer_common(file, destination).await
    }

    /// Downloads the contents of the stored file specified by `filename` and writes the contents to
    /// the `destination`. If there are multiple files with the same filename, the `revision` in the
    /// options provided is used to determine which one to download. If no `revision` is specified,
    /// the most recent file with the given filename is chosen.
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
                })
                .into());
            }

            destination.write_all(chunk.data.bytes).await?;
            n += 1;
        }

        if (n as u64) != file.n() {
            return Err(ErrorKind::GridFs(GridFsErrorKind::WrongNumberOfChunks {
                actual_number: n,
                expected_number: file.n() as u32,
            })
            .into());
        }

        Ok(())
    }
}

pub struct GridFsDownloadStream {
    state: State,
    file: FilesCollectionDocument,
}

type GetBytesFuture = BoxFuture<'static, Result<(Vec<u8>, Cursor<Chunk<'static>>)>>;

enum State {
    Idle {
        cursor: Box<Option<Cursor<Chunk<'static>>>>,
        cached_bytes: Option<Vec<u8>>,
    },
    Busy(GetBytesFuture),
    Done,
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
    /// Gets the file `id` for the stream.
    pub fn files_id(&self) -> &Bson {
        &self.file.id
    }
}

impl AsyncRead for GridFsDownloadStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::result::Result<usize, futures_util::io::Error>> {
        dbg!("in poll read");
        let stream = self.get_mut();

        let future = match &mut stream.state {
            State::Idle {
                cursor,
                cached_bytes,
            } => {
                let bytes_written = match cached_bytes {
                    Some(bytes) => {
                        if bytes.len() >= buf.len() {
                            buf.copy_from_slice(&bytes[..buf.len()]);
                            bytes.drain(..buf.len());
                            return Poll::Ready(Ok(buf.len()));
                        } else {
                            let len = bytes.len();
                            buf[..len].copy_from_slice(bytes);
                            bytes.clear();
                            len
                        }
                    }
                    None => 0,
                };

                if let Some(cursor) = cursor.take() {
                    let chunks_to_read = FilesCollectionDocument::n_from_vals(
                        (buf.len() - bytes_written) as u64,
                        stream.file.chunk_size,
                    );
                    let new_future = get_bytes(
                        cursor,
                        chunks_to_read,
                        stream.file.length,
                        stream.file.chunk_size,
                    )
                    .boxed();
                    stream.state.set_busy(new_future)
                } else {
                    return Poll::Ready(Ok(bytes_written));
                }
            }
            State::Busy(future) => future,
            State::Done => return Poll::Ready(Ok(0)),
        };

        let result = match future.poll_unpin(cx) {
            Poll::Ready(result) => result,
            Poll::Pending => return Poll::Pending,
        };

        match result {
            Ok((mut bytes, cursor)) => {
                let bytes_to_write = std::cmp::min(bytes.len(), buf.len());
                buf[..bytes_to_write].copy_from_slice(&bytes[..bytes_to_write]);

                let leftover = if bytes.len() > bytes_to_write {
                    bytes.drain(..bytes_to_write);
                    Some(bytes)
                } else {
                    None
                };

                if !cursor.is_exhausted() {
                    stream.state = State::Idle {
                        cursor: Box::new(Some(cursor)),
                        cached_bytes: leftover,
                    };
                } else if leftover.is_some() {
                    stream.state = State::Idle {
                        cursor: Box::new(None),
                        cached_bytes: leftover,
                    };
                } else {
                    stream.state = State::Done;
                }

                Poll::Ready(Ok(bytes_to_write))
            }
            Err(error) => {
                stream.state = State::Done;
                let error = futures_io::Error::new(futures_io::ErrorKind::Other, error);
                Poll::Ready(Err(error))
            }
        }
    }
}

async fn get_bytes(
    mut cursor: Cursor<Chunk<'static>>,
    chunks_to_read: u64,
    expected_file_len: u64,
    chunk_size: u32,
) -> Result<(Vec<u8>, Cursor<Chunk<'static>>)> {
    match get_bytes_inner(&mut cursor, chunks_to_read, expected_file_len, chunk_size).await {
        Ok(bytes) => Ok((bytes, cursor)),
        Err(error) => Err(error),
    }
}

async fn get_bytes_inner(
    cursor: &mut Cursor<Chunk<'static>>,
    chunks_to_read: u64,
    expected_file_len: u64,
    chunk_size: u32,
) -> Result<Vec<u8>> {
    let mut bytes = vec![];
    let mut current_n = 0u32;
    while cursor.advance().await? {
        let chunk = cursor.deserialize_current()?;
        let chunk_bytes = chunk.data.bytes;

        if chunk.n != current_n {
            return Err(ErrorKind::GridFs(GridFsErrorKind::MissingChunk { n: current_n }).into());
        }

        let expected_len = FilesCollectionDocument::expected_chunk_length_from_vals(
            expected_file_len,
            chunk_size,
            current_n,
        );
        if chunk_bytes.len() != (expected_len as usize) {
            return Err(ErrorKind::GridFs(GridFsErrorKind::WrongSizeChunk {
                actual_size: chunk.data.bytes.len(),
                expected_size: expected_len,
            })
            .into());
        }

        bytes.extend_from_slice(chunk_bytes);

        current_n += 1;
        if (current_n as u64) == chunks_to_read {
            break;
        }
    }

    let total_n = FilesCollectionDocument::n_from_vals(expected_file_len, chunk_size);
    if (current_n as u64) < chunks_to_read && (current_n as u64) < total_n {
        return Err(ErrorKind::GridFs(GridFsErrorKind::MissingChunk { n: current_n }).into());
    }

    Ok(bytes)
}

// User functions for creating download streams.
impl GridFsBucket {
    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `id`.
    pub async fn open_download_stream(&self, id: Bson) -> Result<GridFsDownloadStream> {
        let file = self.find_file_by_id(&id).await?;
        self.open_download_stream_common(file).await
    }

    /// Opens and returns a [`GridFsDownloadStream`] from which the application can read
    /// the contents of the stored file specified by `filename` and the revision
    /// in `options`.
    pub async fn open_download_stream_by_name(
        &self,
        filename: impl AsRef<str>,
        options: impl Into<Option<GridFsDownloadByNameOptions>>,
    ) -> Result<GridFsDownloadStream> {
        let file = self
            .find_file_by_name(filename.as_ref(), options.into())
            .await?;
        self.open_download_stream_common(file).await
    }

    async fn open_download_stream_common(
        &self,
        file: FilesCollectionDocument,
    ) -> Result<GridFsDownloadStream> {
        let initial_state = if file.length == 0 {
            State::Done
        } else {
            let options = FindOptions::builder().sort(doc! { "n": 1 }).build();
            let cursor = self
                .chunks()
                .find(doc! { "files_id": &file.id }, options)
                .await?;
            State::Idle {
                cursor: Box::new(Some(cursor)),
                cached_bytes: None,
            }
        };
        Ok(GridFsDownloadStream {
            state: initial_state,
            file,
        })
    }
}
