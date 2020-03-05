use std::{
    pin::Pin,
    sync::atomic::{AtomicI32, Ordering},
    task::{Context, Poll},
};

use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::error::Result;

use lazy_static::lazy_static;

/// Closure to obtain a new, unique request ID.
pub(crate) fn next_request_id() -> i32 {
    lazy_static! {
        static ref REQUEST_ID: AtomicI32 = AtomicI32::new(0);
    }

    REQUEST_ID.fetch_add(1, Ordering::SeqCst)
}

/// Serializes `string` to bytes and writes them to `writer` with a null terminator appended.
pub(super) async fn write_cstring<W: AsyncWrite + Unpin>(
    writer: &mut W,
    string: &str,
) -> Result<()> {
    // Write the string's UTF-8 bytes.
    writer.write_all(string.as_bytes()).await?;

    // Write the null terminator.
    writer.write_all(&[0]).await?;

    Ok(())
}

/// A wrapper around `tokio::io::AsyncRead` that keeps track of the number of bytes it has read.
pub(super) struct CountReader<'a, R: AsyncRead + Unpin + Send + 'a> {
    reader: &'a mut R,
    bytes_read: usize,
}

impl<'a, R: AsyncRead + Unpin + Send + 'a> CountReader<'a, R> {
    /// Constructs a new CountReader that wraps `reader`.
    pub(super) fn new(reader: &'a mut R) -> Self {
        CountReader {
            reader,
            bytes_read: 0,
        }
    }

    /// Gets the number of bytes read so far.
    pub(super) fn bytes_read(&self) -> usize {
        self.bytes_read
    }
}

impl<'a, R: AsyncRead + Unpin + Send + 'a> AsyncRead for CountReader<'a, R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.reader).poll_read(cx, buf);

        if let Poll::Ready(Ok(count)) = result {
            self.bytes_read += count;
        }

        result
    }
}
