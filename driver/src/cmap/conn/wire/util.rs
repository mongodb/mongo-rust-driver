use std::{
    pin::Pin,
    sync::atomic::{AtomicI32, Ordering},
    task::{ready, Poll},
};

use tokio::io::AsyncRead;

use crate::checked::Checked;

/// Closure to obtain a new, unique request ID.
pub(crate) fn next_request_id() -> i32 {
    static REQUEST_ID: AtomicI32 = AtomicI32::new(0);

    REQUEST_ID.fetch_add(1, Ordering::SeqCst)
}

pub(super) struct CountReader<R> {
    reader: R,
    bytes_read: Checked<usize>,
}

impl<R: AsyncRead> CountReader<R> {
    /// Constructs a new CountReader that wraps `reader`.
    pub(super) fn new(reader: R) -> Self {
        CountReader {
            reader,
            bytes_read: Checked::new(0),
        }
    }

    /// Gets the number of bytes read so far.
    pub(super) fn bytes_read(&self) -> Checked<usize> {
        self.bytes_read
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for CountReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = Checked::new(buf.filled().len());
        ready!(AsyncRead::poll_read(Pin::new(&mut self.reader), cx, buf))?;
        self.bytes_read += (buf.filled().len() - before)
            .get()
            .map_err(std::io::Error::other)?;
        Poll::Ready(Ok(()))
    }
}
