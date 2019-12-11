use std::{
    io::{self, Read, Write},
    sync::atomic::{AtomicI32, Ordering},
};

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
pub(super) fn write_cstring<W: Write>(writer: &mut W, string: &str) -> Result<()> {
    // Write the string's UTF-8 bytes.
    writer.write_all(string.as_bytes())?;

    // Write the null terminator.
    writer.write_all(&[0])?;

    Ok(())
}

/// A wrapper a `std::io::Read` that keeps track of the number of bytes it has read.
pub(super) struct CountReader<'a, R: 'a + Read> {
    reader: &'a mut R,
    bytes_read: usize,
}

impl<'a, R: 'a + Read> CountReader<'a, R> {
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

impl<'a, R: 'a + Read> Read for CountReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let size = self.reader.read(buf)?;
        self.bytes_read += size;

        Ok(size)
    }
}
