use std::{
    io::{self, Read, Write},
    net::TcpStream,
};

use crate::error::Result;

/// Stream encapsulates the different socket types that can be used and adds a thin wrapper for I/O.
#[derive(Debug)]
pub(super) enum Stream {
    /// In order to add a `Connection` back to the pool when it's dropped, we need to take
    /// ownership of the connection from `&mut self` in `Drop::drop`. To facilitate this, we define
    /// a `Null` stream type which throws away all data written to it and never yields any data
    /// when read. This allows us to `std::mem::replace` the `Connection` and then return it to the
    /// pool.
    Null,

    /// A basic TCP connection to the server.
    Tcp(TcpStream),
    // TODO RUST-203: Add TLS streams.
}

impl Stream {
    /// Creates a new stream connected to `address`.
    pub(super) fn connect(address: &str) -> Result<Self> {
        let stream = TcpStream::connect(address)?;
        Ok(Self::Tcp(stream))
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Null => Ok(0),
            Self::Tcp(ref mut stream) => stream.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Null => Ok(0),
            Self::Tcp(ref mut stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Null => Ok(()),
            Self::Tcp(ref mut stream) => stream.flush(),
        }
    }
}
