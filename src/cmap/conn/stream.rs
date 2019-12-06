use std::{
    io::{self, Read, Write},
    net::TcpStream,
    sync::Arc,
};

use derivative::Derivative;
use webpki::DNSNameRef;

use crate::{
    error::Result,
    options::{StreamAddress, TlsOptions},
};

/// Stream encapsulates the different socket types that can be used and adds a thin wrapper for I/O.
#[derive(Derivative)]
#[derivative(Debug)]
#[allow(clippy::large_enum_variant)]
pub(super) enum Stream {
    /// In order to add a `Connection` back to the pool when it's dropped, we need to take
    /// ownership of the connection from `&mut self` in `Drop::drop`. To facilitate this, we define
    /// a `Null` stream type which throws away all data written to it and never yields any data
    /// when read. This allows us to `std::mem::replace` the `Connection` and then return it to the
    /// pool.
    Null,

    /// A basic TCP connection to the server.
    Tcp(TcpStream),

    /// A TLS connection over TCP.
    Tls(#[derivative(Debug = "ignore")] rustls::StreamOwned<rustls::ClientSession, TcpStream>),
}

impl Stream {
    /// Creates a new stream connected to `address`.
    pub(super) fn connect(host: StreamAddress, tls_options: Option<TlsOptions>) -> Result<Self> {
        let inner = TcpStream::connect(host.to_string())?;
        inner.set_nodelay(true)?;

        match tls_options {
            Some(cfg) => {
                let name = DNSNameRef::try_from_ascii_str(&host.hostname)?;
                let mut tls_config = cfg.into_rustls_config()?;
                tls_config.enable_sni = true;

                let session = rustls::ClientSession::new(&Arc::new(tls_config), name);

                Ok(Stream::Tls(rustls::StreamOwned::new(session, inner)))
            }
            None => Ok(Self::Tcp(inner)),
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Null => Ok(0),
            Self::Tcp(ref mut stream) => stream.read(buf),
            Self::Tls(ref mut stream) => stream.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Null => Ok(0),
            Self::Tcp(ref mut stream) => stream.write(buf),
            Self::Tls(ref mut stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Null => Ok(()),
            Self::Tcp(ref mut stream) => stream.flush(),
            Self::Tls(ref mut stream) => stream.flush(),
        }
    }
}
