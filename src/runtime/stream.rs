use std::{
    net::SocketAddr,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{
    error::{ErrorKind, Result},
    options::ServerAddress,
    runtime,
};

use super::{tls::AsyncTlsStream, TlsConfig};

pub(crate) const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const KEEPALIVE_TIME: Duration = Duration::from_secs(120);

/// A runtime-agnostic async stream possibly using TLS.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum AsyncStream {
    Null,

    /// A basic TCP connection to the server.
    Tcp(AsyncTcpStream),

    /// A TLS connection over TCP.
    Tls(AsyncTlsStream),

    /// A Unix domain socket connection.
    #[cfg(unix)]
    Unix(unix::AsyncUnixStream),
}

impl AsyncStream {
    pub(crate) async fn connect(
        address: ServerAddress,
        tls_cfg: Option<&TlsConfig>,
    ) -> Result<Self> {
        match &address {
            ServerAddress::Tcp { host, .. } => {
                let inner = AsyncTcpStream::connect(&address).await?;

                // If there are TLS options, wrap the inner stream in an AsyncTlsStream.
                match tls_cfg {
                    Some(cfg) => Ok(AsyncStream::Tls(
                        AsyncTlsStream::connect(host, inner, cfg).await?,
                    )),
                    None => Ok(AsyncStream::Tcp(inner)),
                }
            }
            #[cfg(unix)]
            ServerAddress::Unix { .. } => Ok(AsyncStream::Unix(
                unix::AsyncUnixStream::connect(&address).await?,
            )),
        }
    }
}

/// A runtime-agnostic async unix domain socket stream.
#[cfg(unix)]
mod unix {
    use std::{
        ops::DerefMut,
        path::Path,
        pin::Pin,
        task::{Context, Poll},
    };

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    use crate::{client::options::ServerAddress, error::Result};

    #[derive(Debug)]
    pub(crate) enum AsyncUnixStream {
        /// Wrapper around `tokio::net:UnixStream`.
        Tokio(tokio::net::UnixStream),
    }

    impl From<tokio::net::UnixStream> for AsyncUnixStream {
        fn from(stream: tokio::net::UnixStream) -> Self {
            Self::Tokio(stream)
        }
    }

    impl AsyncUnixStream {
        async fn try_connect(address: &Path) -> Result<Self> {
            use tokio::net::UnixStream;

            let stream = UnixStream::connect(address).await?;
            Ok(stream.into())
        }

        pub(crate) async fn connect(address: &ServerAddress) -> Result<Self> {
            debug_assert!(
                matches!(address, ServerAddress::Unix { .. }),
                "address must be unix"
            );

            match address {
                ServerAddress::Unix { ref path } => Self::try_connect(path.as_path()).await,
                _ => unreachable!(),
            }
        }
    }

    impl AsyncRead for AsyncUnixStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf,
        ) -> Poll<tokio::io::Result<()>> {
            match self.deref_mut() {
                Self::Tokio(ref mut inner) => Pin::new(inner).poll_read(cx, buf),
            }
        }
    }

    impl AsyncWrite for AsyncUnixStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<tokio::io::Result<usize>> {
            match self.deref_mut() {
                Self::Tokio(ref mut inner) => Pin::new(inner).poll_write(cx, buf),
            }
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<tokio::io::Result<()>> {
            match self.deref_mut() {
                Self::Tokio(ref mut inner) => Pin::new(inner).poll_flush(cx),
            }
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context,
        ) -> Poll<tokio::io::Result<()>> {
            match self.deref_mut() {
                Self::Tokio(ref mut inner) => Pin::new(inner).poll_shutdown(cx),
            }
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[futures_io::IoSlice<'_>],
        ) -> Poll<std::result::Result<usize, std::io::Error>> {
            match self.get_mut() {
                Self::Tokio(ref mut inner) => Pin::new(inner).poll_write_vectored(cx, bufs),
            }
        }

        fn is_write_vectored(&self) -> bool {
            match self {
                Self::Tokio(ref inner) => inner.is_write_vectored(),
            }
        }
    }
}

/// A runtime-agnostic async stream.
#[derive(Debug)]
pub(crate) enum AsyncTcpStream {
    /// Wrapper around `tokio::net:TcpStream`.
    Tokio(tokio::net::TcpStream),
}

impl From<tokio::net::TcpStream> for AsyncTcpStream {
    fn from(stream: tokio::net::TcpStream) -> Self {
        Self::Tokio(stream)
    }
}

impl AsyncTcpStream {
    async fn try_connect(address: &SocketAddr) -> Result<Self> {
        use tokio::net::TcpStream;

        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;

        let socket = socket2::Socket::from(stream.into_std()?);
        let conf = socket2::TcpKeepalive::new().with_time(KEEPALIVE_TIME);
        socket.set_tcp_keepalive(&conf)?;
        let std_stream = std::net::TcpStream::from(socket);
        let stream = TcpStream::from_std(std_stream)?;

        Ok(stream.into())
    }

    pub(crate) async fn connect(address: &ServerAddress) -> Result<Self> {
        let mut socket_addrs: Vec<_> = runtime::resolve_address(address).await?.collect();

        if socket_addrs.is_empty() {
            return Err(ErrorKind::DnsResolve {
                message: format!("No DNS results for domain {}", address),
            }
            .into());
        }

        // After considering various approaches, we decided to do what other drivers do, namely try
        // each of the addresses in sequence with a preference for IPv4.
        socket_addrs.sort_by_key(|addr| if addr.is_ipv4() { 0 } else { 1 });

        let mut connect_error = None;

        for address in &socket_addrs {
            connect_error = match Self::try_connect(address).await {
                Ok(stream) => return Ok(stream),
                Err(err) => Some(err),
            };
        }

        Err(connect_error.unwrap_or_else(|| {
            ErrorKind::Internal {
                message: "connecting to all DNS results failed but no error reported".to_string(),
            }
            .into()
        }))
    }
}

impl tokio::io::AsyncRead for AsyncStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.deref_mut() {
            Self::Null => Poll::Ready(Ok(())),
            Self::Tcp(ref mut inner) => tokio::io::AsyncRead::poll_read(Pin::new(inner), cx, buf),
            Self::Tls(ref mut inner) => tokio::io::AsyncRead::poll_read(Pin::new(inner), cx, buf),
            #[cfg(unix)]
            Self::Unix(ref mut inner) => tokio::io::AsyncRead::poll_read(Pin::new(inner), cx, buf),
        }
    }
}

impl AsyncWrite for AsyncStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.deref_mut() {
            Self::Null => Poll::Ready(Ok(0)),
            Self::Tcp(ref mut inner) => AsyncWrite::poll_write(Pin::new(inner), cx, buf),
            Self::Tls(ref mut inner) => Pin::new(inner).poll_write(cx, buf),
            #[cfg(unix)]
            Self::Unix(ref mut inner) => AsyncWrite::poll_write(Pin::new(inner), cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.deref_mut() {
            Self::Null => Poll::Ready(Ok(())),
            Self::Tcp(ref mut inner) => AsyncWrite::poll_flush(Pin::new(inner), cx),
            Self::Tls(ref mut inner) => Pin::new(inner).poll_flush(cx),
            #[cfg(unix)]
            Self::Unix(ref mut inner) => AsyncWrite::poll_flush(Pin::new(inner), cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.deref_mut() {
            Self::Null => Poll::Ready(Ok(())),
            Self::Tcp(ref mut inner) => Pin::new(inner).poll_shutdown(cx),
            Self::Tls(ref mut inner) => Pin::new(inner).poll_shutdown(cx),
            #[cfg(unix)]
            Self::Unix(ref mut inner) => Pin::new(inner).poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[futures_io::IoSlice<'_>],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        match self.get_mut() {
            Self::Null => Poll::Ready(Ok(0)),
            Self::Tcp(ref mut inner) => Pin::new(inner).poll_write_vectored(cx, bufs),
            Self::Tls(ref mut inner) => Pin::new(inner).poll_write_vectored(cx, bufs),
            #[cfg(unix)]
            Self::Unix(ref mut inner) => Pin::new(inner).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Null => false,
            Self::Tcp(ref inner) => inner.is_write_vectored(),
            Self::Tls(ref inner) => inner.is_write_vectored(),
            #[cfg(unix)]
            Self::Unix(ref inner) => inner.is_write_vectored(),
        }
    }
}

impl AsyncRead for AsyncTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf,
    ) -> Poll<tokio::io::Result<()>> {
        match self.deref_mut() {
            Self::Tokio(ref mut inner) => Pin::new(inner).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AsyncTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<tokio::io::Result<usize>> {
        match self.deref_mut() {
            Self::Tokio(ref mut inner) => Pin::new(inner).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<tokio::io::Result<()>> {
        match self.deref_mut() {
            Self::Tokio(ref mut inner) => Pin::new(inner).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<tokio::io::Result<()>> {
        match self.deref_mut() {
            Self::Tokio(ref mut inner) => Pin::new(inner).poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[futures_io::IoSlice<'_>],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        match self.get_mut() {
            Self::Tokio(ref mut inner) => Pin::new(inner).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Tokio(ref inner) => inner.is_write_vectored(),
        }
    }
}
