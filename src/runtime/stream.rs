use std::{
    net::SocketAddr,
    ops::DerefMut,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncRead as TokioAsyncRead, AsyncWrite as TokioAsyncWrite};
use tokio_rustls::TlsConnector;
use webpki::DNSNameRef;

use crate::{
    cmap::options::StreamOptions,
    error::{ErrorKind, Result},
    options::StreamAddress,
    RUNTIME,
};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const KEEPALIVE_TIME: Duration = Duration::from_secs(120);

/// A runtime-agnostic async stream possibly using TLS.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum AsyncStream {
    Null,

    /// A basic TCP connection to the server.
    Tcp(AsyncTcpStream),

    /// A TLS connection over TCP.
    Tls(tokio_rustls::client::TlsStream<AsyncTcpStream>),
}

/// A runtime-agnostic async stream.
#[derive(Debug)]
pub(crate) enum AsyncTcpStream {
    /// Wrapper around `tokio::net:TcpStream`.
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::net::TcpStream),

    /// Wrapper around `async_std::net::TcpStream`.
    #[cfg(feature = "async-std-runtime")]
    AsyncStd(async_std::net::TcpStream),
}

#[cfg(feature = "tokio-runtime")]
impl From<tokio::net::TcpStream> for AsyncTcpStream {
    fn from(stream: tokio::net::TcpStream) -> Self {
        Self::Tokio(stream)
    }
}

#[cfg(feature = "async-std-runtime")]
impl From<async_std::net::TcpStream> for AsyncTcpStream {
    fn from(stream: async_std::net::TcpStream) -> Self {
        Self::AsyncStd(stream)
    }
}

impl AsyncTcpStream {
    fn try_connect_common(
        address: &SocketAddr,
        connect_timeout: Duration,
    ) -> Result<std::net::TcpStream> {
        use socket2::{Domain, Protocol, SockAddr, Socket, Type};

        let domain = match address {
            SocketAddr::V4(_) => Domain::ipv4(),
            SocketAddr::V6(_) => Domain::ipv6(),
        };
        let socket = Socket::new(domain, Type::stream(), Some(Protocol::tcp()))?;
        socket.set_keepalive(Some(KEEPALIVE_TIME))?;
        socket.set_nonblocking(true)?;

        let address: SockAddr = address.clone().into();
        if connect_timeout == Duration::from_secs(0) {
            socket.connect(&address)?;
        } else {
            socket.connect_timeout(&address, connect_timeout)?;
        }
        socket.set_nodelay(true)?;

        Ok(socket.into_tcp_stream())
    }

    #[cfg(feature = "tokio-runtime")]
    async fn try_connect(address: &SocketAddr, connect_timeout: Duration) -> Result<Self> {
        use tokio::net::TcpStream;

        let addr_c = address.clone();
        let connect_task = tokio::task::spawn_blocking(move || Self::try_connect_common(&addr_c, connect_timeout));
        let stream = TcpStream::from_std(connect_task.await.map_err(|e| std::io::Error::from(e))??)?;
        Ok(stream.into())
    }

    #[cfg(feature = "async-std-runtime")]
    async fn try_connect(address: &SocketAddr, connect_timeout: Duration) -> Result<Self> {
        use async_std::net::TcpStream;

        let stream: TcpStream = Self::try_connect_common(address, connect_timeout)?.into();
        Ok(stream.into())
    }

    pub(crate) async fn connect_socket_addr(
        address: &SocketAddr,
        connect_timeout: Option<Duration>,
    ) -> Result<Self> {
        let timeout = connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);
        Self::try_connect(address, timeout).await
    }

    async fn connect(address: &StreamAddress, connect_timeout: Option<Duration>) -> Result<Self> {
        let timeout = connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);

        let mut socket_addrs: Vec<_> = RUNTIME.resolve_address(address).await?.collect();

        if socket_addrs.is_empty() {
            return Err(ErrorKind::NoDnsResults(address.clone()).into());
        }

        // After considering various approaches, we decided to do what other drivers do, namely try
        // each of the addresses in sequence with a preference for IPv4.
        socket_addrs.sort_by_key(|addr| if addr.is_ipv4() { 0 } else { 1 });

        let mut connect_error = None;

        for address in &socket_addrs {
            connect_error = match Self::try_connect(address, timeout).await {
                Ok(stream) => return Ok(stream),
                Err(err) => Some(err),
            };
        }

        Err(connect_error.unwrap_or_else(|| ErrorKind::NoDnsResults(address.clone()).into()))
    }
}

impl AsyncStream {
    /// Creates a new Tokio TCP stream connected to the server as specified by `options`.
    pub(crate) async fn connect(options: StreamOptions) -> Result<Self> {
        let inner = AsyncTcpStream::connect(&options.address, options.connect_timeout).await?;

        // If there are TLS options, wrap the inner stream with rustls.
        match options.tls_options {
            Some(cfg) => {
                let name = DNSNameRef::try_from_ascii_str(&options.address.hostname)?;
                let mut tls_config = cfg.into_rustls_config()?;
                tls_config.enable_sni = true;

                let connector: TlsConnector = Arc::new(tls_config).into();
                let session = connector.connect(name, inner).await?;

                Ok(Self::Tls(session))
            }
            None => Ok(Self::Tcp(inner)),
        }
    }
}

impl AsyncRead for AsyncStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.deref_mut() {
            Self::Null => Poll::Ready(Ok(0)),
            Self::Tcp(ref mut inner) => AsyncRead::poll_read(Pin::new(inner), cx, buf),
            Self::Tls(ref mut inner) => {
                let mut buf = tokio::io::ReadBuf::new(buf);
                Pin::new(inner)
                    .poll_read(cx, &mut buf)
                    .map_ok(|_| buf.filled().len())
            }
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
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.deref_mut() {
            Self::Null => Poll::Ready(Ok(())),
            Self::Tcp(ref mut inner) => AsyncWrite::poll_flush(Pin::new(inner), cx),
            Self::Tls(ref mut inner) => Pin::new(inner).poll_flush(cx),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.deref_mut() {
            Self::Null => Poll::Ready(Ok(())),
            Self::Tcp(ref mut inner) => Pin::new(inner).poll_close(cx),
            Self::Tls(ref mut inner) => Pin::new(inner).poll_shutdown(cx),
        }
    }
}

impl AsyncRead for AsyncTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.deref_mut() {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut stream) => {
                // Is there a better way to do this?
                let mut buf = tokio::io::ReadBuf::new(buf);
                Pin::new(stream)
                    .poll_read(cx, &mut buf)
                    .map_ok(|_| buf.filled().len())
            }

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AsyncTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.deref_mut() {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut stream) => {
                use tokio::io::AsyncWrite;

                Pin::new(stream).poll_write(cx, buf)
            }

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.deref_mut() {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut stream) => {
                use tokio::io::AsyncWrite;

                Pin::new(stream).poll_flush(cx)
            }

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.deref_mut() {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut stream) => {
                use tokio::io::AsyncWrite;

                Pin::new(stream).poll_shutdown(cx)
            }

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut stream) => Pin::new(stream).poll_close(cx),
        }
    }
}

// These trait implementations are required to interface with tokio-rustls.

impl TokioAsyncRead for AsyncTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf,
    ) -> Poll<tokio::io::Result<()>> {
        match self.deref_mut() {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut stream) => {
                let s = buf.initialize_unfilled();
                let bread = match Pin::new(stream).poll_read(cx, s) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(b) => b?,
                };

                buf.advance(bread);
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl TokioAsyncWrite for AsyncTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<tokio::io::Result<usize>> {
        AsyncWrite::poll_write(self, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<tokio::io::Result<()>> {
        AsyncWrite::poll_flush(self, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<tokio::io::Result<()>> {
        AsyncWrite::poll_close(self, cx)
    }
}
