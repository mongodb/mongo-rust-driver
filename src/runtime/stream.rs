use std::{
    net::{SocketAddr, ToSocketAddrs},
    ops::DerefMut,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsConnector;
use webpki::DNSNameRef;

use crate::{
    cmap::conn::StreamOptions,
    error::{ErrorKind, Result},
    options::StreamAddress,
};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A runtime-agnostic async stream possibly using TLS.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum AsyncStream {
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
    fn set_nodelay(&self) -> Result<()> {
        match self {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref stream) => stream.set_nodelay(true)?,

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref stream) => stream.set_nodelay(true)?,
        };

        Ok(())
    }

    async fn try_connect(address: &SocketAddr, connect_timeout: Duration) -> Result<Self> {
        #[cfg(feature = "tokio-runtime")]
        use tokio::{net::TcpStream, time::timeout};

        #[cfg(feature = "async-std-runtime")]
        use async_std::{future::timeout, net::TcpStream};

        let stream_future = TcpStream::connect(address);

        // The URI options spec requires that the default connect timeout is 10 seconds, but that 0
        // should indicate no timeout.
        let stream: Self = if connect_timeout == Duration::from_secs(0) {
            stream_future.await?.into()
        } else {
            timeout(connect_timeout, stream_future).await??.into()
        };

        stream.set_nodelay()?;

        Ok(stream)
    }

    async fn connect(address: &StreamAddress, connect_timeout: Option<Duration>) -> Result<Self> {
        let timeout = connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);

        let mut socket_addrs: Vec<_> = address.to_socket_addrs()?.collect();

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
    pub(super) async fn connect(options: StreamOptions) -> Result<Self> {
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
    ) -> Poll<tokio::io::Result<usize>> {
        match self.deref_mut() {
            Self::Tcp(ref mut inner) => Pin::new(inner).poll_read(cx, buf),
            Self::Tls(ref mut inner) => Pin::new(inner).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AsyncStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<tokio::io::Result<usize>> {
        match self.deref_mut() {
            Self::Tcp(ref mut inner) => Pin::new(inner).poll_write(cx, buf),
            Self::Tls(ref mut inner) => Pin::new(inner).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
        match self.deref_mut() {
            Self::Tcp(ref mut inner) => Pin::new(inner).poll_flush(cx),
            Self::Tls(ref mut inner) => Pin::new(inner).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<tokio::io::Result<()>> {
        match self.deref_mut() {
            Self::Tcp(ref mut inner) => Pin::new(inner).poll_shutdown(cx),
            Self::Tls(ref mut inner) => Pin::new(inner).poll_shutdown(cx),
        }
    }
}

impl AsyncRead for AsyncTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<tokio::io::Result<usize>> {
        match self.deref_mut() {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut stream) => Pin::new(stream).poll_read(cx, buf),

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut stream) => {
                use async_std::io::Read;

                Pin::new(stream).poll_read(cx, buf)
            }
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
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut stream) => Pin::new(stream).poll_write(cx, buf),

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut stream) => {
                use async_std::io::Write;

                Pin::new(stream).poll_write(cx, buf)
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
        match self.deref_mut() {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut stream) => Pin::new(stream).poll_flush(cx),

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut stream) => {
                use async_std::io::Write;

                Pin::new(stream).poll_flush(cx)
            }
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<tokio::io::Result<()>> {
        match self.deref_mut() {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut stream) => Pin::new(stream).poll_shutdown(cx),

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut stream) => {
                use async_std::io::Write;

                Pin::new(stream).poll_close(cx)
            }
        }
    }
}
