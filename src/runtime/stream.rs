use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::{cmap::conn::StreamOptions, error::Result};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A runtime-agnostic async stream possibly using TLS.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum AsyncStream {
    /// A basic TCP connection to the server.
    Tcp(AsyncStreamInner),

    /// A TLS connection over TCP.
    Tls(tokio_rustls::client::TlsStream<AsyncStreamInner>),
}

/// A runtime-agnostic async stream.
#[derive(Debug)]
pub(crate) enum AsyncStreamInner {
    /// Wrapper around `tokio::net:TcpStream`.
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::net::TcpStream),

    /// Wrapper around `async_std::net::TcpStream`.
    #[cfg(feature = "async-std-runtime")]
    AsyncStd(async_std::net::TcpStream),
}

impl AsyncStream {
    /// Creates a new Tokio TCP stream connected to the server as specified by `options`.
    #[cfg(feature = "tokio-runtime")]
    pub(super) async fn connect_tokio(options: StreamOptions) -> Result<Self> {
        use std::sync::Arc;

        use tokio::net::TcpStream;
        use tokio_rustls::TlsConnector;
        use webpki::DNSNameRef;

        let std_timeout = options.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);
        let timeout = tokio::time::Duration::new(std_timeout.as_secs(), std_timeout.subsec_nanos());

        // Start the future to connect the stream, but don't `await` it yet.
        let stream_future = TcpStream::connect((
            options.address.hostname.as_str(),
            options.address.port.unwrap_or(27017),
        ));

        // The URI options spec requires that the default is 10 seconds, but that 0 should indicate
        // no timeout.
        let inner = if timeout.as_nanos() == 0 {
            stream_future.await?
        } else {
            // `await` either the timeout or the stream connecting. We need `??` because `timeout`
            // returns a Result<Future<Output=T>>`, and `T` in this case is `Result<TcpStream>`.
            tokio::time::timeout(timeout, stream_future).await??
        };

        inner.set_nodelay(true)?;
        let inner = AsyncStreamInner::Tokio(inner);

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

    #[cfg(feature = "async-std-runtime")]
    pub(super) async fn connect_async_std(options: StreamOptions) -> Result<Self> {
        use std::sync::Arc;

        use async_std::net::TcpStream;
        use tokio_rustls::TlsConnector;
        use webpki::DNSNameRef;

        let timeout = options.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);

        let stream_future = TcpStream::connect((
            options.address.hostname.as_str(),
            options.address.port.unwrap_or(27017),
        ));

        // The URI options spec requires that the default is 10 seconds, but that 0 should indicate
        // no timeout.
        let inner = if timeout.as_nanos() == 0 {
            stream_future.await?
        } else {
            // `await` either the timeout or the stream connecting. We need `??` because `timeout`
            // returns a Result<Future<Output=T>>`, and `T` in this case is `Result<TcpStream>`.
            async_std::future::timeout(timeout, stream_future).await??
        };

        inner.set_nodelay(true)?;
        let inner = AsyncStreamInner::AsyncStd(inner);

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

impl AsyncRead for AsyncStreamInner {
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

impl AsyncWrite for AsyncStreamInner {
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
