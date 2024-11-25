use std::{
    net::SocketAddr,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{
    error::{Error, ErrorKind, Result},
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
        #[cfg(feature = "tokio-runtime")]
        Tokio(tokio::net::UnixStream),

        /// Wrapper around `async_std::net::UnixStream`.
        #[cfg(feature = "async-std-runtime")]
        AsyncStd(async_std::os::unix::net::UnixStream),
    }

    #[cfg(feature = "tokio-runtime")]
    impl From<tokio::net::UnixStream> for AsyncUnixStream {
        fn from(stream: tokio::net::UnixStream) -> Self {
            Self::Tokio(stream)
        }
    }

    #[cfg(feature = "async-std-runtime")]
    impl From<async_std::os::unix::net::UnixStream> for AsyncUnixStream {
        fn from(stream: async_std::os::unix::net::UnixStream) -> Self {
            Self::AsyncStd(stream)
        }
    }

    impl AsyncUnixStream {
        #[cfg(feature = "tokio-runtime")]
        async fn try_connect(address: &Path) -> Result<Self> {
            use tokio::net::UnixStream;

            let stream = UnixStream::connect(address).await?;
            Ok(stream.into())
        }

        #[cfg(feature = "async-std-runtime")]
        async fn try_connect(address: &Path) -> Result<Self> {
            use async_std::os::unix::net::UnixStream;

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
                #[cfg(feature = "tokio-runtime")]
                Self::Tokio(ref mut inner) => Pin::new(inner).poll_read(cx, buf),

                #[cfg(feature = "async-std-runtime")]
                Self::AsyncStd(ref mut inner) => {
                    use tokio_util::compat::FuturesAsyncReadCompatExt;

                    Pin::new(&mut inner.compat()).poll_read(cx, buf)
                }
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
                #[cfg(feature = "tokio-runtime")]
                Self::Tokio(ref mut inner) => Pin::new(inner).poll_write(cx, buf),

                #[cfg(feature = "async-std-runtime")]
                Self::AsyncStd(ref mut inner) => {
                    use tokio_util::compat::FuturesAsyncReadCompatExt;

                    Pin::new(&mut inner.compat()).poll_write(cx, buf)
                }
            }
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<tokio::io::Result<()>> {
            match self.deref_mut() {
                #[cfg(feature = "tokio-runtime")]
                Self::Tokio(ref mut inner) => Pin::new(inner).poll_flush(cx),

                #[cfg(feature = "async-std-runtime")]
                Self::AsyncStd(ref mut inner) => {
                    use tokio_util::compat::FuturesAsyncReadCompatExt;

                    Pin::new(&mut inner.compat()).poll_flush(cx)
                }
            }
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context,
        ) -> Poll<tokio::io::Result<()>> {
            match self.deref_mut() {
                #[cfg(feature = "tokio-runtime")]
                Self::Tokio(ref mut inner) => Pin::new(inner).poll_shutdown(cx),

                #[cfg(feature = "async-std-runtime")]
                Self::AsyncStd(ref mut inner) => {
                    use tokio_util::compat::FuturesAsyncReadCompatExt;

                    Pin::new(&mut inner.compat()).poll_shutdown(cx)
                }
            }
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[futures_io::IoSlice<'_>],
        ) -> Poll<std::result::Result<usize, std::io::Error>> {
            match self.get_mut() {
                #[cfg(feature = "tokio-runtime")]
                Self::Tokio(ref mut inner) => Pin::new(inner).poll_write_vectored(cx, bufs),

                #[cfg(feature = "async-std-runtime")]
                Self::AsyncStd(ref mut inner) => {
                    use tokio_util::compat::FuturesAsyncReadCompatExt;

                    Pin::new(&mut inner.compat()).poll_write_vectored(cx, bufs)
                }
            }
        }

        fn is_write_vectored(&self) -> bool {
            match self {
                #[cfg(feature = "tokio-runtime")]
                Self::Tokio(ref inner) => inner.is_write_vectored(),

                #[cfg(feature = "async-std-runtime")]
                Self::AsyncStd(_) => false,
            }
        }
    }
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
    #[cfg(feature = "tokio-runtime")]
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

    #[cfg(feature = "async-std-runtime")]
    async fn try_connect(address: &SocketAddr) -> Result<Self> {
        use async_std::net::TcpStream;

        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;

        let std_stream: std::net::TcpStream = stream.try_into()?;
        let socket = socket2::Socket::from(std_stream);
        let conf = socket2::TcpKeepalive::new().with_time(KEEPALIVE_TIME);
        socket.set_tcp_keepalive(&conf)?;
        let std_stream = std::net::TcpStream::from(socket);
        let stream = TcpStream::from(std_stream);

        Ok(stream.into())
    }

    pub(crate) async fn connect(address: &ServerAddress) -> Result<Self> {
        // "Happy Eyeballs": try addresses in parallel, interleaving IPv6 and IPv4, preferring IPv6.
        // Based on the implementation in https://codeberg.org/KMK/happy-eyeballs.
        let (addrs_v6, addrs_v4): (Vec<_>, Vec<_>) = runtime::resolve_address(address)
            .await?
            .partition(|a| matches!(a, SocketAddr::V6(_)));
        let socket_addrs = interleave(addrs_v6, addrs_v4);

        if socket_addrs.is_empty() {
            return Err(ErrorKind::DnsResolve {
                message: format!("No DNS results for domain {}", address),
            }
            .into());
        }

        fn handle_join(
            result: std::result::Result<Result<AsyncTcpStream>, tokio::task::JoinError>,
        ) -> Result<AsyncTcpStream> {
            match result {
                Ok(r) => r,
                // JoinError indicates the task was cancelled or paniced, which should never happen
                // here.
                Err(e) => Err(Error::internal(format!("TCP connect task failure: {}", e))),
            }
        }

        static CONNECTION_ATTEMPT_DELAY: Duration = Duration::from_millis(250);

        // Race connections
        let mut attempts = tokio::task::JoinSet::new();
        let mut connect_error = None;
        'spawn: for a in socket_addrs {
            attempts.spawn(async move { Self::try_connect(&a).await });
            let sleep = tokio::time::sleep(CONNECTION_ATTEMPT_DELAY);
            tokio::pin!(sleep); // required for select!
            while !attempts.is_empty() {
                tokio::select! {
                    biased;
                    connect_res = attempts.join_next() => {
                        match connect_res.map(handle_join) {
                            // The gating `while !attempts.is_empty()` should mean this never happens.
                            None => return Err(Error::internal("empty TCP connect task set")),
                            // A connection succeeded, return it. The JoinSet will cancel remaining tasks on drop.
                            Some(Ok(cnx)) => return Ok(cnx),
                            // A connection failed. Remember the error and wait for any other remaining attempts.
                            Some(Err(e)) => {
                                connect_error.get_or_insert(e);
                            },
                        }
                    }
                    // CONNECTION_ATTEMPT_DELAY expired, spawn a new connection attempt.
                    _ = &mut sleep => continue 'spawn
                }
            }
        }

        // No more address to try. Drain the attempts until one succeeds.
        while let Some(result) = attempts.join_next().await {
            match handle_join(result) {
                Ok(cnx) => return Ok(cnx),
                Err(e) => {
                    connect_error.get_or_insert(e);
                }
            }
        }

        // All attempts failed.  Return the first error.
        Err(connect_error.unwrap_or_else(|| {
            ErrorKind::Internal {
                message: "connecting to all DNS results failed but no error reported".to_string(),
            }
            .into()
        }))
    }
}

fn interleave<T>(left: Vec<T>, right: Vec<T>) -> Vec<T> {
    let mut out = Vec::with_capacity(left.len() + right.len());
    let (mut left, mut right) = (left.into_iter(), right.into_iter());
    while let Some(a) = left.next() {
        out.push(a);
        std::mem::swap(&mut left, &mut right);
    }
    out.extend(right);
    out
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
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut inner) => Pin::new(inner).poll_read(cx, buf),

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut inner) => {
                use tokio_util::compat::FuturesAsyncReadCompatExt;

                Pin::new(&mut inner.compat()).poll_read(cx, buf)
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
            Self::Tokio(ref mut inner) => Pin::new(inner).poll_write(cx, buf),

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut inner) => {
                use tokio_util::compat::FuturesAsyncReadCompatExt;

                Pin::new(&mut inner.compat()).poll_write(cx, buf)
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<tokio::io::Result<()>> {
        match self.deref_mut() {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut inner) => Pin::new(inner).poll_flush(cx),

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut inner) => {
                use tokio_util::compat::FuturesAsyncReadCompatExt;

                Pin::new(&mut inner.compat()).poll_flush(cx)
            }
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<tokio::io::Result<()>> {
        match self.deref_mut() {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut inner) => Pin::new(inner).poll_shutdown(cx),

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut inner) => {
                use tokio_util::compat::FuturesAsyncReadCompatExt;

                Pin::new(&mut inner.compat()).poll_shutdown(cx)
            }
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[futures_io::IoSlice<'_>],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        match self.get_mut() {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut inner) => Pin::new(inner).poll_write_vectored(cx, bufs),

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut inner) => {
                use tokio_util::compat::FuturesAsyncReadCompatExt;

                Pin::new(&mut inner.compat()).poll_write_vectored(cx, bufs)
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref inner) => inner.is_write_vectored(),

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(_) => false,
        }
    }
}
