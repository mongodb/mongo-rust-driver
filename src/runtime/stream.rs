use std::{
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use pin_project::pin_project;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

use crate::{
    error::{Error, ErrorKind, Result},
    options::ServerAddress,
    runtime,
};

use super::{
    tls::{tls_connect, TlsStream},
    TlsConfig,
};

pub(crate) const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(not(target_os = "wasi"))]
const KEEPALIVE_TIME: Duration = Duration::from_secs(120);

/// An async stream possibly using TLS.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
#[pin_project(project = AsyncStreamProj)]
pub(crate) enum AsyncStream {
    Null,

    /// A basic TCP connection to the server.
    Tcp(#[pin] TcpStream),

    /// A TLS connection over TCP.
    Tls(#[pin] TlsStream),

    /// A Unix domain socket connection.
    #[cfg(unix)]
    Unix(#[pin] tokio::net::UnixStream),
}

impl AsyncStream {
    pub(crate) async fn connect(
        address: ServerAddress,
        tls_cfg: Option<&TlsConfig>,
        socket_timeout: Option<Duration>,
    ) -> Result<Self> {
        match &address {
            ServerAddress::Tcp { host, .. } => {
                let resolved: Vec<_> = runtime::resolve_address(&address).await?.collect();
                if resolved.is_empty() {
                    return Err(ErrorKind::DnsResolve {
                        message: format!("No DNS results for domain {}", address),
                    }
                    .into());
                }
                let inner = tcp_connect(resolved, socket_timeout).await?;

                // If there are TLS options, wrap the inner stream in an AsyncTlsStream.
                match tls_cfg {
                    Some(cfg) => Ok(AsyncStream::Tls(tls_connect(host, inner, cfg).await?)),
                    None => Ok(AsyncStream::Tcp(inner)),
                }
            }
            #[cfg(unix)]
            ServerAddress::Unix { path } => Ok(AsyncStream::Unix(
                tokio::net::UnixStream::connect(path.as_path()).await?,
            )),
        }
    }
}

async fn tcp_try_connect(
    address: &SocketAddr,
    socket_timeout: Option<Duration>,
) -> Result<TcpStream> {
    let stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;

    let socket = socket2::Socket::from(stream.into_std()?);
    #[cfg(not(target_os = "wasi"))]
    {
        let conf = socket2::TcpKeepalive::new().with_time(KEEPALIVE_TIME);
        socket.set_tcp_keepalive(&conf)?;
    }
    socket.set_write_timeout(socket_timeout)?;
    socket.set_read_timeout(socket_timeout)?;
    let std_stream = std::net::TcpStream::from(socket);
    Ok(TcpStream::from_std(std_stream)?)
}

pub(crate) async fn tcp_connect(
    resolved: Vec<SocketAddr>,
    socket_timeout: Option<Duration>,
) -> Result<TcpStream> {
    // "Happy Eyeballs": try addresses in parallel, interleaving IPv6 and IPv4, preferring IPv6.
    // Based on the implementation in https://codeberg.org/KMK/happy-eyeballs.
    let (addrs_v6, addrs_v4): (Vec<_>, Vec<_>) = resolved
        .into_iter()
        .partition(|a| matches!(a, SocketAddr::V6(_)));
    let socket_addrs = interleave(addrs_v6, addrs_v4);

    fn handle_join(
        result: std::result::Result<Result<TcpStream>, tokio::task::JoinError>,
    ) -> Result<TcpStream> {
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
        attempts.spawn(async move { tcp_try_connect(&a, socket_timeout).await });
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

impl AsyncRead for AsyncStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.project() {
            AsyncStreamProj::Null => Poll::Ready(Ok(())),
            AsyncStreamProj::Tcp(inner) => inner.poll_read(cx, buf),
            AsyncStreamProj::Tls(inner) => inner.poll_read(cx, buf),
            #[cfg(unix)]
            AsyncStreamProj::Unix(inner) => inner.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AsyncStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.project() {
            AsyncStreamProj::Null => Poll::Ready(Ok(0)),
            AsyncStreamProj::Tcp(inner) => inner.poll_write(cx, buf),
            AsyncStreamProj::Tls(inner) => inner.poll_write(cx, buf),
            #[cfg(unix)]
            AsyncStreamProj::Unix(inner) => inner.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            AsyncStreamProj::Null => Poll::Ready(Ok(())),
            AsyncStreamProj::Tcp(inner) => inner.poll_flush(cx),
            AsyncStreamProj::Tls(inner) => inner.poll_flush(cx),
            #[cfg(unix)]
            AsyncStreamProj::Unix(inner) => inner.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            AsyncStreamProj::Null => Poll::Ready(Ok(())),
            AsyncStreamProj::Tcp(inner) => inner.poll_shutdown(cx),
            AsyncStreamProj::Tls(inner) => inner.poll_shutdown(cx),
            #[cfg(unix)]
            AsyncStreamProj::Unix(inner) => inner.poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[futures_io::IoSlice<'_>],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        match self.project() {
            AsyncStreamProj::Null => Poll::Ready(Ok(0)),
            AsyncStreamProj::Tcp(inner) => inner.poll_write_vectored(cx, bufs),
            AsyncStreamProj::Tls(inner) => inner.poll_write_vectored(cx, bufs),
            #[cfg(unix)]
            AsyncStreamProj::Unix(inner) => inner.poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Null => false,
            Self::Tcp(inner) => inner.is_write_vectored(),
            Self::Tls(inner) => inner.is_write_vectored(),
            #[cfg(unix)]
            Self::Unix(inner) => inner.is_write_vectored(),
        }
    }
}
