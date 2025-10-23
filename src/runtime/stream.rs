use std::{
    net::SocketAddr,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::{io::AsyncWrite, net::TcpStream};

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
pub(crate) enum AsyncStream {
    Null,

    /// A basic TCP connection to the server.
    Tcp(TcpStream),

    /// A TLS connection over TCP.
    Tls(TlsStream),

    /// A Unix domain socket connection.
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}

impl AsyncStream {
    pub(crate) async fn connect(
        address: ServerAddress,
        tls_cfg: Option<&TlsConfig>,
    ) -> Result<Self> {
        match &address {
            ServerAddress::Tcp { host, .. } => {
                let resolved: Vec<_> = runtime::resolve_address(&address).await?.collect();
                if resolved.is_empty() {
                    return Err(ErrorKind::DnsResolve {
                        message: format!("No DNS results for domain {address}"),
                    }
                    .into());
                }
                let inner = tcp_connect(resolved).await?;

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

async fn tcp_try_connect(address: &SocketAddr) -> Result<TcpStream> {
    let stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;

    #[cfg(not(target_os = "wasi"))]
    {
        let sock_ref = socket2::SockRef::from(&stream);
        let conf = socket2::TcpKeepalive::new().with_time(KEEPALIVE_TIME);
        sock_ref.set_tcp_keepalive(&conf)?;
    }

    Ok(stream)
}

pub(crate) async fn tcp_connect(resolved: Vec<SocketAddr>) -> Result<TcpStream> {
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
            Err(e) => Err(Error::internal(format!("TCP connect task failure: {e}"))),
        }
    }

    static CONNECTION_ATTEMPT_DELAY: Duration = Duration::from_millis(250);

    // Race connections
    let mut attempts = tokio::task::JoinSet::new();
    let mut connect_error = None;
    'spawn: for a in socket_addrs {
        attempts.spawn(async move { tcp_try_connect(&a).await });
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
