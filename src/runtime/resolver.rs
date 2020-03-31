use std::{future::Future, net::SocketAddr, time::Duration};

use async_trait::async_trait;
use trust_dns_proto::error::ProtoError;
use trust_dns_resolver::{
    lookup::{SrvLookup, TxtLookup},
    name_server::{GenericConnection, GenericConnectionProvider},
    IntoName,
};

use crate::{
    error::{Error, Result},
    runtime::{stream::AsyncTcpStream, AsyncRuntime},
    RUNTIME,
};

// Some shorter type names for the generic constraints of the underlying trust-dns
// `AsyncResolver`.
type AsyncConnectionProvider = GenericConnectionProvider<AsyncRuntimeProvider>;
type TrustDnsResolver =
    trust_dns_resolver::AsyncResolver<GenericConnection, AsyncConnectionProvider>;

/// An async runtime agnostic DNS resolver.
pub(crate) struct AsyncResolver {
    resolver: TrustDnsResolver,
}

impl AsyncResolver {
    pub(crate) async fn new() -> Result<Self> {
        let resolver = TrustDnsResolver::from_system_conf(crate::RUNTIME).await?;
        Ok(Self { resolver })
    }
}

impl AsyncResolver {
    pub async fn srv_lookup<N: IntoName>(&self, query: N) -> Result<SrvLookup> {
        let lookup = self.resolver.srv_lookup(query).await?;
        Ok(lookup)
    }

    pub async fn txt_lookup<N: IntoName>(&self, query: N) -> Result<TxtLookup> {
        let lookup = self.resolver.txt_lookup(query).await?;
        Ok(lookup)
    }
}

/// The provider type used by trustdns to operate using the proper async runtime.
#[derive(Copy, Clone)]
struct AsyncRuntimeProvider;

impl trust_dns_resolver::name_server::RuntimeProvider for AsyncRuntimeProvider {
    type Handle = AsyncRuntime;
    type Tcp = AsyncTcpStream;
    type Timer = AsyncRuntime;
    type Udp = AsyncUdpSocket;
}

// Below are implementations of the various trait requirements that trustdns imposes to use a
// custom runtime. We define them for both async-std and tokio.

#[async_trait]
impl trust_dns_proto::Time for AsyncRuntime {
    async fn delay_for(duration: Duration) {
        RUNTIME.delay_for(duration).await
    }

    async fn timeout<F: 'static + Future + Send>(
        timeout: Duration,
        future: F,
    ) -> std::io::Result<F::Output> {
        RUNTIME
            .timeout(timeout, future)
            .await
            .map_err(Error::into_io_error)
    }
}

impl trust_dns_resolver::name_server::Spawn for AsyncRuntime {
    fn spawn_bg<F>(&mut self, future: F)
    where
        F: Future<Output = std::result::Result<(), ProtoError>> + Send + 'static,
    {
        self.execute(future)
    }
}

#[async_trait]
impl trust_dns_proto::tcp::Connect for AsyncTcpStream {
    type Transport = Self;

    /// connect to tcp
    async fn connect(addr: SocketAddr) -> std::io::Result<Self::Transport> {
        AsyncTcpStream::connect_socket_addr(&addr, None)
            .await
            .map_err(Error::into_io_error)
    }
}

/// A runtime-agnostic UdpSocket necessary for trustdns' resolver.
/// This does not need to be used elsewhere in the driver.
enum AsyncUdpSocket {
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::net::UdpSocket),

    #[cfg(feature = "async-std-runtime")]
    AsyncStd(async_std::net::UdpSocket),
}

#[async_trait]
impl trust_dns_proto::udp::UdpSocket for AsyncUdpSocket {
    async fn bind(addr: &SocketAddr) -> std::io::Result<Self> {
        #[cfg(feature = "tokio-runtime")]
        use tokio::net::UdpSocket;

        #[cfg(feature = "async-std-runtime")]
        use async_std::net::UdpSocket;

        let socket = UdpSocket::bind(addr).await?;
        Ok(socket.into())
    }

    /// Receive data from the socket and returns the number of bytes read and the address from
    /// where the data came on success.
    async fn recv_from(&mut self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        match self {
            #[cfg(feature = "tokio-runtime")]
            AsyncUdpSocket::Tokio(ref mut socket) => socket.recv_from(buf).await,

            #[cfg(feature = "async-std-runtime")]
            AsyncUdpSocket::AsyncStd(ref mut socket) => socket.recv_from(buf).await,
        }
    }

    /// Send data to the given address.
    async fn send_to(&mut self, buf: &[u8], target: &SocketAddr) -> std::io::Result<usize> {
        match self {
            #[cfg(feature = "tokio-runtime")]
            AsyncUdpSocket::Tokio(ref mut socket) => socket.send_to(buf, target).await,

            #[cfg(feature = "async-std-runtime")]
            AsyncUdpSocket::AsyncStd(ref mut socket) => socket.send_to(buf, target).await,
        }
    }
}

#[cfg(feature = "tokio-runtime")]
impl From<tokio::net::UdpSocket> for AsyncUdpSocket {
    fn from(socket: tokio::net::UdpSocket) -> Self {
        Self::Tokio(socket)
    }
}

#[cfg(feature = "async-std-runtime")]
impl From<async_std::net::UdpSocket> for AsyncUdpSocket {
    fn from(socket: async_std::net::UdpSocket) -> Self {
        Self::AsyncStd(socket)
    }
}
