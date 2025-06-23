pub(crate) mod handshake;

use std::time::{Duration, Instant};

use self::handshake::{Handshaker, HandshakerOptions};
use super::{
    conn::{
        pooled::PooledConnection,
        ConnectionGeneration,
        LoadBalancedGeneration,
        PendingConnection,
    },
    Connection,
    PoolGeneration,
};
use crate::{
    client::{
        auth::Credential,
        options::{ClientOptions, ServerAddress, TlsOptions},
    },
    error::{Error as MongoError, ErrorKind, Result},
    hello::HelloReply,
    runtime::{self, stream::DEFAULT_CONNECT_TIMEOUT, AsyncStream, TlsConfig},
    sdam::HandshakePhase,
};

/// Contains the logic to establish a connection, including handshaking, authenticating, and
/// potentially more.
#[derive(Clone)]
pub(crate) struct ConnectionEstablisher {
    /// Contains the logic for handshaking a connection.
    handshaker: Handshaker,

    /// Cached configuration needed to create TLS connections, if needed.
    tls_config: Option<TlsConfig>,

    connect_timeout: Duration,

    #[cfg(test)]
    test_patch_reply: Option<fn(&mut Result<HelloReply>)>,
}

pub(crate) struct EstablisherOptions {
    handshake_options: HandshakerOptions,
    tls_options: Option<TlsOptions>,
    connect_timeout: Option<Duration>,
    #[cfg(test)]
    pub(crate) test_patch_reply: Option<fn(&mut Result<HelloReply>)>,
}

impl EstablisherOptions {
    pub(crate) fn from_client_options(opts: &ClientOptions) -> Self {
        Self {
            handshake_options: HandshakerOptions {
                app_name: opts.app_name.clone(),
                #[cfg(any(
                    feature = "zstd-compression",
                    feature = "zlib-compression",
                    feature = "snappy-compression"
                ))]
                compressors: opts.compressors.clone(),
                driver_info: opts.driver_info.clone(),
                server_api: opts.server_api.clone(),
                load_balanced: opts.load_balanced.unwrap_or(false),
            },
            tls_options: opts.tls_options(),
            connect_timeout: opts.connect_timeout,
            #[cfg(test)]
            test_patch_reply: None,
        }
    }
}

impl ConnectionEstablisher {
    /// Creates a new ConnectionEstablisher from the given options.
    pub(crate) fn new(options: EstablisherOptions) -> Result<Self> {
        let handshaker = Handshaker::new(options.handshake_options)?;

        let tls_config = if let Some(tls_options) = options.tls_options {
            Some(TlsConfig::new(tls_options)?)
        } else {
            None
        };

        let connect_timeout = match options.connect_timeout {
            Some(d) if d.is_zero() => Duration::MAX,
            Some(d) => d,
            None => DEFAULT_CONNECT_TIMEOUT,
        };

        Ok(Self {
            handshaker,
            tls_config,
            connect_timeout,
            #[cfg(test)]
            test_patch_reply: options.test_patch_reply,
        })
    }

    async fn make_stream(&self, address: ServerAddress) -> Result<AsyncStream> {
        runtime::timeout(
            self.connect_timeout,
            AsyncStream::connect(address, self.tls_config.as_ref()),
        )
        .await?
    }

    /// Establishes a connection.
    pub(crate) async fn establish_connection(
        &self,
        mut pending_connection: PendingConnection,
        credential: Option<&Credential>,
    ) -> std::result::Result<PooledConnection, EstablishError> {
        let pool_gen = pending_connection.generation.clone();
        let address = pending_connection.address.clone();
        let cancellation_receiver = pending_connection.cancellation_receiver.take();

        let stream = self
            .make_stream(address)
            .await
            .map_err(|e| EstablishError::pre_hello(e, pool_gen.clone()))?;

        let mut connection = PooledConnection::new(pending_connection, stream);
        #[allow(unused_mut)]
        let mut handshake_result = self
            .handshaker
            .handshake(&mut connection, credential, cancellation_receiver)
            .await;
        #[cfg(test)]
        if let Some(patch) = self.test_patch_reply {
            patch(&mut handshake_result);
        }
        let handshake_result = handshake_result;

        // If the handshake response had a `serviceId` field, this is a connection to a load
        // balancer and must derive its generation from the service_generations map.
        match (&pool_gen, connection.service_id()) {
            (PoolGeneration::Normal(_), _) => {}
            (PoolGeneration::LoadBalanced(gen_map), Some(service_id)) => {
                connection.generation = LoadBalancedGeneration {
                    generation: *gen_map.get(&service_id).unwrap_or(&0),
                    service_id,
                }
                .into();
            }
            (PoolGeneration::LoadBalanced(_), None) => {
                // If the handshake succeeded and there isn't a service id, return a special error.
                // If the handshake failed, just return the error from that instead.
                if handshake_result.is_ok() {
                    return Err(EstablishError::post_hello(
                        ErrorKind::IncompatibleServer {
                            message: "Driver attempted to initialize in load balancing mode, but \
                                      the server does not support this mode."
                                .to_string(),
                        }
                        .into(),
                        connection.generation,
                    ));
                }
            }
        }

        handshake_result.map_err(|e| {
            if connection.stream_description().is_err() {
                EstablishError::pre_hello(e, pool_gen)
            } else {
                EstablishError::post_hello(e, connection.generation)
            }
        })?;

        Ok(connection)
    }

    /// Establishes a monitoring connection.
    pub(crate) async fn establish_monitoring_connection(
        &self,
        address: ServerAddress,
        id: u32,
    ) -> Result<(Connection, HelloReply)> {
        let stream = self.make_stream(address.clone()).await?;
        let mut connection = Connection::new(address, stream, id, Instant::now());

        let hello_reply = self
            .handshaker
            .handshake(&mut connection, None, None)
            .await?;

        Ok((connection, hello_reply))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EstablishError {
    pub(crate) cause: MongoError,
    pub(crate) handshake_phase: HandshakePhase,
}

impl EstablishError {
    fn pre_hello(cause: MongoError, generation: PoolGeneration) -> Self {
        Self {
            cause,
            handshake_phase: HandshakePhase::PreHello { generation },
        }
    }
    fn post_hello(cause: MongoError, generation: ConnectionGeneration) -> Self {
        Self {
            cause,
            handshake_phase: HandshakePhase::PostHello { generation },
        }
    }
}
