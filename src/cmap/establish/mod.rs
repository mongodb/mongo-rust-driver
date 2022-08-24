pub(super) mod handshake;
#[cfg(test)]
mod test;

use self::handshake::{Handshaker, HandshakerOptions};
use super::{
    conn::{ConnectionGeneration, LoadBalancedGeneration, PendingConnection},
    options::ConnectionPoolOptions,
    Connection,
    PoolGeneration,
};
use crate::{
    client::{
        auth::Credential,
        options::{ClientOptions, ServerAddress, ServerApi, TlsOptions},
    },
    error::{Error as MongoError, ErrorKind, Result},
    hello::HelloReply,
    runtime::{stream::AsyncTcpStream, AsyncStream, AsyncTlsStream, HttpClient, TlsConfig},
    sdam::HandshakePhase,
};

/// Contains the logic to establish a connection, including handshaking, authenticating, and
/// potentially more.
#[derive(Clone)]
pub(crate) struct ConnectionEstablisher {
    /// Contains the logic for handshaking a connection.
    handshaker: Handshaker,
    // http_client: HttpClient,
    // credential: Option<Credential>,
    // server_api: Option<ServerApi>,
    // handshaker_options: Option<HandshakerOptions>,
    tls_config: Option<TlsConfig>,
}

pub(crate) struct EstablisherOptions {
    handshake_options: HandshakerOptions,
    tls_options: Option<TlsOptions>,
}

impl EstablisherOptions {
    pub(crate) fn from_client_options(opts: &ClientOptions) -> Self {
        Self {
            handshake_options: HandshakerOptions {
                app_name: opts.app_name.clone(),
                compressors: opts.compressors.clone(),
                driver_info: opts.driver_info.clone(),
                server_api: opts.server_api.clone(),
                load_balanced: opts.load_balanced.unwrap_or(false),
            },
            tls_options: opts.tls_options(),
        }
    }
}

impl ConnectionEstablisher {
    /// Creates a new ConnectionEstablisher from the given options.
    pub(crate) fn new(http_client: HttpClient, options: EstablisherOptions) -> Result<Self> {
        let handshaker = Handshaker::new(http_client, options.handshake_options);

        let tls_config = if let Some(tls_options) = options.tls_options {
            Some(TlsConfig::new(tls_options)?)
        } else {
            None
        };

        Ok(Self {
            handshaker,
            tls_config,
        })
    }

    async fn make_stream(&self, address: ServerAddress) -> Result<AsyncStream> {
        let inner = AsyncTcpStream::connect(&address, None).await?;

        // If there are TLS options, wrap the inner stream with rustls.
        match self.tls_config {
            Some(ref cfg) => {
                let host = address.host();
                Ok(AsyncStream::Tls(
                    AsyncTlsStream::connect(host, inner, cfg).await?,
                ))
            }
            None => Ok(AsyncStream::Tcp(inner)),
        }
    }

    /// Establishes a connection.
    pub(super) async fn establish_connection(
        &self,
        pending_connection: PendingConnection,
        credential: Option<&Credential>,
    ) -> std::result::Result<Connection, EstablishError> {
        let pool_gen = pending_connection.generation.clone();
        let address = pending_connection.address.clone();

        let stream = self
            .make_stream(address)
            .await
            .map_err(|e| EstablishError::pre_hello(e, pool_gen.clone()))?;

        let mut connection = Connection::new_pooled(pending_connection, stream);

        let hello_reply = self
            .handshaker
            .handshake(&mut connection, credential)
            .await
            .map_err(|e| {
                if connection.stream_description.is_none() {
                    EstablishError::pre_hello(e, pool_gen.clone())
                } else {
                    EstablishError::post_hello(e, connection.generation)
                }
            })?;
        let service_id = hello_reply.command_response.service_id;

        // If the handshake response had a `serviceId` field, this is a connection to a load
        // balancer and must derive its generation from the service_generations map.
        match (pool_gen, service_id) {
            (PoolGeneration::Normal(_), _) => {}
            (PoolGeneration::LoadBalanced(gen_map), Some(service_id)) => {
                connection.generation = LoadBalancedGeneration {
                    generation: *gen_map.get(&service_id).unwrap_or(&0),
                    service_id,
                }
                .into();
            }
            _ => {
                return Err(EstablishError::post_hello(
                    ErrorKind::Internal {
                        message: "load-balanced mode mismatch".to_string(),
                    }
                    .into(),
                    connection.generation.clone(),
                ));
            }
        }

        Ok(connection)
    }

    /// Establishes a monitoring connection.
    pub(crate) async fn establish_monitoring_connection(
        &self,
        address: ServerAddress,
    ) -> Result<(Connection, HelloReply)> {
        let stream = self.make_stream(address.clone()).await?;
        let mut connection = Connection::new_monitoring(address, stream);

        let hello_reply = self.handshaker.handshake(&mut connection, None).await?;

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
