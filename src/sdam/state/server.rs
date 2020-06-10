use crate::{
    cmap::{options::ConnectionPoolOptions, Connection, ConnectionPool},
    error::Result,
    options::{ClientOptions, StreamAddress},
    runtime::HttpClient,
};

/// Contains the state for a given server in the topology.
#[derive(Debug)]
pub(crate) struct Server {
    pub(crate) address: StreamAddress,

    /// The connection pool for the server.
    pool: ConnectionPool,
}

impl Server {
    pub(crate) fn new(
        address: StreamAddress,
        options: &ClientOptions,
        http_client: HttpClient,
    ) -> Self {
        Self {
            pool: ConnectionPool::new(
                address.clone(),
                http_client,
                Some(ConnectionPoolOptions::from_client_options(options)),
            ),
            address,
        }
    }

    /// Creates a new Server given the `address` and `options`.
    /// Checks out a connection from the server's pool.
    pub(crate) async fn checkout_connection(&self) -> Result<Connection> {
        self.pool.check_out().await
    }

    /// Clears the connection pool associated with the server.
    pub(crate) async fn clear_connection_pool(&self) {
        self.pool.clear().await;
    }
}
