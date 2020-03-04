use super::WeakTopology;
use crate::{
    cmap::{options::ConnectionPoolOptions, Connection, ConnectionPool},
    error::Result,
    options::{ClientOptions, StreamAddress},
    RUNTIME,
    sdam::Topology,
};

/// Contains the state for a given server in the topology.
#[derive(Debug)]
pub(crate) struct Server {
    pub(crate) address: StreamAddress,

    /// The topology that contains the server. Holding a weak reference allows monitoring threads
    /// to update the topology without keeping it alive after the Client has been dropped.
    topology: WeakTopology,

    /// The connection pool for the server.
    pool: ConnectionPool,
}

impl Server {
    pub(crate) fn new(
        topology: WeakTopology,
        address: StreamAddress,
        options: &ClientOptions,
    ) -> Self {
        Self {
            topology,
            pool: ConnectionPool::new(
                address.clone(),
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

    /// Attempts to upgrade the weak reference to the topology to a strong reference and return it.
    pub(crate) fn topology(&self) -> Option<Topology> {
        self.topology.upgrade()
    }
}
