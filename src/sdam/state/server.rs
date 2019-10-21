use crate::{
    cmap::{options::ConnectionPoolOptions, Connection, ConnectionPool},
    error::Result,
    options::{ClientOptions, StreamAddress},
};

/// Contains the state for a given server in the topology.
#[derive(Debug)]
pub(crate) struct Server {
    /// The connection pool for the server.
    pool: ConnectionPool,
}

impl Server {
    /// Creates a new Server given the `address` and `options`.
    pub(crate) fn new(address: StreamAddress, options: &ClientOptions) -> Self {
        Self {
            pool: ConnectionPool::new(
                address,
                Some(ConnectionPoolOptions::from_client_options(options)),
            ),
        }
    }

    /// Checks out a connection from the server's pool.
    pub(crate) fn checkout_connection(&self) -> Result<Connection> {
        self.pool.check_out()
    }
}
