use std::sync::atomic::{AtomicU32, Ordering};

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
    pub(crate) pool: ConnectionPool,

    /// Number of operations currently using this server.
    operation_count: AtomicU32,
}

impl Server {
    #[cfg(test)]
    pub(crate) fn new_mocked(address: StreamAddress, operation_count: u32) -> Self {
        Self {
            address: address.clone(),
            pool: ConnectionPool::new_mocked(address),
            operation_count: AtomicU32::new(operation_count),
        }
    }

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
            operation_count: AtomicU32::new(0),
        }
    }

    /// Creates a new Server given the `address` and `options`.
    /// Checks out a connection from the server's pool.
    pub(crate) async fn checkout_connection(&self) -> Result<Connection> {
        self.pool.check_out().await
    }

    /// Clears the connection pool associated with the server.
    pub(crate) fn clear_connection_pool(&self) {
        self.pool.clear();
    }

    pub(crate) fn increment_operation_count(&self) {
        self.operation_count.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn decrement_operation_count(&self) {
        self.operation_count.fetch_sub(1, Ordering::SeqCst);
    }

    pub(crate) fn operation_count(&self) -> u32 {
        self.operation_count.load(Ordering::SeqCst)
    }

    /// Opens the connection pool associated with the server.
    pub(crate) async fn open_connection_pool(&self) {
        self.pool.ready().await;
    }
}
