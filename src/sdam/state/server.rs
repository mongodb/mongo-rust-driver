use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use crate::{
    cmap::{options::ConnectionPoolOptions, ConnectionPool},
    options::{ClientOptions, ServerAddress},
    runtime::HttpClient,
    sdam::TopologyUpdater,
};

/// Contains the state for a given server in the topology.
#[derive(Debug)]
pub(crate) struct Server {
    pub(crate) address: ServerAddress,

    /// The connection pool for the server.
    pub(crate) pool: ConnectionPool,

    /// Number of operations currently using this server.
    operation_count: AtomicU32,
}

impl Server {
    #[cfg(test)]
    pub(crate) fn new_mocked(address: ServerAddress, operation_count: u32) -> Self {
        Self {
            address: address.clone(),
            pool: ConnectionPool::new_mocked(address),
            operation_count: AtomicU32::new(operation_count),
        }
    }

    /// Create a new reference counted `Server`, including its connection pool.
    pub(crate) fn new(
        address: ServerAddress,
        options: ClientOptions,
        http_client: HttpClient,
        topology_updater: TopologyUpdater,
    ) -> Arc<Server> {
        Arc::new(Self {
            pool: ConnectionPool::new(
                address.clone(),
                http_client,
                topology_updater,
                Some(ConnectionPoolOptions::from_client_options(&options)),
            ),
            address,
            operation_count: AtomicU32::new(0),
        })
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
}
