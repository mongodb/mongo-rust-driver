use std::{
    sync::{Arc, Condvar, Mutex, RwLock, Weak},
    time::Duration,
};

use super::Topology;
use crate::{
    cmap::{options::ConnectionPoolOptions, Connection, ConnectionPool},
    error::Result,
    options::{ClientOptions, StreamAddress},
};

/// Contains the state for a given server in the topology.
#[derive(Debug)]
pub(crate) struct Server {
    pub(crate) address: StreamAddress,

    /// The topology that contains the server. Holding a weak reference allows monitoring threads
    /// to update the topology without keeping it alive after the Client has been dropped.
    topology: Weak<RwLock<Topology>>,

    /// The connection pool for the server.
    pool: ConnectionPool,

    condvar: Condvar,

    condvar_mutex: Mutex<()>,
}

impl Server {
    pub(crate) fn new(
        topology: Weak<RwLock<Topology>>,
        address: StreamAddress,
        options: &ClientOptions,
    ) -> Self {
        Self {
            topology,
            pool: ConnectionPool::new(
                address.clone(),
                Some(ConnectionPoolOptions::from_client_options(options)),
            ),
            condvar: Default::default(),
            condvar_mutex: Default::default(),
            address,
        }
    }

    /// Creates a new Server given the `address` and `options`.
    /// Checks out a connection from the server's pool.
    pub(crate) fn checkout_connection(&self) -> Result<Connection> {
        self.pool.check_out()
    }

    /// Clears the connection pool associated with the server.
    pub(crate) fn clear_connection_pool(&self) {
        self.pool.clear();
    }

    /// Attempts to upgrade the weak reference to the topology to a strong reference and return it.
    pub(crate) fn topology(&self) -> Option<Arc<RwLock<Topology>>> {
        self.topology.upgrade()
    }

    /// Waits until either the server is requested to do a topology check or until `duration` has
    /// elapsed. Returns `true` if `duration` has elapsed and `false` otherwise.
    pub(crate) fn wait_timeout(&self, duration: Duration) -> bool {
        self.condvar
            .wait_timeout(self.condvar_mutex.lock().unwrap(), duration)
            .unwrap()
            .1
            .timed_out()
    }

    pub(crate) fn request_topology_check(&self) {
        self.condvar.notify_all()
    }
}
