#[cfg(test)]
mod test;

pub(crate) mod options;

use std::{
    collections::VecDeque,
    sync::{atomic::AtomicU32, Arc, RwLock},
};

use self::options::ConnectionPoolOptions;
use crate::error::Result;

#[derive(Debug)]
pub(crate) struct Connection {
    pub(crate) id: u32,
    pub(crate) hostname: String,
    pub(crate) generation: u32,
}

/// User-facing information about a connection to the database.
#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    /// A driver-generated identifier that uniquely identifies the connection.
    pub id: u32,

    /// The hostname of the address of the server that the connection is connected to.
    pub hostname: String,

    /// The port of the address of the server that the connection is connected to.
    pub port: Option<u16>,
}

#[derive(Debug)]
pub(crate) struct WaitQueue {
    queue: Arc<RwLock<VecDeque<()>>>,
}

#[derive(Debug)]
pub(crate) struct ConnectionPool {
    pub(crate) wait_queue: WaitQueue,
    pub(crate) generation: AtomicU32,
    pub(crate) total_connection_count: AtomicU32,
    pub(crate) available_connection_count: AtomicU32,
}

impl ConnectionPool {
    pub(crate) fn new(options: ConnectionPoolOptions) -> Self {
        unimplemented!()
    }

    pub(crate) fn check_out(&mut self) -> Result<Connection> {
        unimplemented!()
    }

    pub(crate) fn check_in(&mut self, conn: Connection) {
        unimplemented!()
    }

    pub(crate) fn clear(&mut self) -> Result<()> {
        unimplemented!()
    }
}
