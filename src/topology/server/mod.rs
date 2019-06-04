mod description;
pub mod monitor;

use std::sync::Arc;

use derivative::Derivative;

pub use self::description::{OpTime, ServerDescription, ServerType};

use crate::{
    error::Result,
    options::Host,
    pool::{is_master, Connection, Pool},
};

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Server {
    #[derivative(Debug = "ignore")]
    tls_config: Option<Arc<rustls::ClientConfig>>,
    host: Host,
    pool: Pool,
    monitor_pool: Pool,
}

impl Server {
    pub(crate) fn new(host: Host, tls_config: Option<Arc<rustls::ClientConfig>>) -> Self {
        Self {
            pool: Pool::new(host.clone(), None, tls_config.clone()).unwrap(),
            monitor_pool: Pool::new(host.clone(), Some(1), tls_config.clone()).unwrap(),
            host,
            tls_config,
        }
    }

    pub(crate) fn address(&self) -> String {
        self.host.display()
    }

    pub(crate) fn acquire_stream(&self) -> Result<Connection> {
        let mut conn = self.pool.get()?;

        // Connection handshake
        is_master(&mut conn, true)?;

        Ok(conn)
    }

    pub(crate) fn check(&mut self, server_type: ServerType) -> ServerDescription {
        let mut conn = self.monitor_pool.get().unwrap();
        let mut description =
            ServerDescription::new(&self.host.display(), Some(is_master(&mut conn, false)));

        if description.error.is_some() {
            self.reset_pools();

            if server_type != ServerType::Unknown {
                conn = self.monitor_pool.get().unwrap();
                description =
                    ServerDescription::new(&self.host.display(), Some(is_master(&mut conn, false)));
            }
        }

        description
    }

    pub(crate) fn reset_pools(&mut self) {
        self.pool = Pool::new(self.host.clone(), None, self.tls_config.clone()).unwrap();
        self.monitor_pool = Pool::new(self.host.clone(), Some(1), self.tls_config.clone()).unwrap();
    }
}
