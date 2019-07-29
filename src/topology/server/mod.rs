mod description;
pub mod monitor;

use std::sync::Arc;

use derivative::Derivative;

pub use self::description::{OpTime, ServerDescription, ServerType};

use crate::{
    client::auth::Credential,
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
    pub(crate) fn new(
        host: Host,
        max_pool_size: Option<u32>,
        tls_config: Option<Arc<rustls::ClientConfig>>,
        credential: Option<Credential>,
    ) -> Self {
        Self {
            pool: Pool::new(host.clone(), max_pool_size, tls_config.clone(), credential).unwrap(),
            monitor_pool: Pool::new(host.clone(), Some(1), tls_config.clone(), None).unwrap(),
            host,
            tls_config,
        }
    }

    pub(crate) fn address(&self) -> String {
        self.host.display()
    }

    pub(crate) fn acquire_stream(&self) -> Result<Connection> {
        let conn = self.pool.get()?;
        Ok(conn)
    }

    pub(crate) fn check(&mut self, server_type: ServerType) -> ServerDescription {
        let mut conn = self.monitor_pool.get().unwrap();
        let mut description = ServerDescription::new(
            &self.host.display(),
            Some(is_master(None, &mut conn, false)),
        );

        if description.error.is_some() {
            self.reset_pools();

            if server_type != ServerType::Unknown {
                conn = self.monitor_pool.get().unwrap();
                description = ServerDescription::new(
                    &self.host.display(),
                    Some(is_master(None, &mut conn, false)),
                );
            }
        }

        description
    }

    pub(crate) fn reset_pools(&mut self) {
        self.pool = Pool::new(self.host.clone(), None, self.tls_config.clone(), None).unwrap();
        self.monitor_pool =
            Pool::new(self.host.clone(), Some(1), self.tls_config.clone(), None).unwrap();
    }
}
