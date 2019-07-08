pub(crate) mod description;
mod server;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock, Weak},
    time::Duration,
};

use derivative::Derivative;

use self::server::{monitor::start_monitor, Server};
use crate::{
    options::{ClientOptions, Host},
    read_preference::ReadPreference,
};

pub(crate) use self::{
    description::{TopologyDescription, TopologyType},
    server::{OpTime, ServerDescription, ServerType},
};

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Topology {
    #[derivative(Debug = "ignore")]
    tls_config: Option<Arc<rustls::ClientConfig>>,
    max_pool_size: Option<u32>,
    connect_timeout: Option<Duration>,
    description: TopologyDescription,
    servers: HashMap<String, Arc<RwLock<Server>>>,
}

impl Topology {
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::new_ret_no_self))]
    pub(crate) fn new(
        options: ClientOptions,
        tls_config: Option<Arc<rustls::ClientConfig>>,
    ) -> Arc<RwLock<Self>> {
        let servers = options
            .hosts
            .iter()
            .map(|host| {
                let server = Arc::new(RwLock::new(Server::new(
                    host.clone(),
                    options.max_pool_size,
                    tls_config.clone(),
                    options.connect_timeout,
                    options.credential.clone(),
                )));
                (host.display(), server)
            })
            .collect();

        let heartbeat_freq = options.heartbeat_freq;

        let topology = Arc::new(RwLock::new(Self {
            tls_config,
            max_pool_size: options.max_pool_size,
            connect_timeout: options.connect_timeout,
            description: TopologyDescription::new(options),
            servers,
        }));

        {
            let topology_lock = topology.read().unwrap();
            for server in topology_lock.servers.values() {
                start_monitor(
                    Arc::downgrade(server),
                    Arc::downgrade(&topology),
                    heartbeat_freq,
                );
            }
        }

        topology
    }

    pub(crate) fn topology_type(&self) -> TopologyType {
        self.description.topology_type
    }

    pub(crate) fn compatibility_error(&self) -> Option<&str> {
        self.description
            .compatibility_error
            .as_ref()
            .map(|s| &s[..])
    }

    pub(crate) fn get_server_type(&self, address: &str) -> Option<ServerType> {
        self.description.get_server_type(address)
    }

    pub fn server_descriptions(&self) -> HashMap<String, ServerDescription> {
        self.description.server_descriptions()
    }

    pub(crate) fn slave_ok(&self, address: &str, read_preference: Option<&ReadPreference>) -> bool {
        self.description.slave_ok(address, read_preference)
    }

    pub(crate) fn get_server(&self, address: &str) -> Option<Arc<RwLock<Server>>> {
        self.servers.get(address).map(Clone::clone)
    }

    pub(crate) fn get_max_wire_version(&self, address: &str) -> Option<i32> {
        self.description
            .server_descriptions()
            .get(address)
            .map(|s| s.max_wire_version)
    }

    pub(crate) fn update_description<'a>(
        &'a mut self,
        description: TopologyDescription,
    ) -> impl Iterator<Item = Weak<RwLock<Server>>> + 'a {
        let existing_addresses: Vec<_> = self.servers.keys().cloned().collect();

        for address in &existing_addresses {
            if !description.contains_server(address) {
                self.servers.remove(address);
            }
        }

        self.description = description;

        let new_servers: Vec<_> = self
            .description
            .server_addresses()
            .filter(|address| !self.servers.contains_key(*address))
            .map(|address| {
                (
                    address.clone(),
                    Arc::new(RwLock::new(Server::new(
                        Host::parse(address).unwrap(),
                        self.max_pool_size,
                        self.tls_config.clone(),
                        self.connect_timeout,
                        None,
                    ))),
                )
            })
            .collect();

        for (address, server) in &new_servers {
            self.servers.insert(address.clone(), server.clone());
        }

        new_servers.into_iter().map(|pair| Arc::downgrade(&pair.1))
    }
}
