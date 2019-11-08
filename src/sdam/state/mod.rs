pub(super) mod server;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use time::PreciseTime;

use self::server::Server;
use super::{monitor::monitor_server, TopologyDescription};
use crate::{
    cmap::{Command, Connection},
    error::{ErrorKind, Result},
    options::{ClientOptions, StreamAddress},
    sdam::description::{server::ServerType, topology::SelectionCriteria},
};

const DEFAULT_SERVER_SELECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Contains the SDAM state for a Client.
#[derive(Debug)]
pub(crate) struct Topology {
    /// The SDAM and server selection state machine.
    pub(super) description: TopologyDescription,

    /// The state associated with each server in the cluster.
    servers: HashMap<StreamAddress, Arc<Server>>,

    server_selection_timeout: Option<Duration>,
}

impl Topology {
    /// Creates a new Topology given the `options`. Arc<RwLock<Topology> is returned rather than
    /// just Topology so that monitoring threads can hold a Weak reference to it.
    pub(crate) fn new(mut options: ClientOptions) -> Result<Arc<RwLock<Self>>> {
        let topology = Arc::new(RwLock::new(Topology {
            description: TopologyDescription::new(options.clone()),
            servers: Default::default(),
            server_selection_timeout: options.server_selection_timeout,
        }));

        let hosts: Vec<_> = options.hosts.drain(..).collect();
        let mut servers = HashMap::new();

        for address in hosts {
            let server = Arc::new(Server::new(
                Arc::downgrade(&topology),
                address.clone(),
                &options,
            ));
            servers.insert(address.clone(), server);
        }

        {
            let mut topology_lock = topology.write().unwrap();
            topology_lock.servers = servers;

            for server in topology_lock.servers.values() {
                let conn =
                    Connection::new(0, server.address.clone(), 0, options.tls_options.clone())?;

                monitor_server(conn, Arc::downgrade(server), options.heartbeat_freq);
            }
        }

        Ok(topology)
    }

    pub(crate) fn update_command_with_read_pref(
        &self,
        server_type: ServerType,
        command: &mut Command,
        criteria: Option<&SelectionCriteria>,
    ) {
        self.description
            .update_command_with_read_pref(server_type, command, criteria)
    }

    /// Selects a server description with the given criteria.
    pub(crate) fn select_server(&self, selector: &SelectionCriteria) -> Result<Arc<Server>> {
        let start_time = PreciseTime::now();
        let timeout = self
            .server_selection_timeout
            .unwrap_or(DEFAULT_SERVER_SELECTION_TIMEOUT);

        while start_time.to(PreciseTime::now()).to_std().unwrap() < timeout {
            // Return error if the wire version is invalid.
            if let Some(error_msg) = self.description.compatibility_error() {
                return Err(ErrorKind::ServerSelectionError {
                    message: error_msg.into(),
                }
                .into());
            }

            // Attempt to select a server. If none is found, request a topology update and restart
            // loop.
            let server_description = match self.description.select_server(&selector) {
                Some(description) => description,
                None => {
                    self.request_topology_check();
                    continue;
                }
            };

            // Attempt to find selected server. If it is not found, then it has been removed since
            // we started our most recent selection attempt, so we should retry.
            let server = match self.servers.get(&server_description.address) {
                Some(server) => server,
                None => {
                    self.request_topology_check();
                    continue;
                }
            };

            // Return the selected server.
            return Ok(server.clone());
        }

        Err(ErrorKind::ServerSelectionError {
            message: "timed out while trying to select server".into(),
        }
        .into())
    }

    fn request_topology_check(&self) {
        for server in self.servers.values() {
            server.request_topology_check();
        }
    }
}
