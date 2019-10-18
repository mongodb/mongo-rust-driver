pub(super) mod server;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use self::server::Server;
use super::TopologyDescription;
use crate::{
    error::{ErrorKind, Result},
    options::{ClientOptions, StreamAddress},
    sdam::description::topology::SelectionCriteria,
};

/// Contains the SDAM state for a Client.
#[derive(Debug)]
pub(crate) struct Topology {
    /// The SDAM and server selection state machine.
    pub(super) description: TopologyDescription,

    /// The state associated with each server in the cluster.
    servers: HashMap<StreamAddress, Arc<Server>>,
}

impl Topology {
    /// Creates a new Topology given the `options`. Arc<RwLock<Topology> is returned rather than
    /// just Topology so that monitoring threads can hold a Weak reference to it.
    pub(crate) fn new(mut options: ClientOptions) -> Result<Arc<RwLock<Self>>> {
        let topology = Arc::new(RwLock::new(Topology {
            description: TopologyDescription::new(options.clone()),
            servers: Default::default(),
        }));

        let hosts: Vec<_> = options.hosts.drain(..).collect();
        let mut servers = HashMap::new();

        for address in hosts {
            let server = Arc::new(Server::new(address.clone(), &options));
            servers.insert(address.clone(), server);
        }

        topology.write().unwrap().servers = servers;

        // TODO RUST-205: Start monitoring threads.

        Ok(topology)
    }

    /// Selects a server description with the given criteria.
    pub(crate) fn select_server(&self, selector: &SelectionCriteria) -> Result<Arc<Server>> {
        // TODO RUST-205: If no server is found, request an update, loop, and check for timeout.

        let server_description = self.description.select_server(selector).ok_or_else(|| {
            // TODO RUST-205: Remove this error and instead restart loop if this occurs.
            ErrorKind::ServerSelectionError {
                message: "No server found matching criteria".into(),
            }
        })?;

        let server = self
            .servers
            .get(&server_description.address)
            .ok_or_else(|| {
                // TODO RUST-205: Remove this error and instead restart loop if this occurs.
                ErrorKind::ServerSelectionError {
                    message: "Selected server with address {}, but topology changed".into(),
                }
            })?;

        Ok(server.clone())
    }
}
