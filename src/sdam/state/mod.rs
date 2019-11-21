pub(super) mod server;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Condvar, Mutex, RwLock},
    time::Duration,
};

use derivative::Derivative;

use self::server::Server;
use super::TopologyDescription;
use crate::{
    cmap::{Command, Connection},
    error::Result,
    options::{ClientOptions, StreamAddress},
    sdam::{
        description::server::{ServerDescription, ServerType},
        monitor::monitor_server,
    },
    selection_criteria::SelectionCriteria,
};

#[derive(Clone)]
pub(crate) struct TopologyUpdateCondvar {
    condvar: Arc<Condvar>,
}

impl TopologyUpdateCondvar {
    pub(crate) fn new() -> Self {
        Self {
            condvar: Arc::new(Condvar::new()),
        }
    }

    pub(crate) fn wait_timeout(&self, duration: Duration) {
        let _ = self
            .condvar
            .wait_timeout(Mutex::new(()).lock().unwrap(), duration)
            .unwrap();
    }

    fn notify(&self) {
        self.condvar.notify_all()
    }
}

/// Contains the SDAM state for a Client.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(crate) struct Topology {
    /// The SDAM and server selection state machine.
    pub(crate) description: TopologyDescription,

    /// The state associated with each server in the cluster.
    pub(crate) servers: HashMap<StreamAddress, Arc<Server>>,

    #[derivative(Debug = "ignore")]
    condvar: TopologyUpdateCondvar,

    options: ClientOptions,
}

impl Topology {
    /// Creates a new Topology given the `options`. Arc<RwLock<Topology> is returned rather than
    /// just Topology so that monitoring threads can hold a Weak reference to it.
    pub(crate) fn new(
        condvar: TopologyUpdateCondvar,
        mut options: ClientOptions,
    ) -> Result<Arc<RwLock<Self>>> {
        let description = TopologyDescription::new(options.clone())?;
        let hosts: Vec<_> = options.hosts.drain(..).collect();

        let topology = Arc::new(RwLock::new(Topology {
            description,
            servers: Default::default(),
            condvar,
            options,
        }));

        {
            let mut topology_lock = topology.write().unwrap();

            for address in hosts {
                topology_lock.add_new_server(address, &topology)?;
            }
        }

        Ok(topology)
    }

    pub(crate) fn notify(&self) {
        self.condvar.notify()
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

    pub(crate) fn request_topology_check(&self) {
        for server in self.servers.values() {
            server.request_topology_check();
        }
    }

    fn add_new_server(
        &mut self,
        address: StreamAddress,
        wrapped_topology: &Arc<RwLock<Topology>>,
    ) -> Result<()> {
        if self.servers.contains_key(&address) {
            return Ok(());
        }

        let options = self.options.clone();

        let server = Arc::new(Server::new(
            Arc::downgrade(wrapped_topology),
            address.clone(),
            &options,
        ));
        self.servers.insert(address.clone(), server.clone());

        let conn = Connection::new(
            0,
            server.address.clone(),
            0,
            options.tls_options.clone(),
            options.cmap_event_handler.clone(),
        )?;

        monitor_server(conn, Arc::downgrade(&server), options.heartbeat_freq);

        Ok(())
    }

    /// Start/stop monitoring threads and create/destroy connection pools based on the new and
    /// removed servers in the topology description.
    ///
    /// This must **ONLY** be called on a copy of a topology, not one that is stored in a client.
    /// The `wrapped_topology` parameter should contain a reference to the Topology that is actually
    /// stored in a client.
    pub(crate) fn update_state(
        &mut self,
        server: ServerDescription,
        wrapped_topology: &Arc<RwLock<Topology>>,
    ) -> Result<()> {
        self.description.update(server)?;

        let addresses: HashSet<_> = self.description.server_addresses().cloned().collect();

        for address in addresses.iter() {
            self.add_new_server(address.clone(), wrapped_topology)?;
        }

        self.servers
            .retain(|address, _| addresses.contains(address));

        Ok(())
    }
}
