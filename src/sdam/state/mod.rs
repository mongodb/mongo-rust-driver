pub(super) mod server;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Weak},
    time::Duration,
};

use tokio::sync::RwLock;

use self::server::Server;
use super::TopologyDescription;
use crate::{
    cmap::{Command, Connection},
    error::{Error, ErrorKind, Result},
    options::{ClientOptions, SelectionCriteria, StreamAddress},
    sdam::{
        description::{
            server::{ServerDescription, ServerType},
            topology::{TopologyDescriptionDiff, TopologyType},
        },
        monitor::Monitor,
        srv_polling::SrvPollingMonitor,
        TopologyMessageManager,
    },
};

/// A strong reference to the topology, which includes the current state as well as the client
/// options and the message manager.
#[derive(Clone, Debug)]
pub(crate) struct Topology {
    state: Arc<RwLock<TopologyState>>,
    common: Common,
}

/// A weak reference to the topology, which includes the current state as well as the client
/// options and the message manager.
#[derive(Clone, Debug)]
pub(crate) struct WeakTopology {
    state: Weak<RwLock<TopologyState>>,
    common: Common,
}

/// Encapsulates the common elements of Topology and WeakTopology, which includes the message
/// manager and the client options.
#[derive(Clone, Debug)]
struct Common {
    message_manager: TopologyMessageManager,
    options: ClientOptions,
}

/// The current state of the topology, which includes the topology description and the set of
/// servers.
#[derive(Clone, Debug)]
pub(crate) struct TopologyState {
    description: TopologyDescription,
    servers: HashMap<StreamAddress, Arc<Server>>,
}

impl Topology {
    /// Creates a new Topology given the `options`.
    pub(crate) fn new(mut options: ClientOptions) -> Result<Self> {
        let description = TopologyDescription::new(options.clone())?;

        let hosts: Vec<_> = options.hosts.drain(..).collect();

        let common = Common {
            message_manager: TopologyMessageManager::new(),
            options: options.clone(),
        };

        let mut topology_state = TopologyState {
            description,
            servers: Default::default(),
        };

        for address in hosts.iter() {
            topology_state.add_new_server(address.clone(), options.clone());
        }

        let state = Arc::new(RwLock::new(topology_state.clone()));
        let topology = Topology { state, common };

        for address in hosts {
            topology_state.start_monitoring_server(address.clone(), topology.downgrade());
        }

        SrvPollingMonitor::start(topology.downgrade());

        Ok(topology)
    }

    /// Creates and returns a weak reference to the topology.
    fn downgrade(&self) -> WeakTopology {
        WeakTopology {
            state: Arc::downgrade(&self.state),
            common: self.common.clone(),
        }
    }

    pub(super) async fn clone_state(&self) -> TopologyState {
        self.state.read().await.clone()
    }

    /// Attempts to select a server with the given `criteria`, returning an error if the topology is
    /// not compatible with the driver.
    pub(crate) async fn attempt_to_select_server(
        &self,
        criteria: &SelectionCriteria,
    ) -> Result<Option<Arc<Server>>> {
        let topology_state = self.state.read().await;

        if let Some(message) = topology_state.description.compatibility_error() {
            return Err(ErrorKind::ServerSelectionError {
                message: message.to_string(),
            }
            .into());
        }

        Ok(topology_state
            .description
            .select_server(criteria)?
            .and_then(|server| topology_state.servers.get(&server.address).cloned()))
    }

    /// Creates a new server selection timeout error message given the `criteria`.
    pub(crate) async fn server_selection_timeout_error_message(
        &self,
        criteria: &SelectionCriteria,
    ) -> String {
        self.state
            .read()
            .await
            .description
            .server_selection_timeout_error_message(criteria)
    }

    /// Signals the SDAM background threads that they should wake up and check the topology.
    pub(crate) fn request_topology_check(&self) {
        self.common.message_manager.request_topology_check();
    }

    /// Waits until either the `timeout` has elapsed or a topology check has been requested.
    pub(crate) async fn wait_for_topology_check_request(&self, timeout: Duration) -> bool {
        self.common
            .message_manager
            .wait_for_topology_check_request(timeout)
            .await
    }

    /// Wakes all tasks waiting for a topology change.
    pub(crate) fn notify_topology_changed(&self) {
        self.common.message_manager.notify_topology_changed();
    }

    /// Waits until either the `timeout` has elapsed or the topology has been updated.
    pub(crate) async fn wait_for_topology_change(&self, timeout: Duration) -> bool {
        self.common
            .message_manager
            .wait_for_topology_change(timeout)
            .await
    }

    /// Handles an error that occurs before the handshake has completed during an operation.
    pub(crate) async fn handle_pre_handshake_error(&self, error: Error, address: StreamAddress) {
        if error.is_network_error() {
            self.mark_server_as_unknown(error, address).await;
        }
    }

    /// Handles an error that occurs after the handshake has completed during an operation.
    pub(crate) async fn handle_post_handshake_error(
        &self,
        error: Error,
        conn: Connection,
        server: Arc<Server>,
    ) {
        // If we encounter certain errors, we must update the topology as per the
        // SDAM spec.
        if error.is_non_timeout_network_error() {
            self.mark_server_as_unknown(error, server.address.clone())
                .await;
            server.clear_connection_pool().await;
        } else if error.is_recovering() || error.is_not_master() {
            self.mark_server_as_unknown(error.clone(), server.address.clone())
                .await;

            self.common.message_manager.request_topology_check();

            let wire_version = conn
                .stream_description()
                .map(|sd| sd.max_wire_version)
                .ok()
                .flatten()
                .unwrap_or(0);

            // in 4.2+, we only clear connection pool if we've received a
            // "node is shutting down" error. Otherwise, we always clear the pool.
            if wire_version < 8 || error.is_shutting_down() {
                server.clear_connection_pool().await;
            }
        }
    }

    /// Marks a server in the cluster as unknown due to the given `error`.
    async fn mark_server_as_unknown(&self, error: Error, address: StreamAddress) {
        let description = ServerDescription::new(address, Some(Err(error)));
        self.update(description).await;
    }

    /// Updates the provided topology in a minimally contentious way by cloning first.
    ///
    /// Returns true if the topology changed as a result of the update and false otherwise.
    pub(crate) async fn update(&self, server_description: ServerDescription) -> bool {
        // Because we're calling clone on the lock guard, we're actually copying the TopologyState
        // itself, not just making a new reference to it. The `servers` field will contain
        // references to the same instances though, since each is wrapped in an `Arc`.
        let mut state_clone = self.state.read().await.clone();

        let old_description = state_clone.description.clone();

        // TODO RUST-232: Theoretically, `TopologyDescription::update` can return an error. However,
        // this can only happen if we try to access a field from the isMaster response when an error
        // occurred during the check. In practice, this can't happen, because the SDAM algorithm
        // doesn't check the fields of an Unknown server, and we only return Unknown server
        // descriptions when errors occur. Once we implement SDAM monitoring, we can
        // properly inform users of errors that occur here.
        if let Ok(diff) = state_clone.update(server_description, &self.common.options) {
            self.update_state(diff, state_clone).await
        } else {
            false
        }
    }

    pub(crate) async fn update_state(
        &self,
        diff: Option<TopologyDescriptionDiff>,
        new_state: TopologyState,
    ) -> bool {
        match diff {
            None => false,
            Some(diff) => {
                // Now that we have the proper state in the copy, acquire a lock on the proper
                // topology and move the info over.
                let mut state_lock = self.state.write().await;
                state_lock.description = new_state.description;
                state_lock.servers = new_state.servers;

                for new_address in diff.new_addresses {
                    state_lock.start_monitoring_server(new_address, self.downgrade());
                }

                self.common.message_manager.notify_topology_changed();

                true
            }
        }
    }

    /// Updates the given `command` as needed based on the `critiera`.
    pub(crate) async fn update_command_with_read_pref(
        &self,
        server_address: &StreamAddress,
        command: &mut Command,
        criteria: Option<&SelectionCriteria>,
    ) {
        self.state
            .read()
            .await
            .update_command_with_read_pref(server_address, command, criteria);
    }
}

impl WeakTopology {
    /// Attempts to convert the WeakTopology to a string reference.
    pub(crate) fn upgrade(&self) -> Option<Topology> {
        Some(Topology {
            state: self.state.upgrade()?,
            common: self.common.clone(),
        })
    }

    pub(crate) fn client_options(&self) -> &ClientOptions {
        &self.common.options
    }
}

impl TopologyState {
    pub(super) fn is_sharded(&self) -> bool {
        self.description.topology_type() == TopologyType::Sharded
    }

    pub(super) fn is_unknown(&self) -> bool {
        self.description.topology_type() == TopologyType::Unknown
    }

    /// Adds a new server to the cluster.
    ///
    /// A reference to the containing Topology is needed in order to start the monitoring task.
    fn add_new_server(&mut self, address: StreamAddress, options: ClientOptions) {
        if self.servers.contains_key(&address) {
            return;
        }

        let server = Arc::new(Server::new(address.clone(), &options));
        self.servers.insert(address, server);
    }

    /// Updates the given `command` as needed based on the `critiera`.
    pub(crate) fn update_command_with_read_pref(
        &self,
        server_address: &StreamAddress,
        command: &mut Command,
        criteria: Option<&SelectionCriteria>,
    ) {
        let server_type = self
            .description
            .get_server_description(server_address)
            .map(|desc| desc.server_type)
            .unwrap_or(ServerType::Unknown);

        self.description
            .update_command_with_read_pref(server_type, command, criteria)
    }

    /// Start/stop monitoring tasks and create/destroy connection pools based on the new and
    /// removed servers in the topology description.
    ///
    /// This must **ONLY** be called on a copy of a TopologyState, not one that is stored in a
    /// client. The `topology` parameter should contain a reference to the Topology that
    /// is actually stored in a client.
    pub(crate) fn update(
        &mut self,
        server: ServerDescription,
        options: &ClientOptions,
    ) -> Result<Option<TopologyDescriptionDiff>> {
        let old_description = self.description.clone();
        self.description.update(server)?;

        let hosts: HashSet<_> = self.description.server_addresses().cloned().collect();
        self.sync_hosts(&hosts, options);

        Ok(old_description.diff(&self.description))
    }

    pub(crate) fn update_hosts(
        &mut self,
        hosts: &HashSet<StreamAddress>,
        options: &ClientOptions,
    ) -> Option<TopologyDescriptionDiff> {
        let old_description = self.description.clone();
        self.description.sync_hosts(&hosts);

        self.sync_hosts(&hosts, options);

        old_description.diff(&self.description)
    }

    pub(crate) fn sync_hosts(&mut self, hosts: &HashSet<StreamAddress>, options: &ClientOptions) {
        for address in hosts.iter() {
            self.add_new_server(address.clone(), options.clone());
        }

        self.servers.retain(|host, _| hosts.contains(host));
    }

    /// Start a monitor for the server at the given address if it is part of the topology.
    fn start_monitoring_server(&self, address: StreamAddress, topology: WeakTopology) {
        if let Some(server) = self.servers.get(&address) {
            Monitor::start(address, Arc::downgrade(&server), topology);
        }
    }
}
