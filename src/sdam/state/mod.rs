pub(super) mod server;

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
        Weak,
    },
};

use tokio::sync::{RwLock, RwLockWriteGuard};

use self::server::Server;
use super::{
    description::topology::server_selection::SelectedServer,
    message_manager::TopologyMessageSubscriber,
    SessionSupportStatus,
    TopologyDescription,
};
use crate::{
    client::ClusterTime,
    cmap::{Command, Connection},
    error::{Error, Result},
    options::{ClientOptions, SelectionCriteria, StreamAddress},
    runtime::HttpClient,
    sdam::{
        description::{
            server::{ServerDescription, ServerType},
            topology::{server_selection, TopologyDescriptionDiff, TopologyType},
        },
        srv_polling::SrvPollingMonitor,
        TopologyMessageManager,
    },
    RUNTIME,
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
    is_alive: Arc<AtomicBool>,
    message_manager: TopologyMessageManager,
    options: ClientOptions,
}

/// The current state of the topology, which includes the topology description and the set of
/// servers.
#[derive(Debug)]
struct TopologyState {
    http_client: HttpClient,
    description: TopologyDescription,
    servers: HashMap<StreamAddress, Arc<Server>>,
    #[cfg(test)]
    mocked: bool,
}

impl Topology {
    /// Creates a new TopologyDescription with the set of servers initialized to the addresses
    /// specified in `hosts` and each other field set to its default value. No monitoring threads
    /// will be started for the servers in the topology that's returned.
    #[cfg(test)]
    pub(super) fn new_mocked(options: ClientOptions) -> Self {
        let description = TopologyDescription::new(options.clone()).unwrap();

        let common = Common {
            is_alive: Arc::new(AtomicBool::new(true)),
            message_manager: TopologyMessageManager::new(),
            options: options.clone(),
        };

        let http_client = HttpClient::default();

        let state = TopologyState {
            description,
            servers: Default::default(),
            http_client: http_client.clone(),
            mocked: true,
        };

        let topology = Self {
            state: Arc::new(RwLock::new(state)),
            common,
        };

        // we can block in place here because we're the only ones with access to the lock, so it
        // should be acquired immediately.
        let mut topology_state = RUNTIME.block_in_place(topology.state.write());

        for address in options.hosts {
            topology_state.servers.insert(
                address.clone(),
                Server::create(
                    address,
                    &ClientOptions::default(),
                    topology.downgrade(),
                    http_client.clone(),
                )
                .0,
            );
        }

        drop(topology_state);
        topology
    }

    /// Creates a new Topology given the `options`.
    pub(crate) fn new(mut options: ClientOptions) -> Result<Self> {
        let description = TopologyDescription::new(options.clone())?;

        let hosts: Vec<_> = options.hosts.drain(..).collect();

        let common = Common {
            is_alive: Arc::new(AtomicBool::new(true)),
            message_manager: TopologyMessageManager::new(),
            options: options.clone(),
        };

        let http_client = HttpClient::default();

        #[cfg(test)]
        let topology_state = TopologyState {
            description,
            servers: Default::default(),
            http_client,
            mocked: false,
        };

        #[cfg(not(test))]
        let topology_state = TopologyState {
            description,
            servers: Default::default(),
            http_client,
        };

        let state = Arc::new(RwLock::new(topology_state));
        let topology = Topology { state, common };

        // we can block in place here because we're the only ones with access to the lock, so it
        // should be acquired immediately.
        let mut topology_state = RUNTIME.block_in_place(topology.state.write());
        for address in hosts {
            topology_state.add_new_server(address, options.clone(), &topology.downgrade());
        }

        SrvPollingMonitor::start(topology.downgrade());

        drop(topology_state);
        Ok(topology)
    }

    pub(crate) fn mark_closed(&self) {
        self.common.is_alive.store(false, Ordering::SeqCst);
    }

    /// Gets the addresses of the servers in the cluster.
    #[cfg(test)]
    pub(crate) async fn servers(&self) -> HashSet<StreamAddress> {
        self.state.read().await.servers.keys().cloned().collect()
    }

    #[cfg(test)]
    pub(crate) async fn description(&self) -> TopologyDescription {
        self.state.read().await.description.clone()
    }

    /// Creates and returns a weak reference to the topology.
    pub(super) fn downgrade(&self) -> WeakTopology {
        WeakTopology {
            state: Arc::downgrade(&self.state),
            common: self.common.clone(),
        }
    }

    /// Clones the underlying TopologyState. This will return a separate TopologyState than the one
    /// contained by this `Topology`, but it will share references to the same Servers (and by
    /// extension the connection pools).
    /// Attempts to select a server with the given `criteria`, returning an error if the topology is
    /// not compatible with the driver.
    pub(crate) async fn attempt_to_select_server(
        &self,
        criteria: &SelectionCriteria,
    ) -> Result<Option<SelectedServer>> {
        let topology_state = self.state.read().await;

        server_selection::attempt_to_select_server(
            criteria,
            &topology_state.description,
            &topology_state.servers,
        )
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

    /// Subscribe to notifications of requests to perform a server check.
    pub(crate) fn subscribe_to_topology_check_requests(&self) -> TopologyMessageSubscriber {
        self.common
            .message_manager
            .subscribe_to_topology_check_requests()
    }

    /// Subscribe to notifications that the topology has been updated.
    pub(crate) fn subscribe_to_topology_changes(&self) -> TopologyMessageSubscriber {
        self.common.message_manager.subscribe_to_topology_changes()
    }

    /// Wakes all tasks waiting for a topology change.
    pub(crate) fn notify_topology_changed(&self) {
        self.common.message_manager.notify_topology_changed();
    }

    pub(crate) async fn handle_application_error(
        &self,
        error: Error,
        handshake: HandshakePhase,
        server: &Server,
    ) -> bool {
        let state_lock = self.state.write().await;
        if handshake.generation() < server.pool.generation() {
            return false;
        }

        if error.is_state_change_error() {
            let updated = self
                .mark_server_as_unknown(error.to_string(), server, state_lock)
                .await;

            if updated && (error.is_shutting_down() || handshake.wire_version().unwrap_or(0) < 8) {
                server.pool.clear().await;
            }
            self.request_topology_check();

            updated
        } else if error.is_non_timeout_network_error()
            || (handshake.is_before_completion()
                && (error.is_auth_error()
                    || error.is_network_timeout()
                    || error.is_command_error()))
        {
            let updated = self
                .mark_server_as_unknown(error.to_string(), server, state_lock)
                .await;
            if updated {
                server.pool.clear().await;
            }
            updated
        } else {
            false
        }
    }

    pub(crate) async fn handle_monitor_error(&self, error: Error, server: &Server) -> bool {
        let state_lock = self.state.write().await;
        let updated = self
            .mark_server_as_unknown(error.to_string(), server, state_lock)
            .await;
        if updated {
            server.pool.clear().await;
        }
        updated
    }

    /// Marks a server in the cluster as unknown due to the given `error`.
    /// Returns whether the topology changed as a result of the update.
    async fn mark_server_as_unknown(
        &self,
        error: String,
        server: &Server,
        state_lock: RwLockWriteGuard<'_, TopologyState>,
    ) -> bool {
        let description = ServerDescription::new(server.address.clone(), Some(Err(error)));
        self.update_and_notify(server, description, state_lock)
            .await
    }

    /// Update the topology using the given server description.
    ///
    /// Because this method takes a lock guard as a parameter, it is mainly useful for sychronizing
    /// updates to the topology with other state management.
    ///
    /// Returns a boolean indicating whether the topology changed as a result of the update.
    async fn update_and_notify(
        &self,
        server: &Server,
        server_description: ServerDescription,
        mut state_lock: RwLockWriteGuard<'_, TopologyState>,
    ) -> bool {
        let is_available = server_description.is_available();
        // TODO RUST-232: Theoretically, `TopologyDescription::update` can return an error. However,
        // this can only happen if we try to access a field from the isMaster response when an error
        // occurred during the check. In practice, this can't happen, because the SDAM algorithm
        // doesn't check the fields of an Unknown server, and we only return Unknown server
        // descriptions when errors occur. Once we implement SDAM monitoring, we can
        // properly inform users of errors that occur here.
        match state_lock.update(server_description, &self.common.options, self.downgrade()) {
            Ok(Some(_)) => {
                if is_available {
                    server.pool.mark_as_ready().await;
                }
                true
            }
            _ => false,
        }
    }

    /// Updates the topology using the given `ServerDescription`. Monitors for new servers will
    /// be started as necessary.
    ///
    /// Returns true if the topology changed as a result of the update and false otherwise.
    pub(crate) async fn update(
        &self,
        server: &Server,
        server_description: ServerDescription,
    ) -> bool {
        self.update_and_notify(server, server_description, self.state.write().await)
            .await
    }

    /// Updates the hosts included in this topology, starting and stopping monitors as necessary.
    pub(crate) async fn update_hosts(
        &self,
        hosts: HashSet<StreamAddress>,
        options: &ClientOptions,
    ) -> bool {
        self.state
            .write()
            .await
            .update_hosts(&hosts, options, self.downgrade());
        true
    }

    /// Update the topology's highest seen cluster time.
    /// If the provided cluster time is not higher than the topology's currently highest seen
    /// cluster time, this method has no effect.
    pub(crate) async fn advance_cluster_time(&self, cluster_time: &ClusterTime) {
        self.state
            .write()
            .await
            .description
            .advance_cluster_time(cluster_time);
    }

    /// Get the topology's currently highest seen cluster time.
    pub(crate) async fn cluster_time(&self) -> Option<ClusterTime> {
        self.state
            .read()
            .await
            .description
            .cluster_time()
            .map(Clone::clone)
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

    /// Gets the latest information on whether sessions are supported or not.
    pub(crate) async fn session_support_status(&self) -> SessionSupportStatus {
        self.state.read().await.description.session_support_status()
    }

    pub(super) async fn is_sharded(&self) -> bool {
        self.state.read().await.description.topology_type() == TopologyType::Sharded
    }

    pub(super) async fn is_unknown(&self) -> bool {
        self.state.read().await.description.topology_type() == TopologyType::Unknown
    }

    pub(crate) async fn get_server_description(
        &self,
        address: &StreamAddress,
    ) -> Option<ServerDescription> {
        self.state
            .read()
            .await
            .description
            .get_server_description(address)
            .cloned()
    }

    #[cfg(test)]
    pub(crate) async fn get_servers(&self) -> HashMap<StreamAddress, Weak<Server>> {
        self.state
            .read()
            .await
            .servers
            .iter()
            .map(|(addr, server)| (addr.clone(), Arc::downgrade(server)))
            .collect()
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

    pub(crate) fn is_alive(&self) -> bool {
        self.common.is_alive.load(Ordering::SeqCst)
    }

    pub(crate) fn client_options(&self) -> &ClientOptions {
        &self.common.options
    }
}

impl TopologyState {
    /// Adds a new server to the cluster.
    ///
    /// A reference to the containing Topology is needed in order to start the monitoring task.
    fn add_new_server(
        &mut self,
        address: StreamAddress,
        options: ClientOptions,
        topology: &WeakTopology,
    ) {
        if self.servers.contains_key(&address) {
            return;
        }

        let (server, monitor) = Server::create(
            address.clone(),
            &options,
            topology.clone(),
            self.http_client.clone(),
        );
        self.servers.insert(address, server);

        #[cfg(test)]
        if !self.mocked {
            monitor.start()
        }

        #[cfg(not(test))]
        monitor.start();
    }

    /// Updates the given `command` as needed based on the `criteria`.
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

    /// Update the topology description based on the provided server description. Also add new
    /// servers and remove missing ones as needed.
    fn update(
        &mut self,
        server: ServerDescription,
        options: &ClientOptions,
        topology: WeakTopology,
    ) -> std::result::Result<Option<TopologyDescriptionDiff>, String> {
        let old_description = self.description.clone();
        self.description.update(server)?;

        let hosts: HashSet<_> = self.description.server_addresses().cloned().collect();
        self.sync_hosts(&hosts, options, &topology);

        let diff = old_description.diff(&self.description);
        Ok(diff)
    }

    /// Start/stop monitoring tasks and create/destroy connection pools based on the new and
    /// removed servers in the topology description.
    fn update_hosts(
        &mut self,
        hosts: &HashSet<StreamAddress>,
        options: &ClientOptions,
        topology: WeakTopology,
    ) -> Option<TopologyDescriptionDiff> {
        let old_description = self.description.clone();
        self.description.sync_hosts(&hosts);

        self.sync_hosts(&hosts, options, &topology);

        old_description.diff(&self.description)
    }

    fn sync_hosts(
        &mut self,
        hosts: &HashSet<StreamAddress>,
        options: &ClientOptions,
        topology: &WeakTopology,
    ) {
        for address in hosts.iter() {
            self.add_new_server(address.clone(), options.clone(), topology);
        }

        self.servers.retain(|host, _| hosts.contains(host));
    }
}

/// Enum describing a point in time during an operation's execution relative to when the MongoDB
/// handshake for the conection being used in that operation.
///
/// This is used to determine the error handling semantics for certain error types.
pub(crate) enum HandshakePhase {
    /// Describes an point that occurred before the handshake completed (e.g. when opening the
    /// socket or while performing authentication)
    BeforeCompletion { generation: u32 },

    /// Describes a point in time after the handshake completed (e.g. when the command was sent to
    /// the server).
    AfterCompletion {
        generation: u32,
        max_wire_version: i32,
    },
}

impl HandshakePhase {
    pub(crate) fn after_completion(handshaked_connection: &Connection) -> Self {
        Self::AfterCompletion {
            generation: handshaked_connection.generation,
            // given that this is a handshaked connection, the stream description should
            // always be available, so 0 should never actually be returned here.
            max_wire_version: handshaked_connection
                .stream_description()
                .ok()
                .and_then(|sd| sd.max_wire_version)
                .unwrap_or(0),
        }
    }

    /// The generation of the connection that was used in the handshake.
    fn generation(&self) -> u32 {
        match self {
            HandshakePhase::BeforeCompletion { generation } => *generation,
            HandshakePhase::AfterCompletion { generation, .. } => *generation,
        }
    }

    /// Whether this phase is before the handshake completed or not.
    fn is_before_completion(&self) -> bool {
        matches!(self, HandshakePhase::BeforeCompletion { .. })
    }

    /// The wire version of the server as reported by the handshake. If the handshake did not
    /// complete, this returns `None`.
    fn wire_version(&self) -> Option<i32> {
        match self {
            HandshakePhase::AfterCompletion {
                max_wire_version, ..
            } => Some(*max_wire_version),
            HandshakePhase::BeforeCompletion { .. } => None,
        }
    }
}
