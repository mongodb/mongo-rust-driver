pub(super) mod server;

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
        Weak,
    },
    time::Duration,
};

#[cfg(test)]
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{RwLock, RwLockWriteGuard};

use self::server::Server;
use super::{
    description::topology::{server_selection::SelectedServer, TransactionSupportStatus},
    message_manager::TopologyMessageSubscriber,
    ServerInfo,
    SessionSupportStatus,
    TopologyDescription,
};
use crate::{
    bson::oid::ObjectId,
    client::ClusterTime,
    cmap::{conn::ConnectionGeneration, Command, Connection, PoolGeneration},
    error::{load_balanced_mode_mismatch, Error, Result},
    event::sdam::{
        ServerClosedEvent,
        ServerDescriptionChangedEvent,
        ServerOpeningEvent,
        TopologyClosedEvent,
        TopologyDescriptionChangedEvent,
        TopologyOpeningEvent,
    },
    options::{ClientOptions, SelectionCriteria, ServerAddress},
    runtime,
    runtime::HttpClient,
    sdam::{
        description::{
            server::{ServerDescription, ServerType},
            topology::{server_selection, TopologyType},
        },
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
    is_alive: Arc<AtomicBool>,
    message_manager: TopologyMessageManager,
    options: ClientOptions,
    id: ObjectId,
}

/// The current state of the topology, which includes the topology description and the set of
/// servers.
#[derive(Debug)]
struct TopologyState {
    http_client: HttpClient,
    description: TopologyDescription,
    servers: HashMap<ServerAddress, Arc<Server>>,
    options: ClientOptions,
    id: ObjectId,
}

impl Topology {
    /// Creates a new Topology given the `options`.
    pub(crate) fn new(options: ClientOptions) -> Result<Self> {
        let description = TopologyDescription::new(options.clone())?;
        let is_load_balanced = description.topology_type() == TopologyType::LoadBalanced;

        let id = ObjectId::new();
        if let Some(ref handler) = options.sdam_event_handler {
            let event = TopologyOpeningEvent { topology_id: id };
            handler.handle_topology_opening_event(event);
        }

        let common = Common {
            is_alive: Arc::new(AtomicBool::new(true)),
            message_manager: TopologyMessageManager::new(),
            options: options.clone(),
            id,
        };

        let http_client = HttpClient::default();

        let topology_state = TopologyState {
            description,
            servers: Default::default(),
            http_client,
            options: options.clone(),
            id,
        };

        let state = Arc::new(RwLock::new(topology_state));
        let topology = Topology { state, common };

        // we can block in place here because we're the only ones with access to the lock, so it
        // should be acquired immediately.
        let mut topology_state = runtime::block_in_place(topology.state.write());
        for address in &options.hosts {
            topology_state.add_new_server(address.clone(), options.clone(), &topology.downgrade());
        }

        if let Some(ref handler) = options.sdam_event_handler {
            let event = TopologyDescriptionChangedEvent {
                topology_id: id,
                previous_description: TopologyDescription::new_empty().into(),
                new_description: topology_state.description.clone().into(),
            };
            handler.handle_topology_description_changed_event(event);

            for server_address in &options.hosts {
                let event = ServerOpeningEvent {
                    topology_id: id,
                    address: server_address.clone(),
                };
                handler.handle_server_opening_event(event);
            }
        }

        if is_load_balanced {
            for server_address in &options.hosts {
                // Load-balanced clients don't have a heartbeat monitor, so we synthesize
                // updating the server to `ServerType::LoadBalancer` with an RTT of 0 so it'll
                // be selected.
                let new_desc = ServerDescription {
                    server_type: ServerType::LoadBalancer,
                    average_round_trip_time: Some(Duration::from_nanos(0)),
                    ..ServerDescription::new(server_address.clone(), None)
                };
                topology_state
                    .update(new_desc, &options, topology.downgrade())
                    .map_err(Error::internal)?;
            }
        }
        #[cfg(test)]
        let disable_monitoring_threads = options
            .test_options
            .map(|to| to.disable_monitoring_threads)
            .unwrap_or(false);
        #[cfg(not(test))]
        let disable_monitoring_threads = false;
        if !is_load_balanced && !disable_monitoring_threads {
            SrvPollingMonitor::start(topology.downgrade());
        }

        drop(topology_state);
        Ok(topology)
    }

    pub(crate) fn close(&self) {
        self.common.is_alive.store(false, Ordering::SeqCst);
    }

    /// Gets the addresses of the servers in the cluster.
    #[cfg(test)]
    pub(crate) async fn servers(&self) -> HashSet<ServerAddress> {
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
        match &handshake {
            HandshakePhase::PreHello { generation } => {
                match (generation, server.pool.generation()) {
                    (PoolGeneration::Normal(hgen), PoolGeneration::Normal(sgen)) => {
                        if *hgen < sgen {
                            return false;
                        }
                    }
                    // Pre-hello handshake errors are ignored in load-balanced mode.
                    (PoolGeneration::LoadBalanced(_), PoolGeneration::LoadBalanced(_)) => {
                        return false
                    }
                    _ => load_balanced_mode_mismatch!(false),
                }
            }
            HandshakePhase::PostHello { generation }
            | HandshakePhase::AfterCompletion { generation, .. } => {
                if generation.is_stale(&server.pool.generation()) {
                    return false;
                }
            }
        }

        let is_load_balanced = state_lock.description.topology_type() == TopologyType::LoadBalanced;
        if error.is_state_change_error() {
            let updated = is_load_balanced
                || self
                    .mark_server_as_unknown(error.to_string(), server, state_lock)
                    .await;

            if updated && (error.is_shutting_down() || handshake.wire_version().unwrap_or(0) < 8) {
                server.pool.clear(error, handshake.service_id()).await;
            }
            self.request_topology_check();

            updated
        } else if error.is_non_timeout_network_error()
            || (handshake.is_before_completion()
                && (error.is_auth_error()
                    || error.is_network_timeout()
                    || error.is_command_error()))
        {
            let updated = is_load_balanced
                || self
                    .mark_server_as_unknown(error.to_string(), server, state_lock)
                    .await;
            if updated {
                server.pool.clear(error, handshake.service_id()).await;
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
            // The heartbeat monitor is disabled in load-balanced mode, so this will never have a
            // service id.
            server.pool.clear(error, None).await;
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
        let server_type = server_description.server_type;
        // TODO RUST-580: Theoretically, `TopologyDescription::update` can return an error. However,
        // this can only happen if we try to access a field from the hello response when an error
        // occurred during the check. In practice, this can't happen, because the SDAM algorithm
        // doesn't check the fields of an Unknown server, and we only return Unknown server
        // descriptions when errors occur. Once we implement logging, we can properly inform users
        // of errors that occur here.
        match state_lock.update(server_description, &self.common.options, self.downgrade()) {
            Ok(true) => {
                if server_type.is_data_bearing()
                    || (server_type != ServerType::Unknown
                        && state_lock.description.topology_type() == TopologyType::Single)
                {
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
        hosts: HashSet<ServerAddress>,
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
    pub(crate) async fn update_command_with_read_pref<T>(
        &self,
        server_address: &ServerAddress,
        command: &mut Command<T>,
        criteria: Option<&SelectionCriteria>,
    ) {
        self.state
            .read()
            .await
            .update_command_with_read_pref(server_address, command, criteria)
    }

    /// Gets the latest information on whether sessions are supported or not.
    pub(crate) async fn session_support_status(&self) -> SessionSupportStatus {
        self.state.read().await.description.session_support_status()
    }

    /// Gets the latest information on whether transactions are support or not.
    pub(crate) async fn transaction_support_status(&self) -> TransactionSupportStatus {
        self.state
            .read()
            .await
            .description
            .transaction_support_status()
    }

    pub(crate) async fn topology_type(&self) -> TopologyType {
        self.state.read().await.description.topology_type()
    }

    pub(crate) async fn get_server_description(
        &self,
        address: &ServerAddress,
    ) -> Option<ServerDescription> {
        self.state
            .read()
            .await
            .description
            .get_server_description(address)
            .cloned()
    }

    #[cfg(test)]
    pub(crate) async fn get_servers(&self) -> HashMap<ServerAddress, Weak<Server>> {
        self.state
            .read()
            .await
            .servers
            .iter()
            .map(|(addr, server)| (addr.clone(), Arc::downgrade(server)))
            .collect()
    }

    #[cfg(test)]
    pub(crate) async fn sync_workers(&self) {
        self.state.read().await.sync_workers().await;
    }
}

impl Drop for TopologyState {
    fn drop(&mut self) {
        if let Some(ref handler) = self.options.sdam_event_handler {
            if matches!(self.description.topology_type, TopologyType::LoadBalanced) {
                for host in self.servers.keys() {
                    let event = ServerClosedEvent {
                        address: host.clone(),
                        topology_id: self.id,
                    };
                    handler.handle_server_closed_event(event);
                }
            }
            let event = TopologyClosedEvent {
                topology_id: self.id,
            };
            handler.handle_topology_closed_event(event);
        }
    }
}

impl WeakTopology {
    /// Attempts to convert the WeakTopology to a strong reference.
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
        address: ServerAddress,
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
        if options
            .test_options
            .map(|to| to.disable_monitoring_threads)
            .unwrap_or(false)
        {
            return;
        }

        monitor.start();
    }

    /// Updates the given `command` as needed based on the `criteria`.
    pub(crate) fn update_command_with_read_pref<T>(
        &self,
        server_address: &ServerAddress,
        command: &mut Command<T>,
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
    ) -> std::result::Result<bool, String> {
        let old_description = self.description.clone();
        self.description.update(server)?;

        let hosts: HashSet<_> = self.description.server_addresses().cloned().collect();
        self.sync_hosts(&hosts, options, &topology);

        let diff = old_description.diff(&self.description);
        let topology_changed = diff.is_some();

        if let Some(ref handler) = options.sdam_event_handler {
            if let Some(diff) = diff {
                for (address, (previous_description, new_description)) in diff.changed_servers {
                    let event = ServerDescriptionChangedEvent {
                        address: address.clone(),
                        topology_id: topology.common.id,
                        previous_description: ServerInfo::new_owned(previous_description.clone()),
                        new_description: ServerInfo::new_owned(new_description.clone()),
                    };
                    handler.handle_server_description_changed_event(event);
                }

                for address in diff.removed_addresses {
                    let event = ServerClosedEvent {
                        address: address.clone(),
                        topology_id: topology.common.id,
                    };
                    handler.handle_server_closed_event(event);
                }

                for address in diff.added_addresses {
                    let event = ServerOpeningEvent {
                        address: address.clone(),
                        topology_id: topology.common.id,
                    };
                    handler.handle_server_opening_event(event);
                }

                let event = TopologyDescriptionChangedEvent {
                    topology_id: topology.common.id,
                    previous_description: old_description.clone().into(),
                    new_description: self.description.clone().into(),
                };
                handler.handle_topology_description_changed_event(event);
            }
        }

        Ok(topology_changed)
    }

    /// Start/stop monitoring tasks and create/destroy connection pools based on the new and
    /// removed servers in the topology description.
    fn update_hosts(
        &mut self,
        hosts: &HashSet<ServerAddress>,
        options: &ClientOptions,
        topology: WeakTopology,
    ) {
        self.description.sync_hosts(hosts);
        self.sync_hosts(hosts, options, &topology);
    }

    fn sync_hosts(
        &mut self,
        hosts: &HashSet<ServerAddress>,
        options: &ClientOptions,
        topology: &WeakTopology,
    ) {
        for address in hosts.iter() {
            self.add_new_server(address.clone(), options.clone(), topology);
        }

        self.servers.retain(|host, _| hosts.contains(host));
    }

    #[cfg(test)]
    async fn sync_workers(&self) {
        let rxen: FuturesUnordered<_> = self
            .servers
            .values()
            .map(|v| v.pool.sync_worker())
            .collect();
        let _: Vec<_> = rxen.collect().await;
    }
}

/// Enum describing a point in time during an operation's execution relative to when the MongoDB
/// handshake for the conection being used in that operation.
///
/// This is used to determine the error handling semantics for certain error types.
#[derive(Debug, Clone)]
pub(crate) enum HandshakePhase {
    /// Describes a point that occurred before the initial hello completed (e.g. when opening the
    /// socket).
    PreHello { generation: PoolGeneration },

    /// Describes a point in time after the initial hello has completed, but before the entire
    /// handshake (e.g. including authentication) completes.
    PostHello { generation: ConnectionGeneration },

    /// Describes a point in time after the handshake completed (e.g. when the command was sent to
    /// the server).
    AfterCompletion {
        generation: ConnectionGeneration,
        max_wire_version: i32,
    },
}

impl HandshakePhase {
    pub(crate) fn after_completion(handshaked_connection: &Connection) -> Self {
        Self::AfterCompletion {
            generation: handshaked_connection.generation.clone(),
            // given that this is a handshaked connection, the stream description should
            // always be available, so 0 should never actually be returned here.
            max_wire_version: handshaked_connection
                .stream_description()
                .ok()
                .and_then(|sd| sd.max_wire_version)
                .unwrap_or(0),
        }
    }

    /// The `serviceId` reported by the server.  If the initial hello has not completed, returns
    /// `None`.
    pub(crate) fn service_id(&self) -> Option<ObjectId> {
        match self {
            HandshakePhase::PreHello { .. } => None,
            HandshakePhase::PostHello { generation, .. } => generation.service_id(),
            HandshakePhase::AfterCompletion { generation, .. } => generation.service_id(),
        }
    }

    /// Whether this phase is before the handshake completed or not.
    fn is_before_completion(&self) -> bool {
        !matches!(self, HandshakePhase::AfterCompletion { .. })
    }

    /// The wire version of the server as reported by the handshake. If the handshake did not
    /// complete, this returns `None`.
    fn wire_version(&self) -> Option<i32> {
        match self {
            HandshakePhase::AfterCompletion {
                max_wire_version, ..
            } => Some(*max_wire_version),
            _ => None,
        }
    }
}
