pub(super) mod server;

// use std::{
//     collections::{HashMap},
//     sync::{
//         atomic::{AtomicBool},
//         Arc,
//         Weak,
//     },
// };

// #[cfg(test)]
// use futures_util::stream::{FuturesUnordered, StreamExt};
// use tokio::sync::{RwLock};

// use self::server::Server;
// use super::{
//     TopologyDescription,
// };
// use crate::{
//     bson::oid::ObjectId,
//     options::{ClientOptions, ServerAddress},
//     runtime::HttpClient,
//     sdam::{
//         TopologyMessageManager,
//     },
// };

// /// A strong reference to the topology, which includes the current state as well as the client
// /// options and the message manager.
// #[derive(Clone, Debug)]
// pub(crate) struct Topology {
//     state: Arc<RwLock<TopologyState>>,
//     common: Common,
// }

// /// A weak reference to the topology, which includes the current state as well as the client
// /// options and the message manager.
// #[derive(Clone, Debug)]
// pub(crate) struct WeakTopology {
//     state: Weak<RwLock<TopologyState>>,
//     common: Common,
// }

// /// Encapsulates the common elements of Topology and WeakTopology, which includes the message
// /// manager and the client options.
// #[derive(Clone, Debug)]
// struct Common {
//     is_alive: Arc<AtomicBool>,
//     message_manager: TopologyMessageManager,
//     options: ClientOptions,
//     id: ObjectId,
// }

// /// The current state of the topology, which includes the topology description and the set of
// /// servers.
// #[derive(Debug)]
// struct TopologyState {
//     http_client: HttpClient,
//     description: TopologyDescription,
//     servers: HashMap<ServerAddress, Arc<Server>>,
//     options: ClientOptions,
//     id: ObjectId,
// }

// impl Topology {
//     /// Creates a new Topology given the `options`.
//     pub(crate) fn new(options: ClientOptions) -> Result<Self> {
//         let description = TopologyDescription::new(options.clone())?;
//         let is_load_balanced = description.topology_type() == TopologyType::LoadBalanced;

//         let id = ObjectId::new();
//         if let Some(ref handler) = options.sdam_event_handler {
//             let event = TopologyOpeningEvent { topology_id: id };
//             handler.handle_topology_opening_event(event);
//         }

//         let common = Common {
//             is_alive: Arc::new(AtomicBool::new(true)),
//             message_manager: TopologyMessageManager::new(),
//             options: options.clone(),
//             id,
//         };

//         let http_client = HttpClient::default();

//         let topology_state = TopologyState {
//             description,
//             servers: Default::default(),
//             http_client,
//             options: options.clone(),
//             id,
//         };

//         let state = Arc::new(RwLock::new(topology_state));
//         let topology = Topology { state, common };

//         // we can block in place here because we're the only ones with access to the lock, so it
//         // should be acquired immediately.
//         let mut topology_state = runtime::block_in_place(topology.state.write());
//         for address in &options.hosts {
//             topology_state.add_new_server(address.clone(), options.clone(),
// &topology.downgrade());         }

//         if let Some(ref handler) = options.sdam_event_handler {
//             let event = TopologyDescriptionChangedEvent {
//                 topology_id: id,
//                 previous_description: TopologyDescription::new_empty().into(),
//                 new_description: topology_state.description.clone().into(),
//             };
//             handler.handle_topology_description_changed_event(event);

//             for server_address in &options.hosts {
//                 let event = ServerOpeningEvent {
//                     topology_id: id,
//                     address: server_address.clone(),
//                 };
//                 handler.handle_server_opening_event(event);
//             }
//         }

//         if is_load_balanced {
//             for server_address in &options.hosts {
//                 // Load-balanced clients don't have a heartbeat monitor, so we synthesize
//                 // updating the server to `ServerType::LoadBalancer` with an RTT of 0 so it'll
//                 // be selected.
//                 let new_desc = ServerDescription {
//                     server_type: ServerType::LoadBalancer,
//                     average_round_trip_time: Some(Duration::from_nanos(0)),
//                     ..ServerDescription::new(server_address.clone(), None)
//                 };
//                 topology_state
//                     .update(new_desc, &options, topology.downgrade())
//                     .map_err(Error::internal)?;
//             }
//         }
//         #[cfg(test)]
//         let disable_monitoring_threads = options
//             .test_options
//             .map(|to| to.disable_monitoring_threads)
//             .unwrap_or(false);
//         #[cfg(not(test))]
//         let disable_monitoring_threads = false;
//         if !is_load_balanced && !disable_monitoring_threads {
//             // SrvPollingMonitor::start();
//         }

//         drop(topology_state);
//         Ok(topology)
//     }

//     pub(crate) fn close(&self) {
//         self.common.is_alive.store(false, Ordering::SeqCst);
//     }

//     /// Gets the addresses of the servers in the cluster.
//     #[cfg(test)]
//     pub(crate) async fn servers(&self) -> HashSet<ServerAddress> {
//         self.state.read().await.servers.keys().cloned().collect()
//     }

//     #[cfg(test)]
//     pub(crate) async fn description(&self) -> TopologyDescription {
//         self.state.read().await.description.clone()
//     }

//     /// Creates and returns a weak reference to the topology.
//     pub(super) fn downgrade(&self) -> WeakTopology {
//         WeakTopology {
//             state: Arc::downgrade(&self.state),
//             common: self.common.clone(),
//         }
//     }

//     /// Clones the underlying TopologyState. This will return a separate TopologyState than the
// one     /// contained by this `Topology`, but it will share references to the same Servers (and
// by     /// extension the connection pools).
//     /// Attempts to select a server with the given `criteria`, returning an error if the topology
// is     /// not compatible with the driver.
//     pub(crate) async fn attempt_to_select_server(
//         &self,
//         criteria: &SelectionCriteria,
//     ) -> Result<Option<SelectedServer>> {
//         let topology_state = self.state.read().await;

//         server_selection::attempt_to_select_server(
//             criteria,
//             &topology_state.description,
//             &topology_state.servers,
//         )
//     }

//     /// Creates a new server selection timeout error message given the `criteria`.
//     pub(crate) async fn server_selection_timeout_error_message(
//         &self,
//         criteria: &SelectionCriteria,
//     ) -> String {
//         self.state
//             .read()
//             .await
//             .description
//             .server_selection_timeout_error_message(criteria)
//     }

//     /// Signals the SDAM background threads that they should wake up and check the topology.
//     pub(crate) fn request_topology_check(&self) {
//         self.common.message_manager.request_topology_check();
//     }

//     /// Subscribe to notifications of requests to perform a server check.
//     pub(crate) fn subscribe_to_topology_check_requests(&self) -> TopologyMessageSubscriber {
//         self.common
//             .message_manager
//             .subscribe_to_topology_check_requests()
//     }

//     /// Subscribe to notifications that the topology has been updated.
//     pub(crate) fn subscribe_to_topology_changes(&self) -> TopologyMessageSubscriber {
//         self.common.message_manager.subscribe_to_topology_changes()
//     }

//     /// Wakes all tasks waiting for a topology change.
//     pub(crate) fn notify_topology_changed(&self) {
//         self.common.message_manager.notify_topology_changed();
//     }

//     // pub(crate) async fn handle_application_error(
//     //     &self,
//     //     error: Error,
//     //     handshake: HandshakePhase,
//     //     server: &Server,
//     // ) -> bool {
//     // let state_lock = self.state.write().await;
//     // match &handshake {
//     //     HandshakePhase::PreHello { generation } => {
//     //         match (generation, server.pool.generation()) {
//     //             (PoolGeneration::Normal(hgen), PoolGeneration::Normal(sgen)) => {
//     //                 if *hgen < sgen {
//     //                     return false;
//     //                 }
//     //             }
//     //             // Pre-hello handshake errors are ignored in load-balanced mode.
//     //             (PoolGeneration::LoadBalanced(_), PoolGeneration::LoadBalanced(_)) => {
//     //                 return false
//     //             }
//     //             _ => load_balanced_mode_mismatch!(false),
//     //         }
//     //     }
//     //     HandshakePhase::PostHello { generation }
//     //     | HandshakePhase::AfterCompletion { generation, .. } => {
//     //         if generation.is_stale(&server.pool.generation()) {
//     //             return false;
//     //         }
//     //     }
//     // }

//     // let is_load_balanced = state_lock.description.topology_type() ==
// TopologyType::LoadBalanced;     // if error.is_state_change_error() {
//     //     let updated = is_load_balanced
//     //         || self
//     //             .mark_server_as_unknown(error.to_string(), server, state_lock)
//     //             .await;

//     //     if updated && (error.is_shutting_down() || handshake.wire_version().unwrap_or(0) < 8)
// {     //         server.pool.clear(error, handshake.service_id()).await;
//     //     }
//     //     self.request_topology_check();

//     //     updated
//     // } else if error.is_non_timeout_network_error()
//     //     || (handshake.is_before_completion()
//     //         && (error.is_auth_error()
//     //             || error.is_network_timeout()
//     //             || error.is_command_error()))
//     // {
//     //     let updated = is_load_balanced
//     //         || self
//     //             .mark_server_as_unknown(error.to_string(), server, state_lock)
//     //             .await;
//     //     if updated {
//     //         server.pool.clear(error, handshake.service_id()).await;
//     //     }
//     //     updated
//     // } else {
//     //     false
//     // }
//     //     todo!()
//     // }

//     pub(crate) async fn handle_monitor_error(&self, error: Error, server: &Server) -> bool {
//         let state_lock = self.state.write().await;
//         let updated = self
//             .mark_server_as_unknown(error.to_string(), server, state_lock)
//             .await;
//         if updated {
//             // The heartbeat monitor is disabled in load-balanced mode, so this will never have a
//             // service id.
//             server.pool.clear(error, None).await;
//         }
//         updated
//     }

//     /// Marks a server in the cluster as unknown due to the given `error`.
//     /// Returns whether the topology changed as a result of the update.
//     async fn mark_server_as_unknown(
//         &self,
//         error: String,
//         server: &Server,
//         state_lock: RwLockWriteGuard<'_, TopologyState>,
//     ) -> bool {
//         let description = ServerDescription::new(server.address.clone(), Some(Err(error)));
//         self.update_and_notify(server, description, state_lock)
//             .await
//     }

//     /// Update the topology using the given server description.
//     ///
//     /// Because this method takes a lock guard as a parameter, it is mainly useful for
// sychronizing     /// updates to the topology with other state management.
//     ///
//     /// Returns a boolean indicating whether the topology changed as a result of the update.
//     async fn update_and_notify(
//         &self,
//         server: &Server,
//         server_description: ServerDescription,
//         mut state_lock: RwLockWriteGuard<'_, TopologyState>,
//     ) -> bool {
//         let server_type = server_description.server_type;
//         // TODO RUST-580: Theoretically, `TopologyDescription::update` can return an error.
// However,         // this can only happen if we try to access a field from the hello response when
// an error         // occurred during the check. In practice, this can't happen, because the SDAM
// algorithm         // doesn't check the fields of an Unknown server, and we only return Unknown
// server         // descriptions when errors occur. Once we implement logging, we can properly
// inform users         // of errors that occur here.
//         match state_lock.update(server_description, &self.common.options, self.downgrade()) {
//             Ok(true) => {
//                 if server_type.is_data_bearing()
//                     || (server_type != ServerType::Unknown
//                         && state_lock.description.topology_type() == TopologyType::Single)
//                 {
//                     server.pool.mark_as_ready().await;
//                 }
//                 true
//             }
//             _ => false,
//         }
//     }

//     /// Updates the topology using the given `ServerDescription`. Monitors for new servers will
//     /// be started as necessary.
//     ///
//     /// Returns true if the topology changed as a result of the update and false otherwise.
//     pub(crate) async fn update(
//         &self,
//         server: &Server,
//         server_description: ServerDescription,
//     ) -> bool {
//         self.update_and_notify(server, server_description, self.state.write().await)
//             .await
//     }

//     /// Updates the hosts included in this topology, starting and stopping monitors as necessary.
//     pub(crate) async fn update_hosts(
//         &self,
//         hosts: HashSet<ServerAddress>,
//         options: &ClientOptions,
//     ) -> bool {
//         self.state
//             .write()
//             .await
//             .update_hosts(&hosts, options, self.downgrade());
//         true
//     }

//     /// Update the topology's highest seen cluster time.
//     /// If the provided cluster time is not higher than the topology's currently highest seen
//     /// cluster time, this method has no effect.
//     pub(crate) async fn advance_cluster_time(&self, cluster_time: &ClusterTime) {
//         self.state
//             .write()
//             .await
//             .description
//             .advance_cluster_time(cluster_time);
//     }

//     /// Get the topology's currently highest seen cluster time.
//     pub(crate) async fn cluster_time(&self) -> Option<ClusterTime> {
//         self.state
//             .read()
//             .await
//             .description
//             .cluster_time()
//             .map(Clone::clone)
//     }

//     /// Gets the latest information on whether sessions are supported or not.
//     pub(crate) async fn session_support_status(&self) -> SessionSupportStatus {
//         self.state.read().await.description.session_support_status()
//     }

//     /// Gets the latest information on whether transactions are support or not.
//     pub(crate) async fn transaction_support_status(&self) -> TransactionSupportStatus {
//         self.state
//             .read()
//             .await
//             .description
//             .transaction_support_status()
//     }

//     pub(crate) async fn topology_type(&self) -> TopologyType {
//         self.state.read().await.description.topology_type()
//     }

//     pub(crate) async fn get_server_description(
//         &self,
//         address: &ServerAddress,
//     ) -> Option<ServerDescription> {
//         self.state
//             .read()
//             .await
//             .description
//             .get_server_description(address)
//             .cloned()
//     }

//     #[cfg(test)]
//     pub(crate) async fn get_servers(&self) -> HashMap<ServerAddress, Weak<Server>> {
//         self.state
//             .read()
//             .await
//             .servers
//             .iter()
//             .map(|(addr, server)| (addr.clone(), Arc::downgrade(server)))
//             .collect()
//     }

//     #[cfg(test)]
//     pub(crate) async fn sync_workers(&self) {
//         self.state.read().await.sync_workers().await;
//     }
// }

// impl TopologyState {
//     #[cfg(test)]
//     async fn sync_workers(&self) {
//         let rxen: FuturesUnordered<_> = self
//             .servers
//             .values()
//             .map(|v| v.pool.sync_worker())
//             .collect();
//         let _: Vec<_> = rxen.collect().await;
//     }
// }
