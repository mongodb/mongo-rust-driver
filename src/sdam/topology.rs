use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use bson::oid::ObjectId;
use tokio::sync::{
    broadcast,
    mpsc::{UnboundedReceiver, UnboundedSender},
    watch::{self, Ref},
};

use crate::{
    client::options::{ClientOptions, ServerAddress},
    cmap::{conn::ConnectionGeneration, Command, Connection, PoolGeneration},
    error::{load_balanced_mode_mismatch, Error, Result},
    event::sdam::{
        ServerClosedEvent,
        ServerDescriptionChangedEvent,
        ServerOpeningEvent,
        TopologyDescriptionChangedEvent,
        TopologyOpeningEvent,
    },
    runtime::{self, AcknowledgedMessage, HttpClient},
    selection_criteria::SelectionCriteria,
    ClusterTime,
    ServerInfo,
    ServerType,
    TopologyType,
};

use super::{
    description::topology::TopologyDescriptionDiff,
    Monitor,
    Server,
    ServerDescription,
    SessionSupportStatus,
    Topology,
    TopologyDescription,
    TransactionSupportStatus,
};

#[derive(Debug)]
pub(crate) struct NewTopology {
    watcher: TopologyWatcher,
    updater: TopologyUpdater,
    update_requester: UpdateRequester,
}

impl NewTopology {
    pub(crate) fn new(options: ClientOptions) -> Result<NewTopology> {
        let http_client = HttpClient::default();
        let description = TopologyDescription::new(options.clone())?;
        let is_load_balanced = options.load_balanced == Some(true);
        let (update_requester, update_request_receiver) = UpdateRequester::channel();

        let (updater, update_receiver) = TopologyUpdater::channel();

        let servers = description
            .server_addresses()
            .map(|address| {
                (
                    address.clone(),
                    Server::new(
                        address.clone(),
                        options.clone(),
                        http_client.clone(),
                        updater.clone(),
                    ),
                )
            })
            .collect();

        let state = TopologyState {
            description,
            servers,
        };

        let addresses = state.servers.keys().cloned().collect::<Vec<_>>();

        let (watcher, broadcaster) = TopologyWatcher::channel(state);

        #[cfg(test)]
        let disable_monitoring_threads = options
            .test_options
            .as_ref()
            .map(|to| to.disable_monitoring_threads)
            .unwrap_or(false);
        #[cfg(not(test))]
        let disable_monitoring_threads = false;
        if !is_load_balanced && !disable_monitoring_threads {
            for address in addresses {
                Monitor::start(
                    address.clone(),
                    updater.clone(),
                    watcher.clone(),
                    update_requester.subscribe(),
                    options.clone(),
                );
            }
        }

        let worker = TopologyWorker {
            id: ObjectId::new(),
            update_receiver,
            broadcaster,
            options,
            http_client,
            topology_watcher: watcher.clone(),
            topology_updater: updater.clone(),
            update_request_receiver,
            update_requester: update_requester.clone(),
        };

        // Emit events for the initialization of the topology and the seed list.
        if let Some(ref handler) = worker.options.sdam_event_handler {
            let event = TopologyOpeningEvent {
                topology_id: worker.id,
            };
            handler.handle_topology_opening_event(event);

            let new_description = worker
                .broadcaster
                .borrow_latest()
                .description
                .clone()
                .into();
            let event = TopologyDescriptionChangedEvent {
                topology_id: worker.id,
                previous_description: TopologyDescription::new_empty().into(),
                new_description,
            };
            handler.handle_topology_description_changed_event(event);

            for server_address in worker.options.hosts.iter() {
                let event = ServerOpeningEvent {
                    topology_id: worker.id,
                    address: server_address.clone(),
                };
                handler.handle_server_opening_event(event);
            }
        }

        // Load-balanced clients don't have a heartbeat monitor, so we synthesize
        // updating the server to `ServerType::LoadBalancer` with an RTT of 0 so it'll
        // be selected.
        if is_load_balanced {
            let mut new_state = worker.broadcaster.clone_latest();
            let old_description = new_state.description.clone();

            for server_address in new_state.servers.keys() {
                let new_desc = ServerDescription {
                    server_type: ServerType::LoadBalancer,
                    average_round_trip_time: Some(Duration::from_nanos(0)),
                    ..ServerDescription::new(server_address.clone(), None)
                };
                new_state
                    .description
                    .update(new_desc)
                    .map_err(Error::internal)?;
            }

            worker.process_topology_diff(&old_description, &new_state.description);
            worker.broadcaster.publish_new_state(new_state);
        }

        worker.start();

        Ok(NewTopology {
            watcher,
            updater,
            update_requester,
        })
    }

    pub(crate) fn watch(&self) -> TopologyWatcher {
        let mut watcher = self.watcher.clone();
        // mark the latest topology as seen
        watcher.receiver.borrow_and_update();
        watcher
    }

    #[cfg(test)]
    pub(crate) fn clone_updater(&self) -> TopologyUpdater {
        self.updater.clone()
    }

    pub(crate) fn request_update(&self) {
        self.update_requester.request()
    }

    pub(crate) async fn handle_application_error(
        &self,
        address: ServerAddress,
        error: Error,
        phase: HandshakePhase,
    ) {
        self.updater
            .handle_application_error(address, error, phase)
            .await;
    }

    pub(crate) fn cluster_time(&self) -> Option<ClusterTime> {
        self.watcher
            .borrow_latest()
            .description
            .cluster_time()
            .cloned()
    }

    pub(crate) fn topology_type(&self) -> TopologyType {
        self.watcher.borrow_latest().description.topology_type
    }

    pub(crate) fn session_support_status(&self) -> SessionSupportStatus {
        self.watcher
            .borrow_latest()
            .description
            .session_support_status()
    }

    pub(crate) fn transaction_support_status(&self) -> TransactionSupportStatus {
        self.watcher
            .borrow_latest()
            .description
            .transaction_support_status()
    }

    pub(crate) async fn advance_cluster_time(&self, to: ClusterTime) {
        self.updater.advance_cluster_time(to).await;
    }

    /// Updates the given `command` as needed based on the `criteria`.
    pub(crate) fn update_command_with_read_pref<T>(
        &self,
        server_address: &ServerAddress,
        command: &mut Command<T>,
        criteria: Option<&SelectionCriteria>,
    ) {
        self.watcher
            .borrow_latest()
            .description
            .update_command_with_read_pref(server_address, command, criteria)
    }

    /// Creates a new server selection timeout error message given the `criteria`.
    pub(crate) fn server_selection_timeout_error_message(
        &self,
        criteria: &SelectionCriteria,
    ) -> String {
        self.watcher
            .borrow_latest()
            .description
            .server_selection_timeout_error_message(criteria)
    }

    /// Gets the addresses of the servers in the cluster.
    #[cfg(test)]
    pub(crate) fn server_addresses(&self) -> HashSet<ServerAddress> {
        self.watcher
            .borrow_latest()
            .servers
            .keys()
            .cloned()
            .collect()
    }

    /// Gets the addresses of the servers in the cluster.
    #[cfg(test)]
    pub(crate) fn servers(&self) -> HashMap<ServerAddress, Arc<Server>> {
        self.watcher.borrow_latest().servers.clone()
    }

    #[cfg(test)]
    pub(crate) fn description(&self) -> TopologyDescription {
        self.watcher.borrow_latest().description.clone()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TopologyState {
    pub(crate) description: TopologyDescription,
    pub(crate) servers: HashMap<ServerAddress, Arc<Server>>,
}

#[derive(Debug)]
pub(crate) enum UpdateMessage {
    AdvanceClusterTime(ClusterTime),
    ServerUpdate(ServerDescription),
    SyncHosts(HashSet<ServerAddress>),
    MonitorError {
        address: ServerAddress,
        error: Error,
    },
    ApplicationError {
        address: ServerAddress,
        error: Error,
        phase: HandshakePhase,
    },
}

struct TopologyWorker {
    id: ObjectId,
    update_receiver: TopologyUpdateReceiver,
    broadcaster: TopologyBroadcaster,
    update_requester: UpdateRequester,
    options: ClientOptions,
    http_client: HttpClient,

    topology_watcher: TopologyWatcher,
    topology_updater: TopologyUpdater,
    update_request_receiver: TopologyUpdateRequestReceiver,
}

impl TopologyWorker {
    fn start(mut self) {
        runtime::execute(async move {
            while let Some(update) = self.update_receiver.recv().await {
                let (update, ack) = update.into_parts();
                println!("{:#?}", update);
                let changed = match update {
                    UpdateMessage::AdvanceClusterTime(to) => {
                        self.advance_cluster_time(to);
                        true
                    }
                    UpdateMessage::SyncHosts(hosts) => {
                        let mut state = self.broadcaster.clone_latest();
                        self.sync_hosts(&mut state, hosts);
                        self.broadcaster.publish_new_state(state);
                        true
                    }
                    UpdateMessage::ServerUpdate(sd) => self.update_server(sd).await.unwrap(),
                    UpdateMessage::MonitorError { address, error } => {
                        self.handle_monitor_error(address, error).await
                    }
                    UpdateMessage::ApplicationError {
                        address,
                        error,
                        phase,
                    } => self.handle_application_error(address, error, phase).await,
                };
                ack.acknowledge(changed);
            }
            println!("topology shutting down");
        });
    }

    fn advance_cluster_time(&mut self, to: ClusterTime) {
        let mut latest_state = self.broadcaster.clone_latest();
        latest_state.description.advance_cluster_time(&to);
        self.broadcaster.publish_new_state(latest_state);
    }

    fn sync_hosts(&self, state: &mut TopologyState, hosts: HashSet<ServerAddress>) {
        state.servers.retain(|host, _| hosts.contains(host));

        for address in hosts {
            if state.servers.contains_key(&address) {
                continue;
            }
            println!("adding {} to servers", address);
            let server = Server::new(
                address.clone(),
                self.options.clone(),
                self.http_client.clone(),
                self.topology_updater.clone(),
            );
            state.servers.insert(address.clone(), server);

            #[cfg(test)]
            let disable_monitoring_threads = self
                .options
                .test_options
                .as_ref()
                .map(|to| to.disable_monitoring_threads)
                .unwrap_or(false);
            #[cfg(not(test))]
            let disable_monitoring_threads = false;

            if !disable_monitoring_threads {
                Monitor::start(
                    address,
                    self.topology_updater.clone(),
                    self.topology_watcher.clone(),
                    self.update_requester.subscribe(),
                    self.options.clone(),
                );
            }
        }
    }

    async fn update_server(&mut self, sd: ServerDescription) -> std::result::Result<bool, String> {
        println!("updating {} to {:?}", sd.address, sd.server_type);
        let server_type = sd.server_type;
        let server_address = sd.address.clone();

        let mut latest_state = self.broadcaster.clone_latest();
        let old_description = latest_state.description.clone();

        latest_state.description.update(sd)?;

        let hosts = latest_state
            .description
            .server_addresses()
            .cloned()
            .collect();
        self.sync_hosts(&mut latest_state, hosts);

        let topology_changed =
            self.process_topology_diff(&old_description, &latest_state.description);
        // let diff = old_description.diff(&latest_state.description);
        // let topology_changed = diff.is_some();

        // if let Some(ref handler) = self.options.sdam_event_handler {
        //     if let Some(diff) = diff {
        //         for (address, (previous_description, new_description)) in diff.changed_servers {
        //             let event = ServerDescriptionChangedEvent {
        //                 address: address.clone(),
        //                 topology_id: self.id,
        //                 previous_description:
        // ServerInfo::new_owned(previous_description.clone()),                 
        // new_description: ServerInfo::new_owned(new_description.clone()),             };
        //             handler.handle_server_description_changed_event(event);
        //         }

        //         for address in diff.removed_addresses {
        //             let event = ServerClosedEvent {
        //                 address: address.clone(),
        //                 topology_id: self.id,
        //             };
        //             handler.handle_server_closed_event(event);
        //         }

        //         for address in diff.added_addresses {
        //             let event = ServerOpeningEvent {
        //                 address: address.clone(),
        //                 topology_id: self.id,
        //             };
        //             handler.handle_server_opening_event(event);
        //         }

        //         let event = TopologyDescriptionChangedEvent {
        //             topology_id: self.id,
        //             previous_description: old_description.clone().into(),
        //             new_description: latest_state.description.clone().into(),
        //         };
        //         handler.handle_topology_description_changed_event(event);
        //     }
        // }

        if topology_changed {
            if server_type.is_data_bearing()
                || (server_type != ServerType::Unknown
                    && latest_state.description.topology_type() == TopologyType::Single)
            {
                if let Some(s) = latest_state.servers.get(&server_address) {
                    s.pool.mark_as_ready().await;
                }
            }
            // println!("broadcasting new state {:#?}", latest_state);
            self.broadcaster.publish_new_state(latest_state)
        } else {
            println!("topology didn't change");
        }

        Ok(topology_changed)
    }

    fn process_topology_diff(
        &self,
        old_description: &TopologyDescription,
        new_description: &TopologyDescription,
    ) -> bool {
        let diff = old_description.diff(new_description);
        let changed = diff.is_some();
        if let Some(ref handler) = self.options.sdam_event_handler {
            if let Some(diff) = diff {
                for (address, (previous_description, new_description)) in diff.changed_servers {
                    let event = ServerDescriptionChangedEvent {
                        address: address.clone(),
                        topology_id: self.id,
                        previous_description: ServerInfo::new_owned(previous_description.clone()),
                        new_description: ServerInfo::new_owned(new_description.clone()),
                    };
                    handler.handle_server_description_changed_event(event);
                }

                for address in diff.removed_addresses {
                    let event = ServerClosedEvent {
                        address: address.clone(),
                        topology_id: self.id,
                    };
                    handler.handle_server_closed_event(event);
                }

                for address in diff.added_addresses {
                    let event = ServerOpeningEvent {
                        address: address.clone(),
                        topology_id: self.id,
                    };
                    handler.handle_server_opening_event(event);
                }

                let event = TopologyDescriptionChangedEvent {
                    topology_id: self.id,
                    previous_description: old_description.clone().into(),
                    new_description: new_description.clone().into(),
                };
                handler.handle_topology_description_changed_event(event);
            }
        }
        changed
    }

    async fn mark_server_as_unknown(&mut self, address: ServerAddress, error: Error) -> bool {
        let description = ServerDescription::new(address, Some(Err(error.to_string())));
        self.update_server(description).await.unwrap_or(false)
    }

    pub(crate) async fn handle_application_error(
        &mut self,
        address: ServerAddress,
        error: Error,
        handshake: HandshakePhase,
    ) -> bool {
        println!("handling application error {:#?} for {}", error, address);

        let server = match self.server(&address) {
            Some(s) => s,
            None => return false,
        };

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

        let is_load_balanced = self.broadcaster.borrow_latest().description.topology_type()
            == TopologyType::LoadBalanced;
        if error.is_state_change_error() {
            let updated = is_load_balanced
                || self
                    .update_server(ServerDescription::new(
                        server.address.clone(),
                        Some(Err(error.to_string())),
                    ))
                    .await
                    .unwrap();

            if updated && (error.is_shutting_down() || handshake.wire_version().unwrap_or(0) < 8) {
                server.pool.clear(error, handshake.service_id()).await;
            }
            self.update_requester.request();

            updated
        } else if error.is_non_timeout_network_error()
            || (handshake.is_before_completion()
                && (error.is_auth_error()
                    || error.is_network_timeout()
                    || error.is_command_error()))
        {
            let updated = is_load_balanced
                || self
                    .mark_server_as_unknown(server.address.clone(), error.clone())
                    .await;
            if updated {
                server.pool.clear(error, handshake.service_id()).await;
            }
            updated
        } else {
            false
        }
    }

    pub(crate) async fn handle_monitor_error(
        &mut self,
        address: ServerAddress,
        error: Error,
    ) -> bool {
        match self.server(&address) {
            Some(server) => {
                println!("handling monitor error {:#?} for {}", error, address);
                let updated = self.mark_server_as_unknown(address, error.clone()).await;
                if updated {
                    // The heartbeat monitor is disabled in load-balanced mode, so this will never
                    // have a service id.
                    server.pool.clear(error, None).await;
                }
                updated
            }
            None => {
                println!(
                    "{} not in {:?}",
                    address,
                    self.broadcaster
                        .borrow_latest()
                        .servers
                        .keys()
                        .collect::<Vec<_>>()
                );
                false
            }
        }
    }

    fn server(&self, address: &ServerAddress) -> Option<Arc<Server>> {
        self.broadcaster
            .borrow_latest()
            .servers
            .get(address)
            .cloned()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TopologyUpdater {
    sender: UnboundedSender<AcknowledgedMessage<UpdateMessage, bool>>,
}

impl TopologyUpdater {
    pub(crate) fn channel() -> (TopologyUpdater, TopologyUpdateReceiver) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let updater = TopologyUpdater { sender: tx };

        let update_receiver = TopologyUpdateReceiver {
            update_receiver: rx,
        };

        (updater, update_receiver)
    }

    async fn send_message(&self, update: UpdateMessage) -> bool {
        let (message, receiver) = AcknowledgedMessage::package(update);

        match self.sender.send(message) {
            Ok(_) => receiver.wait_for_acknowledgment().await.unwrap_or(false),
            _ => false,
        }
    }

    pub(crate) async fn handle_monitor_error(&self, address: ServerAddress, error: Error) -> bool {
        // let (message, receiver) =

        // let _: std::result::Result<_, _> = self
        //     .sender
        //     .send(UpdateMessage::MonitorError { address, error });
        self.send_message(UpdateMessage::MonitorError { address, error })
            .await
    }

    pub(crate) async fn handle_application_error(
        &self,
        address: ServerAddress,
        error: Error,
        phase: HandshakePhase,
    ) -> bool {
        self.send_message(UpdateMessage::ApplicationError {
            address,
            error,
            phase,
        })
        .await
    }

    pub(crate) async fn update(&self, sd: ServerDescription) -> bool {
        self.send_message(UpdateMessage::ServerUpdate(sd)).await
    }

    pub(crate) async fn advance_cluster_time(&self, to: ClusterTime) {
        self.send_message(UpdateMessage::AdvanceClusterTime(to))
            .await;
    }

    pub(crate) async fn sync_hosts(&self, hosts: HashSet<ServerAddress>) {
        self.send_message(UpdateMessage::SyncHosts(hosts)).await;
    }
}

pub(crate) struct TopologyUpdateReceiver {
    update_receiver: UnboundedReceiver<AcknowledgedMessage<UpdateMessage, bool>>,
}

impl TopologyUpdateReceiver {
    pub(crate) async fn recv(&mut self) -> Option<AcknowledgedMessage<UpdateMessage, bool>> {
        self.update_receiver.recv().await
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TopologyWatcher {
    receiver: watch::Receiver<TopologyState>,
}

impl TopologyWatcher {
    fn channel(initial_state: TopologyState) -> (TopologyWatcher, TopologyBroadcaster) {
        let (tx, rx) = watch::channel(initial_state);
        let watcher = TopologyWatcher { receiver: rx };
        let broadcaster = TopologyBroadcaster { state_sender: tx };
        (watcher, broadcaster)
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.receiver.has_changed().is_ok()
    }

    pub(crate) fn server_description(&self, address: &ServerAddress) -> Option<ServerDescription> {
        self.receiver
            .borrow()
            .description
            .get_server_description(address)
            .cloned()
    }

    pub(crate) fn clone_latest(&mut self) -> TopologyState {
        self.receiver.borrow_and_update().clone()
    }

    pub(crate) async fn wait_for_update(&mut self, timeout: Duration) -> bool {
        let changed = runtime::timeout(timeout, self.receiver.changed())
            .await
            .is_ok();
        self.receiver.borrow_and_update();
        changed
    }

    pub(crate) fn borrow_latest(&self) -> Ref<TopologyState> {
        self.receiver.borrow()
    }

    pub(crate) fn topology_type(&self) -> TopologyType {
        self.borrow_latest().description.topology_type
    }
}

struct TopologyBroadcaster {
    state_sender: watch::Sender<TopologyState>,
}

impl TopologyBroadcaster {
    fn clone_latest(&self) -> TopologyState {
        self.borrow_latest().clone()
    }

    fn borrow_latest(&self) -> Ref<TopologyState> {
        self.state_sender.borrow()
    }

    fn publish_new_state(&self, state: TopologyState) {
        let _ = self.state_sender.send(state);
    }
}

#[derive(Clone, Debug)]
struct UpdateRequester {
    sender: broadcast::Sender<()>,
}

impl UpdateRequester {
    fn channel() -> (UpdateRequester, TopologyUpdateRequestReceiver) {
        let (tx, rx) = broadcast::channel(1);
        (
            UpdateRequester { sender: tx },
            TopologyUpdateRequestReceiver { receiver: rx },
        )
    }

    fn request(&self) {
        let _ = self.sender.send(());
    }

    fn subscribe(&self) -> TopologyUpdateRequestReceiver {
        TopologyUpdateRequestReceiver {
            receiver: self.sender.subscribe(),
        }
    }
}

pub(crate) struct TopologyUpdateRequestReceiver {
    receiver: broadcast::Receiver<()>,
}

impl TopologyUpdateRequestReceiver {
    pub(crate) async fn wait_for_update_request(&mut self, timeout: Duration) {
        let _: std::result::Result<_, _> = runtime::timeout(timeout, self.receiver.recv()).await;
    }

    pub(crate) fn clear_update_requests(&mut self) {
        let _: std::result::Result<_, _> = self.receiver.try_recv();
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
