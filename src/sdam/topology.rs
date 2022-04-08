use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use bson::oid::ObjectId;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    watch::{self, Ref},
};

use crate::{
    client::options::{ClientOptions, ServerAddress},
    cmap::{conn::ConnectionGeneration, Command, Connection, PoolGeneration},
    error::{Error, Result},
    event::sdam::{
        ServerClosedEvent,
        ServerDescriptionChangedEvent,
        ServerOpeningEvent,
        TopologyDescriptionChangedEvent,
    },
    runtime,
    runtime::HttpClient,
    selection_criteria::SelectionCriteria,
    ClusterTime,
    ServerInfo,
    ServerType,
    TopologyType,
};

use super::{
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

        for address in addresses {
            Monitor::start(
                address,
                updater.clone(),
                watcher.clone(),
                update_request_receiver.clone(),
                options.clone(),
            );
        }

        TopologyWorker {
            id: ObjectId::new(),
            update_receiver,
            broadcaster,
            options,
            http_client,
            topology_watcher: watcher.clone(),
            topology_updater: updater.clone(),
            update_request_receiver,
        }
        .start();

        Ok(NewTopology {
            watcher,
            updater,
            update_requester,
        })
    }

    pub(crate) fn watch(&self) -> TopologyWatcher {
        self.watcher.clone()
    }

    #[cfg(test)]
    pub(crate) fn clone_updater(&self) -> TopologyUpdater {
        self.updater.clone()
    }

    pub(crate) fn request_update(&self) {
        self.update_requester.request()
    }

    pub(crate) fn handle_application_error(
        &self,
        address: ServerAddress,
        error: Error,
        phase: HandshakePhase,
    ) {
        self.updater.handle_application_error(address, error, phase);
    }

    pub(crate) fn cluster_time(&self) -> Option<ClusterTime> {
        self.watcher
            .borrow_latest_state()
            .description
            .cluster_time()
            .cloned()
    }

    pub(crate) fn topology_type(&self) -> TopologyType {
        self.watcher.borrow_latest_state().description.topology_type
    }

    pub(crate) fn session_support_status(&self) -> SessionSupportStatus {
        self.watcher
            .borrow_latest_state()
            .description
            .session_support_status()
    }

    pub(crate) fn transaction_support_status(&self) -> TransactionSupportStatus {
        self.watcher
            .borrow_latest_state()
            .description
            .transaction_support_status()
    }

    pub(crate) fn advance_cluster_time(&self, to: ClusterTime) {
        self.updater.advance_cluster_time(to)
    }

    /// Updates the given `command` as needed based on the `criteria`.
    pub(crate) fn update_command_with_read_pref<T>(
        &self,
        server_address: &ServerAddress,
        command: &mut Command<T>,
        criteria: Option<&SelectionCriteria>,
    ) {
        self.watcher
            .borrow_latest_state()
            .description
            .update_command_with_read_pref(server_address, command, criteria)
    }

    /// Creates a new server selection timeout error message given the `criteria`.
    pub(crate) fn server_selection_timeout_error_message(
        &self,
        criteria: &SelectionCriteria,
    ) -> String {
        self.watcher
            .borrow_latest_state()
            .description
            .server_selection_timeout_error_message(criteria)
    }

    /// Gets the addresses of the servers in the cluster.
    #[cfg(test)]
    pub(crate) fn server_addresses(&self) -> HashSet<ServerAddress> {
        self.watcher
            .borrow_latest_state()
            .servers
            .keys()
            .cloned()
            .collect()
    }

    /// Gets the addresses of the servers in the cluster.
    #[cfg(test)]
    pub(crate) fn servers(&self) -> HashMap<ServerAddress, Arc<Server>> {
        self.watcher.borrow_latest_state().servers.clone()
    }

    #[cfg(test)]
    pub(crate) fn description(&self) -> TopologyDescription {
        self.watcher.borrow_latest_state().description.clone()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TopologyState {
    pub(crate) description: TopologyDescription,
    pub(crate) servers: HashMap<ServerAddress, Arc<Server>>,
}

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
                match update {
                    UpdateMessage::AdvanceClusterTime(to) => {
                        self.advance_cluster_time(to);
                    }
                    UpdateMessage::SyncHosts(hosts) => {
                        let mut state = self.broadcaster.clone_latest();
                        self.sync_hosts(&mut state, hosts);
                        self.broadcaster.publish_new_state(state);
                    }
                    UpdateMessage::ServerUpdate(sd) => {
                        println!("received update for {}", sd.address);
                        self.update_server(sd).await.unwrap();
                    }
                    UpdateMessage::MonitorError { address, error } => {
                        todo!()
                    }
                    UpdateMessage::ApplicationError {
                        address,
                        error,
                        phase,
                    } => {
                        todo!()
                    }
                }
            }
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
            let server = Server::new(
                address.clone(),
                self.options.clone(),
                self.http_client.clone(),
                self.topology_updater.clone(),
            );
            state.servers.insert(address.clone(), server);
            Monitor::start(
                address,
                self.topology_updater.clone(),
                self.topology_watcher.clone(),
                self.update_request_receiver.clone(),
                self.options.clone(),
            );
        }
    }

    async fn update_server(&mut self, sd: ServerDescription) -> std::result::Result<bool, String> {
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
        // let hosts: HashSet<_> = latest_state
        //     .description
        //     .server_addresses()
        //     .cloned()
        //     .collect();

        // latest_state.servers.retain(|host, _| hosts.contains(host));

        // for address in hosts {
        //     if latest_state.servers.contains_key(&address) {
        //         continue;
        //     }
        //     let server = Server::new(
        //         address.clone(),
        //         self.options.clone(),
        //         self.http_client.clone(),
        //         self.topology_updater.clone(),
        //     );
        //     latest_state.servers.insert(address.clone(), server);
        //     Monitor::start(
        //         address,
        //         self.topology_updater.clone(),
        //         self.topology_watcher.clone(),
        //         self.update_request_receiver.clone(),
        //         self.options.clone(),
        //     );
        // }

        let diff = old_description.diff(&latest_state.description);
        let topology_changed = diff.is_some();

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
                    new_description: latest_state.description.clone().into(),
                };
                handler.handle_topology_description_changed_event(event);
            }
        }

        if topology_changed {
            if server_type.is_data_bearing()
                || (server_type != ServerType::Unknown
                    && latest_state.description.topology_type() == TopologyType::Single)
            {
                if let Some(s) = latest_state.servers.get(&server_address) {
                    s.pool.mark_as_ready().await;
                }
            }
            self.broadcaster.publish_new_state(latest_state)
        }

        Ok(topology_changed)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TopologyUpdater {
    sender: UnboundedSender<UpdateMessage>,
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

    pub(crate) fn handle_monitor_error(&self, address: ServerAddress, error: Error) {
        let _: std::result::Result<_, _> = self
            .sender
            .send(UpdateMessage::MonitorError { address, error });
    }

    pub(crate) fn handle_application_error(
        &self,
        address: ServerAddress,
        error: Error,
        phase: HandshakePhase,
    ) {
        let _: std::result::Result<_, _> = self.sender.send(UpdateMessage::ApplicationError {
            address,
            error,
            phase,
        });
    }

    pub(crate) fn update(&self, sd: ServerDescription) {
        let _: std::result::Result<_, _> = self.sender.send(UpdateMessage::ServerUpdate(sd));
    }

    pub(crate) fn advance_cluster_time(&self, to: ClusterTime) {
        let _: std::result::Result<_, _> = self.sender.send(UpdateMessage::AdvanceClusterTime(to));
    }

    pub(crate) fn sync_hosts(&self, hosts: HashSet<ServerAddress>) {
        let _: std::result::Result<_, _> = self.sender.send(UpdateMessage::SyncHosts(hosts));
    }
}

pub(crate) struct TopologyUpdateReceiver {
    update_receiver: UnboundedReceiver<UpdateMessage>,
}

impl TopologyUpdateReceiver {
    pub(crate) async fn recv(&mut self) -> Option<UpdateMessage> {
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

    pub(crate) fn clone_latest_state(&mut self) -> TopologyState {
        self.receiver.borrow_and_update().clone()
    }

    pub(crate) async fn wait_for_update(&mut self, timeout: Duration) -> bool {
        let changed = runtime::timeout(timeout, self.receiver.changed())
            .await
            .is_ok();
        self.receiver.borrow_and_update();
        changed
    }

    pub(crate) fn borrow_latest_state(&self) -> Ref<TopologyState> {
        self.receiver.borrow()
    }

    pub(crate) fn topology_type(&self) -> TopologyType {
        self.borrow_latest_state().description.topology_type
    }
}

struct TopologyBroadcaster {
    state_sender: watch::Sender<TopologyState>,
}

impl TopologyBroadcaster {
    fn clone_latest(&self) -> TopologyState {
        self.state_sender.borrow().clone()
    }

    fn publish_new_state(&self, state: TopologyState) {
        let _ = self.state_sender.send(state);
    }
}

#[derive(Debug)]
struct UpdateRequester {
    sender: watch::Sender<()>,
}

impl UpdateRequester {
    fn channel() -> (UpdateRequester, TopologyUpdateRequestReceiver) {
        let (tx, rx) = watch::channel(());
        (
            UpdateRequester { sender: tx },
            TopologyUpdateRequestReceiver { receiver: rx },
        )
    }

    fn request(&self) {
        let _ = self.sender.send(());
    }
}

#[derive(Clone)]
pub(crate) struct TopologyUpdateRequestReceiver {
    receiver: watch::Receiver<()>,
}

impl TopologyUpdateRequestReceiver {
    pub(crate) async fn wait_for_update_request(&mut self, timeout: Duration) {
        let _: std::result::Result<_, _> = runtime::timeout(timeout, self.receiver.changed()).await;
    }

    pub(crate) fn clear_update_requests(&mut self) {
        self.receiver.borrow_and_update();
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
