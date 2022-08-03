use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
    time::Duration,
};

use bson::oid::ObjectId;
#[cfg(test)]
use futures_util::stream::{FuturesUnordered, StreamExt};
use futures_util::FutureExt;
use tokio::sync::{
    broadcast,
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    watch::{self, Ref},
};

use crate::{
    client::options::{ClientOptions, ServerAddress},
    cmap::{conn::ConnectionGeneration, Command, Connection, PoolGeneration},
    error::{load_balanced_mode_mismatch, Error, Result},
    event::sdam::{
        handle_sdam_event,
        SdamEvent,
        ServerClosedEvent,
        ServerDescriptionChangedEvent,
        ServerOpeningEvent,
        TopologyClosedEvent,
        TopologyDescriptionChangedEvent,
        TopologyOpeningEvent,
    },
    runtime::{self, AcknowledgedMessage, HttpClient, WorkerHandle, WorkerHandleListener},
    selection_criteria::SelectionCriteria,
    ClusterTime,
    ServerInfo,
    ServerType,
    TopologyType,
};

use super::{
    srv_polling::SrvPollingMonitor,
    Monitor,
    Server,
    ServerDescription,
    SessionSupportStatus,
    TopologyDescription,
    TransactionSupportStatus,
};

/// A struct providing access to the client's current view of the topology.
/// When this is dropped, monitors will stop performing checks.
#[derive(Debug)]
pub(crate) struct Topology {
    watcher: TopologyWatcher,
    updater: TopologyUpdater,
    check_requester: TopologyCheckManager,
    _worker_handle: WorkerHandle,
}

impl Topology {
    pub(crate) fn new(options: ClientOptions) -> Result<Topology> {
        let http_client = HttpClient::default();
        let description = TopologyDescription::default();

        let update_requester = TopologyCheckManager::new();
        let event_emitter = options.sdam_event_handler.as_ref().map(|handler| {
            let (tx, mut rx) = mpsc::unbounded_channel::<AcknowledgedMessage<SdamEvent>>();

            let handler = handler.clone();
            runtime::execute(async move {
                while let Some(event) = rx.recv().await {
                    let (event, ack) = event.into_parts();

                    let is_closed = matches!(event, SdamEvent::TopologyClosed(_));
                    handle_sdam_event(handler.as_ref(), event);

                    ack.acknowledge(());

                    // no more events should be emitted after a TopologyClosedEvent
                    if is_closed {
                        break;
                    };
                }
            });
            SdamEventEmitter { sender: tx }
        });

        let (updater, update_receiver) = TopologyUpdater::channel();
        let (worker_handle, handle_listener) = WorkerHandleListener::channel();
        let state = TopologyState {
            description,
            servers: Default::default(),
        };
        let (watcher, publisher) = TopologyWatcher::channel(state);

        let mut worker = TopologyWorker {
            id: ObjectId::new(),
            update_receiver,
            publisher,
            options,
            http_client,
            topology_watcher: watcher.clone(),
            topology_updater: updater.clone(),
            check_manager: update_requester.clone(),
            handle_listener,
            event_emitter,
        };

        worker.initialize()?;
        worker.start();

        Ok(Topology {
            watcher,
            updater,
            check_requester: update_requester,
            _worker_handle: worker_handle,
        })
    }

    /// Begin watching for changes in the topology.
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

    /// Request that all server monitors perform an immediate check of the topology.
    pub(crate) fn request_update(&self) {
        self.check_requester.request_check()
    }

    /// Handle an error that occurred during operation execution.
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

    /// Get the topology's currently highest seen cluster time.
    pub(crate) fn cluster_time(&self) -> Option<ClusterTime> {
        self.watcher
            .peek_latest()
            .description
            .cluster_time()
            .cloned()
    }

    /// Update the topology's highest seen cluster time.
    /// If the provided cluster time is not higher than the topology's currently highest seen
    /// cluster time, this method has no effect.
    pub(crate) async fn advance_cluster_time(&self, to: ClusterTime) {
        self.updater.advance_cluster_time(to).await;
    }

    pub(crate) fn topology_type(&self) -> TopologyType {
        self.watcher.peek_latest().description.topology_type
    }

    /// Gets the latest information on whether sessions are supported or not.
    pub(crate) fn session_support_status(&self) -> SessionSupportStatus {
        self.watcher
            .peek_latest()
            .description
            .session_support_status()
    }

    /// Gets the latest information on whether transactions are support or not.
    pub(crate) fn transaction_support_status(&self) -> TransactionSupportStatus {
        self.watcher
            .peek_latest()
            .description
            .transaction_support_status()
    }

    /// Updates the given `command` as needed based on the `criteria`.
    pub(crate) fn update_command_with_read_pref<T>(
        &self,
        server_address: &ServerAddress,
        command: &mut Command<T>,
        criteria: Option<&SelectionCriteria>,
    ) {
        self.watcher
            .peek_latest()
            .description
            .update_command_with_read_pref(server_address, command, criteria)
    }

    /// Creates a new server selection timeout error message given the `criteria`.
    pub(crate) fn server_selection_timeout_error_message(
        &self,
        criteria: &SelectionCriteria,
    ) -> String {
        self.watcher
            .peek_latest()
            .description
            .server_selection_timeout_error_message(criteria)
    }

    /// Gets the addresses of the servers in the cluster.
    #[cfg(test)]
    pub(crate) fn server_addresses(&self) -> HashSet<ServerAddress> {
        self.watcher.peek_latest().servers.keys().cloned().collect()
    }

    /// Gets the addresses of the servers in the cluster.
    #[cfg(test)]
    pub(crate) fn servers(&self) -> HashMap<ServerAddress, Arc<Server>> {
        self.watcher.peek_latest().servers.clone()
    }

    #[cfg(test)]
    pub(crate) fn description(&self) -> TopologyDescription {
        self.watcher.peek_latest().description.clone()
    }

    #[cfg(test)]
    pub(crate) async fn sync_workers(&self) {
        self.updater.sync_workers().await;
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
    ServerUpdate(Box<ServerDescription>),
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
    #[cfg(test)]
    SyncWorkers,
}

struct TopologyWorker {
    id: ObjectId,
    update_receiver: TopologyUpdateReceiver,
    handle_listener: WorkerHandleListener,
    publisher: TopologyPublisher,
    event_emitter: Option<SdamEventEmitter>,
    options: ClientOptions,
    http_client: HttpClient,

    // the following fields stored here for creating new server monitors
    topology_watcher: TopologyWatcher,
    topology_updater: TopologyUpdater,
    check_manager: TopologyCheckManager,
}

impl TopologyWorker {
    fn initialize(&mut self) -> Result<()> {
        self.emit_event(|| {
            SdamEvent::TopologyOpening(TopologyOpeningEvent {
                topology_id: self.id,
            })
        });

        let mut new_description = self.borrow_latest_state().description.clone();

        new_description.initialize(&self.options)?;

        self.update_topology(new_description);

        if self.options.load_balanced == Some(true) {
            let base = ServerDescription::new(self.options.hosts[0].clone(), None, None);
            self.update_server(ServerDescription {
                server_type: ServerType::LoadBalancer,
                average_round_trip_time: Some(Duration::from_millis(0)),
                ..base
            });
        }

        Ok(())
    }

    fn start(mut self) {
        if self.monitoring_enabled() {
            SrvPollingMonitor::start(
                self.topology_updater.clone(),
                self.topology_watcher.clone(),
                self.options.clone(),
            );
        }

        runtime::execute(async move {
            loop {
                tokio::select! {
                    Some(update) = self.update_receiver.recv() => {
                        let (update, ack) = update.into_parts();
                        let changed = match update {
                            UpdateMessage::AdvanceClusterTime(to) => {
                                self.advance_cluster_time(to);
                                true
                            }
                            UpdateMessage::SyncHosts(hosts) => {
                                self.sync_hosts(hosts)
                            }
                            UpdateMessage::ServerUpdate(sd) => self.update_server(*sd),
                            UpdateMessage::MonitorError { address, error } => {
                                self.handle_monitor_error(address, error).await
                            }
                            UpdateMessage::ApplicationError {
                                address,
                                error,
                                phase,
                            } => self.handle_application_error(address, error, phase).await,
                            #[cfg(test)]
                            UpdateMessage::SyncWorkers => {
                                let rxen: FuturesUnordered<_> = self
                                    .borrow_latest_state()
                                    .servers
                                    .values()
                                    .map(|v| v.pool.sync_worker())
                                    .collect();
                                let _: Vec<_> = rxen.collect().await;
                                false
                            }
                        };
                        ack.acknowledge(changed);
                    },
                    _ = self.handle_listener.wait_for_all_handle_drops() => {
                        break
                    }
                }
            }

            // indicate to the topology watchers that the topology is no longer alive
            drop(self.publisher);

            if let Some(emitter) = self.event_emitter {
                emitter
                    .emit(SdamEvent::TopologyClosed(TopologyClosedEvent {
                        topology_id: self.id,
                    }))
                    .await;
            }
        });
    }

    /// Get a reference to the latest topology state.
    ///
    /// Note that this holds a read lock, so broadcasting a new state while an
    /// outstanding reference to the `TopologyState` is held will deadlock.
    fn borrow_latest_state(&self) -> Ref<TopologyState> {
        self.topology_watcher.peek_latest()
    }

    fn advance_cluster_time(&mut self, to: ClusterTime) {
        let mut latest_state = self.borrow_latest_state().clone();
        latest_state.description.advance_cluster_time(&to);
        self.publisher.publish_new_state(latest_state);
    }

    fn sync_hosts(&mut self, hosts: HashSet<ServerAddress>) -> bool {
        let mut new_description = self.borrow_latest_state().description.clone();
        new_description.sync_hosts(&hosts);
        self.update_topology(new_description)
    }

    /// Update the topology using the provided `ServerDescription`.
    fn update_server(&mut self, sd: ServerDescription) -> bool {
        let mut latest_description = self.borrow_latest_state().description.clone();
        // TODO: RUST-1270 change this method to not return a result.
        let _ = latest_description.update(sd);
        self.update_topology(latest_description)
    }

    /// Emit the appropriate SDAM monitoring events given the changes to the
    /// topology as the result of an update, if any.
    fn update_topology(&self, new_description: TopologyDescription) -> bool {
        let old_state = self.borrow_latest_state().clone();

        let mut new_state = TopologyState {
            description: new_description,
            servers: old_state.servers.clone(),
        };

        let diff = old_state.description.diff(&new_state.description);
        let changed = diff.is_some();
        if let Some(diff) = diff {
            for (address, (previous_description, new_description)) in diff.changed_servers {
                if new_description.server_type.is_data_bearing()
                    || (new_description.server_type != ServerType::Unknown
                        && new_state.description.topology_type() == TopologyType::Single)
                {
                    if let Some(s) = new_state.servers.get(address) {
                        s.pool.mark_as_ready();
                    }
                }
                self.emit_event(|| {
                    SdamEvent::ServerDescriptionChanged(Box::new(ServerDescriptionChangedEvent {
                        address: address.clone(),
                        topology_id: self.id,
                        previous_description: ServerInfo::new_owned(previous_description.clone()),
                        new_description: ServerInfo::new_owned(new_description.clone()),
                    }))
                });
            }

            for address in diff.removed_addresses {
                let removed_server = new_state.servers.remove(address);
                debug_assert!(
                    removed_server.is_some(),
                    "tried to remove non-existent address from topology: {}",
                    address
                );

                self.emit_event(|| {
                    SdamEvent::ServerClosed(ServerClosedEvent {
                        address: address.clone(),
                        topology_id: self.id,
                    })
                });
            }

            self.emit_event(|| {
                SdamEvent::TopologyDescriptionChanged(Box::new(TopologyDescriptionChangedEvent {
                    topology_id: self.id,
                    previous_description: old_state.description.clone().into(),
                    new_description: new_state.description.clone().into(),
                }))
            });

            for address in diff.added_addresses {
                if new_state.servers.contains_key(address) {
                    debug_assert!(
                        false,
                        "adding address that already exists in topology: {}",
                        address
                    );
                    continue;
                }

                let (monitor_handle, listener) = WorkerHandleListener::channel();

                let server = Server::new(
                    address.clone(),
                    monitor_handle,
                    self.options.clone(),
                    self.http_client.clone(),
                    self.topology_updater.clone(),
                );
                new_state.servers.insert(address.clone(), server);

                if self.monitoring_enabled() {
                    Monitor::start(
                        address.clone(),
                        self.topology_updater.clone(),
                        self.topology_watcher.clone(),
                        self.event_emitter.clone(),
                        self.check_manager.subscribe(),
                        listener,
                        self.options.clone(),
                    );
                }

                self.emit_event(|| {
                    SdamEvent::ServerOpening(ServerOpeningEvent {
                        address: address.clone(),
                        topology_id: self.id,
                    })
                });
            }
        }

        self.publisher.publish_new_state(new_state);
        changed
    }

    /// Mark the server at the given address as Unknown using the provided error as the cause.
    fn mark_server_as_unknown(&mut self, address: ServerAddress, error: Error) -> bool {
        let description = ServerDescription::new(address, Some(Err(error)), None);
        self.update_server(description)
    }

    /// Handle an error that occurred during opreration execution.
    pub(crate) async fn handle_application_error(
        &mut self,
        address: ServerAddress,
        error: Error,
        handshake: HandshakePhase,
    ) -> bool {
        match self.server_description(&address) {
            Some(sd) => {
                if let Some(existing_tv) = sd.topology_version() {
                    if let Some(tv) = error.topology_version() {
                        // If the error is from a stale topology version, ignore it.
                        if !tv.is_more_recent_than(existing_tv) {
                            return false;
                        }
                    }
                }
            }
            None => return false,
        }

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

        let is_load_balanced =
            self.borrow_latest_state().description.topology_type() == TopologyType::LoadBalanced;
        if error.is_state_change_error() {
            let updated = is_load_balanced || self.mark_server_as_unknown(address, error.clone());

            if updated && (error.is_shutting_down() || handshake.wire_version().unwrap_or(0) < 8) {
                server.pool.clear(error, handshake.service_id()).await;
            }
            self.check_manager.request_check();

            updated
        } else if error.is_non_timeout_network_error()
            || (handshake.is_before_completion()
                && (error.is_auth_error()
                    || error.is_network_timeout()
                    || error.is_command_error()))
        {
            let updated = is_load_balanced
                || self.mark_server_as_unknown(server.address.clone(), error.clone());
            if updated {
                server
                    .pool
                    .clear(error.clone(), handshake.service_id())
                    .await;
                if !error.is_auth_error() {
                    self.check_manager.cancel_in_progress_checks(error);
                }
            }
            updated
        } else {
            false
        }
    }

    /// Handle an error that occurred during a monitor check.
    pub(crate) async fn handle_monitor_error(
        &mut self,
        address: ServerAddress,
        error: Error,
    ) -> bool {
        match self.server(&address) {
            Some(server) => {
                let updated = self.mark_server_as_unknown(address, error.clone());
                if updated {
                    // The heartbeat monitor is disabled in load-balanced mode, so this will never
                    // have a service id.
                    server.pool.clear(error, None).await;
                }
                updated
            }
            None => false,
        }
    }

    /// Get the server at the provided address if present in the topology.
    fn server(&self, address: &ServerAddress) -> Option<Arc<Server>> {
        self.borrow_latest_state().servers.get(address).cloned()
    }

    /// Get the server at the provided address if present in the topology.
    fn server_description(&self, address: &ServerAddress) -> Option<ServerDescription> {
        self.borrow_latest_state()
            .description
            .get_server_description(address)
            .cloned()
    }

    fn emit_event(&self, make_event: impl FnOnce() -> SdamEvent) {
        if let Some(ref emitter) = self.event_emitter {
            let _ = emitter.emit(make_event());
        }
    }

    fn monitoring_enabled(&self) -> bool {
        #[cfg(test)]
        {
            self.options
                .test_options
                .as_ref()
                .map(|to| !to.disable_monitoring_threads)
                .unwrap_or(true)
        }

        #[cfg(not(test))]
        true
    }
}

/// Struct used to update the topology.
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

    /// Handle an error that occurred during a monitor check.
    pub(crate) async fn handle_monitor_error(&self, address: ServerAddress, error: Error) -> bool {
        self.send_message(UpdateMessage::MonitorError { address, error })
            .await
    }

    /// Handle an error that occurred during operation execution.
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

    /// Update the topology using the provided server description, returning a bool
    /// indicating whether the topology changed as a result of the update.
    pub(crate) async fn update(&self, sd: ServerDescription) -> bool {
        self.send_message(UpdateMessage::ServerUpdate(Box::new(sd)))
            .await
    }

    pub(crate) async fn advance_cluster_time(&self, to: ClusterTime) {
        self.send_message(UpdateMessage::AdvanceClusterTime(to))
            .await;
    }

    /// Update the provided state to contain the given list of hosts, removing any
    /// existing servers whose addresses aren't present in the list.
    ///
    /// This will start server monitors for the newly added servers.
    pub(crate) async fn sync_hosts(&self, hosts: HashSet<ServerAddress>) {
        self.send_message(UpdateMessage::SyncHosts(hosts)).await;
    }

    #[cfg(test)]
    pub(crate) async fn sync_workers(&self) {
        self.send_message(UpdateMessage::SyncWorkers).await;
    }
}

/// Receiver used to listen for updates to the topology.
pub(crate) struct TopologyUpdateReceiver {
    update_receiver: UnboundedReceiver<AcknowledgedMessage<UpdateMessage, bool>>,
}

impl TopologyUpdateReceiver {
    pub(crate) async fn recv(&mut self) -> Option<AcknowledgedMessage<UpdateMessage, bool>> {
        self.update_receiver.recv().await
    }
}

/// Struct used to get the latest topology state and monitor the topology for changes.
#[derive(Debug, Clone)]
pub(crate) struct TopologyWatcher {
    receiver: watch::Receiver<TopologyState>,
}

impl TopologyWatcher {
    fn channel(initial_state: TopologyState) -> (TopologyWatcher, TopologyPublisher) {
        let (tx, rx) = watch::channel(initial_state);
        let watcher = TopologyWatcher { receiver: rx };
        let publisher = TopologyPublisher { state_sender: tx };
        (watcher, publisher)
    }

    /// Whether the topology is still active or if all `Client` instances using it have gone
    /// out of scope.
    pub(crate) fn is_alive(&self) -> bool {
        self.receiver.has_changed().is_ok()
    }

    /// Get a server description for the server at the provided address.
    pub(crate) fn server_description(&self, address: &ServerAddress) -> Option<ServerDescription> {
        self.receiver
            .borrow()
            .description
            .get_server_description(address)
            .cloned()
    }

    /// Clone the latest state, marking it as seen.
    pub(crate) fn observe_latest(&mut self) -> TopologyState {
        self.receiver.borrow_and_update().clone()
    }

    /// Wait for a new state to be published or for the timeout to be reached, returning a bool
    /// indicating whether an update was seen or not.
    ///
    /// This method marks the new topology state as seen.
    pub(crate) async fn wait_for_update(&mut self, timeout: Duration) -> bool {
        let changed = runtime::timeout(timeout, self.receiver.changed())
            .await
            .is_ok();
        self.receiver.borrow_and_update();
        changed
    }

    /// Borrow the latest state. This does not mark it as seen.
    ///
    /// Note: this method holds a read lock on the state, so it is best if the borrow is
    /// short-lived. For longer use-cases, clone the `TopologyState` or use `observe_latest`
    /// instead.
    pub(crate) fn peek_latest(&self) -> Ref<TopologyState> {
        self.receiver.borrow()
    }

    pub(crate) fn topology_type(&self) -> TopologyType {
        self.peek_latest().description.topology_type
    }
}

/// Struct used to broadcsat the latest view of the topology.
struct TopologyPublisher {
    state_sender: watch::Sender<TopologyState>,
}

impl TopologyPublisher {
    /// Publish a new state, notifying all the of the outstanding `TopologyWatcher`s.
    ///
    /// Note that even if the provided state is equivalent to the previously broadcasted
    /// `TopologyState`, the watchers will still be notified.
    fn publish_new_state(&self, state: TopologyState) {
        let _ = self.state_sender.send(state);
    }
}

/// Handle used to request that monitors perform immediate checks of the topology.
#[derive(Clone, Debug)]
struct TopologyCheckManager {
    sender: broadcast::Sender<TopologyCheckMessage>,
}

/// Messages that a `TopologyCheckManager` can broadcast to all monitors.
#[derive(Debug, Clone)]
enum TopologyCheckMessage {
    /// The monitor should perform an immediate check at next opportunity.
    ImmediateCheck,

    /// The monitor should cancel any in-progress checks.
    CancelCheck { reason: Error },
}

impl TopologyCheckManager {
    fn new() -> TopologyCheckManager {
        let (tx, _rx) = broadcast::channel(1);
        TopologyCheckManager { sender: tx }
    }

    /// Request that all monitors perform immediate checks.
    fn request_check(&self) {
        let _ = self.sender.send(TopologyCheckMessage::ImmediateCheck);
    }

    fn cancel_in_progress_checks(&self, reason: Error) {
        let _ = self
            .sender
            .send(TopologyCheckMessage::CancelCheck { reason });
    }

    /// Subscribe to check requests.
    fn subscribe(&self) -> TopologyCheckRequestReceiver {
        TopologyCheckRequestReceiver {
            receiver: self.sender.subscribe(),
        }
    }
}

/// Receiver used to listen for check requests.
pub(crate) struct TopologyCheckRequestReceiver {
    receiver: broadcast::Receiver<TopologyCheckMessage>,
}

impl TopologyCheckRequestReceiver {
    /// Wait until a check request is seen or the given duration has elapsed.
    pub(crate) async fn wait_for_check_request(&mut self, timeout: Duration) {
        let _: std::result::Result<_, _> = runtime::timeout(timeout, async {
            loop {
                match self.receiver.recv().await {
                    Ok(TopologyCheckMessage::ImmediateCheck) => return,
                    Ok(TopologyCheckMessage::CancelCheck { .. }) => continue,
                    Err(_) => return,
                }
            }
        })
        .await;
    }

    // pub(crate) fn subscribe_to_cancellations(&self) -> Option<()> {
    //     self.receiver.clone()
    // }

    /// Wait until a cancellation is seen or the given duration has elapsed, ignoring any check
    /// requests. If a cancellation was requested, this method returns the error that caused it.
    /// Otherwise None is returned.
    pub(crate) async fn listen_for_cancellation(&mut self) -> Option<Error> {
        loop {
            match self.receiver.recv().await {
                Ok(TopologyCheckMessage::CancelCheck { reason }) => return Some(reason),
                Ok(TopologyCheckMessage::ImmediateCheck) => continue,
                Err(_) => return None,
            }
        }
    }

    /// Clear out prior check requests so that the next call to `wait_for_check_requests` doesn't
    /// return immediately.
    pub(crate) fn clear_all_requests(&mut self) {
        let _: std::result::Result<_, _> = self.receiver.try_recv();
    }
}

/// Handle used to emit SDAM events.
///
/// If the topology has been closed, events emitted via this handle will not be sent to
/// handlers.
#[derive(Clone)]
pub(crate) struct SdamEventEmitter {
    sender: UnboundedSender<AcknowledgedMessage<SdamEvent>>,
}

impl SdamEventEmitter {
    /// Emit an SDAM event.
    ///
    /// This method returns a future that can be awaited until the event has been actually emitted.
    /// It is not necessary to await this future.
    pub(crate) fn emit(&self, event: impl Into<SdamEvent>) -> impl Future<Output = ()> {
        let (msg, ack) = AcknowledgedMessage::package(event.into());
        // if event handler has stopped listening, no more events should be emitted,
        // so we can safely ignore any send errors here.
        let _ = self.sender.send(msg);

        ack.wait_for_acknowledgment().map(|_| ())
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
