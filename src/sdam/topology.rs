use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use bson::oid::ObjectId;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    watch,
};

use crate::{
    client::options::{ClientOptions, ServerAddress},
    error::{Error, Result},
    event::sdam::{
        ServerClosedEvent,
        ServerDescriptionChangedEvent,
        ServerOpeningEvent,
        TopologyDescriptionChangedEvent,
    },
    runtime,
    runtime::HttpClient,
    ServerInfo,
};

use super::{HMonitor, HandshakePhase, Server, ServerDescription, TopologyDescription};

pub(crate) struct NewTopology {
    watcher: TopologyWatcher,
    updater: TopologyUpdater,
}

impl NewTopology {
    pub(crate) fn new(options: ClientOptions) -> Result<NewTopology> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<UpdateMessage>();

        let state = TopologyState {
            description: TopologyDescription::new(options.clone())?,
            servers: HashMap::new(),
        };
        // let (watch_sender, watch_receiver) = tokio::sync::watch::channel(state);

        let (watcher, broadcaster) = TopologyWatcher::channel(state);

        let (updater, update_receiver) = TopologyUpdater::channel();

        TopologyWorker {
            id: ObjectId::new(),
            update_receiver,
            broadcaster,
            options,
            http_client: HttpClient::default(),

            topology_watcher: watcher.clone(),
            topology_updater: updater.clone(),
        }
        .start();

        Ok(NewTopology { watcher, updater })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TopologyState {
    description: TopologyDescription,
    servers: HashMap<ServerAddress, Arc<Server>>,
}
impl TopologyState {
    // /// Adds a new server to the cluster.
    // ///
    // /// A reference to the containing Topology is needed in order to start the monitoring task.
    // fn add_new_server(
    //     &mut self,
    //     address: ServerAddress,
    //     options: ClientOptions,
    // ) {
    //     if self.servers.contains_key(&address) {
    //         return;
    //     }

    //     let (server, monitor) = Server::create(
    //         address.clone(),
    //         &options,
    //         topology.clone(),
    //         self.http_client.clone(),
    //     );
    //     self.servers.insert(address, server);

    //     #[cfg(test)]
    //     if options
    //         .test_options
    //         .map(|to| to.disable_monitoring_threads)
    //         .unwrap_or(false)
    //     {
    //         return;
    //     }

    //     monitor.start();
    // }

    // /// Updates the given `command` as needed based on the `criteria`.
    // pub(crate) fn update_command_with_read_pref<T>(
    //     &self,
    //     server_address: &ServerAddress,
    //     command: &mut Command<T>,
    //     criteria: Option<&SelectionCriteria>,
    // ) {
    //     let server_type = self
    //         .description
    //         .get_server_description(server_address)
    //         .map(|desc| desc.server_type)
    //         .unwrap_or(ServerType::Unknown);

    //     self.description
    //         .update_command_with_read_pref(server_type, command, criteria)
    // }

    // /// Update the topology description based on the provided server description. Also add new
    // /// servers and remove missing ones as needed.
    // fn update(
    //     &mut self,
    //     server: ServerDescription,
    //     options: &ClientOptions,
    //     topology: WeakTopology,
    // ) -> std::result::Result<bool, String> {
    //     let old_description = self.description.clone();
    //     self.description.update(server)?;

    //     let hosts: HashSet<_> = self.description.server_addresses().cloned().collect();
    //     self.sync_hosts(&hosts, options, &topology);

    //     let diff = old_description.diff(&self.description);
    //     let topology_changed = diff.is_some();

    //     if let Some(ref handler) = options.sdam_event_handler {
    //         if let Some(diff) = diff {
    //             for (address, (previous_description, new_description)) in diff.changed_servers {
    //                 let event = ServerDescriptionChangedEvent {
    //                     address: address.clone(),
    //                     topology_id: topology.common.id,
    //                     previous_description:
    // ServerInfo::new_owned(previous_description.clone()),                     new_description:
    // ServerInfo::new_owned(new_description.clone()),                 };
    //                 handler.handle_server_description_changed_event(event);
    //             }

    //             for address in diff.removed_addresses {
    //                 let event = ServerClosedEvent {
    //                     address: address.clone(),
    //                     topology_id: topology.common.id,
    //                 };
    //                 handler.handle_server_closed_event(event);
    //             }

    //             for address in diff.added_addresses {
    //                 let event = ServerOpeningEvent {
    //                     address: address.clone(),
    //                     topology_id: topology.common.id,
    //                 };
    //                 handler.handle_server_opening_event(event);
    //             }

    //             let event = TopologyDescriptionChangedEvent {
    //                 topology_id: topology.common.id,
    //                 previous_description: old_description.clone().into(),
    //                 new_description: self.description.clone().into(),
    //             };
    //             handler.handle_topology_description_changed_event(event);
    //         }
    //     }

    //     Ok(topology_changed)
    // }

    // /// Start/stop monitoring tasks and create/destroy connection pools based on the new and
    // /// removed servers in the topology description.
    // fn update_hosts(
    //     &mut self,
    //     hosts: &HashSet<ServerAddress>,
    //     options: &ClientOptions,
    //     topology: WeakTopology,
    // ) {
    //     self.description.sync_hosts(hosts);
    //     self.sync_hosts(hosts, options, &topology);
    // }

    // fn sync_hosts(
    //     &mut self,
    //     hosts: &HashSet<ServerAddress>,
    //     options: &ClientOptions,
    //     topology: &WeakTopology,
    // ) {
    //     for address in hosts.iter() {
    //         self.add_new_server(address.clone(), options.clone(), topology);
    //     }

    //     self.servers.retain(|host, _| hosts.contains(host));
    // }

    // #[cfg(test)]
    // async fn sync_workers(&self) {
    //     let rxen: FuturesUnordered<_> = self
    //         .servers
    //         .values()
    //         .map(|v| v.pool.sync_worker())
    //         .collect();
    //     let _: Vec<_> = rxen.collect().await;
    // }
}

enum UpdateMessage {
    ServerUpdate(ServerDescription),
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
}

impl TopologyWorker {
    fn start(mut self) {
        runtime::execute(async move {
            loop {
                tokio::select! {
                    Some(update) = self.update_receiver.recv() => {
                        match update {
                            UpdateMessage::ServerUpdate(sd) => {
                                self.update_server(sd);
                            },
                            UpdateMessage::MonitorError { address, error } => {
                                todo!()
                            },
                            UpdateMessage::ApplicationError { address, error, phase } => {
                                todo!()
                            }
                        }
                    },
                    Some(_) = self.broadcaster.receive_check_request() => {
                        self.update_receiver.request_monitor_checks();
                    }
                }
            }
        });
    }

    fn update_server(&mut self, sd: ServerDescription) -> std::result::Result<bool, String> {
        let mut latest_state = self.broadcaster.clone_latest();
        let old_description = latest_state.description.clone();

        latest_state.description.update(sd)?;

        let hosts: HashSet<_> = latest_state
            .description
            .server_addresses()
            .cloned()
            .collect();

        latest_state.servers.retain(|host, _| hosts.contains(host));

        for address in hosts {
            if latest_state.servers.contains_key(&address) {
                continue;
            }
            // TODO: create server, add to servers, start monitor
            let server = Server::new(
                address.clone(),
                self.options.clone(),
                self.http_client.clone(),
            );
            latest_state.servers.insert(address.clone(), server);
            let mut monitor = HMonitor::new(
                address,
                self.topology_updater.clone(),
                self.topology_watcher.clone(),
                self.options.clone(),
            );
            runtime::execute(async move { monitor.execute().await });
        }

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

        Ok(topology_changed)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TopologyUpdater {
    sender: UnboundedSender<UpdateMessage>,
    update_request_receiver: watch::Receiver<()>,
}

impl TopologyUpdater {
    fn channel() -> (TopologyUpdater, TopologyUpdateReceiver) {
        todo!()
    }

    pub(crate) fn handle_monitor_error(&self, address: ServerAddress, error: Error) {
        let _: std::result::Result<_, _> = self
            .sender
            .send(UpdateMessage::MonitorError { address, error });
    }

    pub(crate) fn update(&self, sd: ServerDescription) {
        let _: std::result::Result<_, _> = self.sender.send(UpdateMessage::ServerUpdate(sd));
    }

    pub(crate) fn clear_update_requests(&mut self) {
        self.update_request_receiver.borrow_and_update();
    }

    pub(crate) async fn wait_for_update_request(&mut self, timeout: Duration) {
        let _: std::result::Result<_, _> =
            runtime::timeout(timeout, self.update_request_receiver.changed()).await;
    }
}

struct TopologyUpdateReceiver {
    update_receiver: UnboundedReceiver<UpdateMessage>,
    check_request_sender: watch::Sender<()>,
}

impl TopologyUpdateReceiver {
    async fn recv(&mut self) -> Option<UpdateMessage> {
        self.update_receiver.recv().await
    }

    fn request_monitor_checks(&mut self) {
        let _: std::result::Result<_, _> = self.check_request_sender.send(());
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TopologyWatcher {
    receiver: watch::Receiver<TopologyState>,
}

impl TopologyWatcher {
    fn channel(initial_state: TopologyState) -> (TopologyWatcher, TopologyBroadcaster) {
        todo!()
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.receiver.has_changed().is_err()
    }

    pub(crate) fn server_description(&self, address: &ServerAddress) -> Option<ServerDescription> {
        self.receiver
            .borrow()
            .description
            .get_server_description(address)
            .cloned()
    }
}

struct TopologyBroadcaster {
    state_sender: watch::Sender<TopologyState>,
}

impl TopologyBroadcaster {
    async fn receive_check_request(&mut self) -> Option<()> {
        todo!()
    }

    fn clone_latest(&self) -> TopologyState {
        self.state_sender.borrow().clone()
    }
}
