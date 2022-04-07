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
    ServerType,
    TopologyType,
};

use super::{HMonitor, HandshakePhase, Server, ServerDescription, Topology, TopologyDescription};

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
            HMonitor::start(
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

    pub(crate) fn request_update(&self) {
        self.update_requester.request()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TopologyState {
    pub(crate) description: TopologyDescription,
    pub(crate) servers: HashMap<ServerAddress, Arc<Server>>,
}

pub(crate) enum UpdateMessage {
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
    update_request_receiver: TopologyUpdateRequestReceiver,
}

impl TopologyWorker {
    fn start(mut self) {
        runtime::execute(async move {
            loop {
                tokio::select! {
                    Some(update) = self.update_receiver.recv() => {
                        match update {
                            UpdateMessage::ServerUpdate(sd) => {
                                println!("received update for {}", sd.address);
                                self.update_server(sd).await.unwrap();
                            },
                            UpdateMessage::MonitorError { address, error } => {
                                todo!()
                            },
                            UpdateMessage::ApplicationError { address, error, phase } => {
                                todo!()
                            }
                        }
                    },
                }
            }
        });
    }

    async fn update_server(&mut self, sd: ServerDescription) -> std::result::Result<bool, String> {
        let server_type = sd.server_type;
        let server_address = sd.address.clone();

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
            let server = Server::new(
                address.clone(),
                self.options.clone(),
                self.http_client.clone(),
                self.topology_updater.clone(),
            );
            latest_state.servers.insert(address.clone(), server);
            HMonitor::start(
                address,
                self.topology_updater.clone(),
                self.topology_watcher.clone(),
                self.update_request_receiver.clone(),
                self.options.clone(),
            );
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

        if topology_changed {
            if server_type.is_data_bearing()
                || (server_type != ServerType::Unknown
                    && latest_state.description.topology_type() == TopologyType::Single)
            {
                if let Some(s) = latest_state.servers.get(&server_address) {
                    s.pool.mark_as_ready().await;
                }
            }
            let _ = self.broadcaster.state_sender.send(latest_state);
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
}

struct TopologyBroadcaster {
    state_sender: watch::Sender<TopologyState>,
}

impl TopologyBroadcaster {
    fn clone_latest(&self) -> TopologyState {
        self.state_sender.borrow().clone()
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

#[tokio::test]
async fn foo() {
    let mut top = NewTopology::new(crate::test::CLIENT_OPTIONS.clone()).unwrap();

    runtime::timeout(
        Duration::from_secs(5),
        runtime::spawn(async move {
            while top.watcher.receiver.changed().await.is_ok() {
                let state = top.watcher.receiver.borrow();
                println!("state change");
                println!("description: {:#?}", state.description);
                println!("servers: {:#?}", state.servers);
            }
        }),
    )
    .await;
}
