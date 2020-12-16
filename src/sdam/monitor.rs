use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use super::{
    description::server::{ServerDescription, ServerType},
    state::{server::Server, Topology, WeakTopology},
    ServerUpdate,
    ServerUpdateReceiver,
};
use crate::{
    bson::doc,
    cmap::{is_master, Command, Connection, Handshaker, PoolGenerationSubscriber},
    error::Result,
    is_master::IsMasterReply,
    options::{ClientOptions, StreamAddress},
    RUNTIME,
};

pub(super) const DEFAULT_HEARTBEAT_FREQUENCY: Duration = Duration::from_secs(10);

pub(crate) const MIN_HEARTBEAT_FREQUENCY: Duration = Duration::from_millis(500);

pub(crate) struct Monitor {
    address: StreamAddress,
    connection: Option<Connection>,
    handshaker: Handshaker,
    server: Weak<Server>,
    server_type: ServerType,
    generation_subscriber: PoolGenerationSubscriber,
    client_options: ClientOptions,
    update_receiver: ServerUpdateReceiver,
}

impl Monitor {
    /// Starts a monitoring thread associated with a given Server. A weak reference is used to
    /// ensure that the monitoring thread doesn't keep the server alive after it's been removed
    /// from the topology or the client has been dropped.

    pub(super) fn new(
        address: StreamAddress,
        server: &Arc<Server>,
        client_options: ClientOptions,
        update_receiver: ServerUpdateReceiver,
    ) -> Self {
        let handshaker = Handshaker::new(Some(client_options.clone().into()));

        Self {
            address,
            connection: None,
            server: Arc::downgrade(server),
            server_type: ServerType::Unknown,
            generation_subscriber: server.pool.subscribe_to_generation_updates(),
            handshaker,
            client_options,
            update_receiver,
        }
    }

    // pub(super) fn start(address: StreamAddress, server: &Arc<Server>, topology: WeakTopology) {
    //     let handshaker = Handshaker::new(Some(topology.client_options().clone().into()));
    //     let mut monitor = Self {
    //         address,
    //         connection: None,
    //         server: Arc::downgrade(server),
    //         server_type: ServerType::Unknown,
    //         generation_subscriber: server.pool.subscribe_to_generation_updates(),
    //         topology,
    //         handshaker,
    //     };

    //     RUNTIME.execute(async move {
    //         monitor.execute().await;
    //     });
    // }

    pub(super) fn start(mut self, topology: WeakTopology) {
        RUNTIME.execute(async move {
            self.execute(topology).await;
        });
    }

    async fn execute(&mut self, weak_topology: WeakTopology) {
        let heartbeat_frequency = self
            .client_options
            .heartbeat_freq
            .unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY);

        while weak_topology.is_alive() {
            if self.server.upgrade().is_none() {
                break;
            }

            let topology = match weak_topology.upgrade() {
                Some(topology) => topology,
                None => break,
            };

            if self.check_server(&topology).await {
                topology.notify_topology_changed();
            }

            let mut topology_check_requests_subscriber =
                topology.subscribe_to_topology_check_requests().await;

            let min_frequency = self
                .client_options
                .heartbeat_freq_test
                .unwrap_or(MIN_HEARTBEAT_FREQUENCY);
            let mut wait_for_next_check = Box::pin(async move {
                RUNTIME.delay_for(min_frequency).await;
                topology_check_requests_subscriber
                    .wait_for_message(heartbeat_frequency - min_frequency)
                    .await;
            });

            // drop strong reference to topology before going back to sleep in case it drops off
            // in between checks.
            drop(topology);

            loop {
                tokio::select! {
                    _ = &mut wait_for_next_check => { break; },

                    // If the pool encounters an error establishing a connection, it will
                    // notify the update receiver and need to be handled.
                    Some(update) = self.update_receiver.recv() => {
                        println!("got update {:?}", update);
                        if let Some(topology) = weak_topology.upgrade() {
                            match update.message() {
                                ServerUpdate::Error { error, error_generation } => {
                                    println!("got error checking generation");
                                    if *error_generation == self.generation_subscriber.generation() {
                                        println!("generation equal, updating topology and clearing pool");
                                        topology.handle_pre_handshake_error(error.clone(), self.address.clone()).await;
                                        self.clear_connection_pool();
                                    }
                                }
                            }
                        }
                    }
                };
            }
        }
    }

    /// Checks the the server by running an `isMaster` command. If an I/O error occurs, the
    /// connection will replaced with a new one.
    ///
    /// Returns true if the topology has changed and false otherwise.
    async fn check_server(&mut self, topology: &Topology) -> bool {
        let check_result = self
            .perform_is_master()
            .await
            .map(|reply| ServerDescription::new(self.address.clone(), Some(Ok(reply))));

        let mut previous_description = topology
            .get_server_description(&self.address)
            .await
            .unwrap_or_else(|| ServerDescription::new(self.address.clone(), None));

        let server_description = match check_result {
            Ok(desc) => desc,
            Err(e) => {
                let new_desc = ServerDescription::new(self.address.clone(), Some(Err(e.clone())));
                self.clear_connection_pool();

                if e.is_network_error() && previous_description.server_type != ServerType::Unknown {
                    previous_description = new_desc;
                    ServerDescription::new(
                        self.address.clone(),
                        Some(self.perform_is_master().await),
                    )
                } else {
                    new_desc
                }
            }
        };
        if previous_description.server_type == ServerType::Unknown
            && server_description.server_type != ServerType::Unknown
        {
            self.ready_connection_pool().await;
        }

        self.server_type = server_description.server_type;

        topology.update(server_description).await
    }

    async fn perform_is_master(&mut self) -> Result<IsMasterReply> {
        let result = match self.connection {
            Some(ref mut conn) => {
                let mut command = Command::new_read(
                    "isMaster".into(),
                    "admin".into(),
                    None,
                    doc! { "isMaster": 1 },
                );
                if let Some(ref server_api) = self.client_options.server_api {
                    command.set_server_api(server_api);
                }
                is_master(command, conn).await
            }
            None => {
                let mut connection = Connection::connect_monitoring(
                    self.address.clone(),
                    self.client_options.connect_timeout,
                    self.client_options.tls_options(),
                )
                .await?;

                let res = self
                    .handshaker
                    .handshake(&mut connection)
                    .await
                    .map(|r| r.is_master_reply);
                self.connection = Some(connection);
                res
            }
        };

        if result
            .as_ref()
            .err()
            .map(|e| e.kind.is_network_error())
            .unwrap_or(false)
        {
            self.connection.take();
        }

        result
    }

    fn clear_connection_pool(&self) {
        if let Some(server) = self.server.upgrade() {
            server.pool.clear();
        }
    }

    async fn ready_connection_pool(&self) {
        if let Some(server) = self.server.upgrade() {
            server.pool.mark_as_ready().await;
        }
    }
}
