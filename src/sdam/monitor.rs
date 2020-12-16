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
    error::{Error, Result},
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
    /// Creates a monitor associated with a given Server.
    /// This method does not start the monitor. Use `Monitor::start` to do so.
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

    /// Start the monitor task.
    /// A weak reference is used to ensure that the monitor doesn't keep the server alive after it's
    /// been removed from the topology or the client has been dropped.
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
            let server = match self.server.upgrade() {
                Some(server) => server,
                None => break,
            };

            let topology = match weak_topology.upgrade() {
                Some(topology) => topology,
                None => break,
            };

            if self.check_server(&topology, &server).await {
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
            drop(server);

            loop {
                tokio::select! {
                    _ = &mut wait_for_next_check => { break; },

                    // If the pool encounters an error establishing a connection, it will
                    // notify the update receiver and need to be handled.
                    Some(update) = self.update_receiver.recv() => {
                        if let Some(topology) = weak_topology.upgrade() {
                            if let Some(server) = self.server.upgrade() {
                                match update.message() {
                                    ServerUpdate::Error { error, error_generation } => {
                                        if *error_generation == self.generation_subscriber.generation() {
                                            topology.handle_pre_handshake_error(error.clone(), &server).await;
                                        }
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
    async fn check_server(&mut self, topology: &Topology, server: &Server) -> bool {
        let check_result = match self.perform_is_master().await {
            Ok(reply) => Ok(reply),
            Err(e) => {
                self.handle_error(e.clone(), topology, server).await;

                if e.is_network_error() && self.server_type != ServerType::Unknown {
                    self.perform_is_master().await
                } else {
                    Err(e)
                }
            }
        };

        match check_result {
            Ok(reply) => {
                let server_description =
                    ServerDescription::new(server.address.clone(), Some(Ok(reply)));
                self.server_type = server_description.server_type;

                if server_description.server_type != ServerType::Unknown {
                    self.ready_connection_pool().await;
                }

                topology.update(server_description).await
            }
            Err(e) => self.handle_error(e, topology, server).await,
        }
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

    async fn ready_connection_pool(&self) {
        if let Some(server) = self.server.upgrade() {
            server.pool.mark_as_ready().await;
        }
    }

    async fn handle_error(&mut self, error: Error, topology: &Topology, server: &Server) -> bool {
        topology.handle_pre_handshake_error(error, server).await;
        std::mem::replace(&mut self.server_type, ServerType::Unknown) != ServerType::Unknown
    }
}
