use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use super::{
    description::server::ServerDescription,
    state::{server::Server, HandshakePhase, Topology, WeakTopology},
    ServerUpdate,
    ServerUpdateReceiver,
};
use crate::{
    bson::doc,
    cmap::{is_master, Command, Connection, Handshaker},
    error::{Error, Result},
    is_master::IsMasterReply,
    options::{ClientOptions, StreamAddress},
    RUNTIME,
};

pub(super) const DEFAULT_HEARTBEAT_FREQUENCY: Duration = Duration::from_secs(10);

pub(crate) const MIN_HEARTBEAT_FREQUENCY: Duration = Duration::from_millis(500);

pub(crate) struct Monitor {
    address: StreamAddress,
    server: Arc<Server>,
    client_options: ClientOptions,
    update_receiver: ServerUpdateReceiver,
    topology: WeakTopology,
}

impl Monitor {
    /// Creates a monitor associated with a given Server.
    /// This method does not start the monitor. Use `Monitor::start` to do so.
    pub(super) fn new(
        address: StreamAddress,
        server: &Arc<Server>,
        topology: WeakTopology,
        client_options: ClientOptions,
        update_receiver: ServerUpdateReceiver,
    ) -> Self {
        Self {
            address,
            server: server.clone(),
            client_options,
            update_receiver,
            topology,
        }
    }

    /// Start the monitor tasks.
    /// A weak reference is used to ensure that the monitor doesn't keep the topology alive after
    /// it's been removed from the topology or the client has been dropped.
    pub(super) fn start(self) {
        let mut heartbeat_monitor = HeartbeatMonitor::new(
            self.address,
            Arc::downgrade(&self.server),
            self.topology.clone(),
            self.client_options,
        );
        RUNTIME.execute(async move {
            heartbeat_monitor.execute().await;
        });

        let update_monitor = UpdateMonitor {
            server: Arc::downgrade(&self.server),
            topology: self.topology,
            update_receiver: self.update_receiver,
        };
        RUNTIME.execute(async move {
            update_monitor.execute().await;
        });
    }
}

/// Monitor that performs regular heartbeats to determine server status.
struct HeartbeatMonitor {
    address: StreamAddress,
    connection: Option<Connection>,
    handshaker: Handshaker,
    server: Weak<Server>,
    topology: WeakTopology,
    client_options: ClientOptions,
}

impl HeartbeatMonitor {
    fn new(
        address: StreamAddress,
        server: Weak<Server>,
        topology: WeakTopology,
        client_options: ClientOptions,
    ) -> Self {
        let handshaker = Handshaker::new(Some(client_options.clone().into()));
        Self {
            address,
            server,
            client_options,
            handshaker,
            topology,
            connection: None,
        }
    }

    async fn execute(&mut self) {
        let heartbeat_frequency = self
            .client_options
            .heartbeat_freq
            .unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY);

        while self.topology.is_alive() {
            let server = match self.server.upgrade() {
                Some(server) => server,
                None => break,
            };

            let topology = match self.topology.upgrade() {
                Some(topology) => topology,
                None => break,
            };

            if self.check_server(&topology, &server).await {
                topology.notify_topology_changed();
            }

            let mut topology_check_requests_subscriber =
                topology.subscribe_to_topology_check_requests();

            // drop strong reference to topology before going back to sleep in case it drops off
            // in between checks.
            drop(topology);
            drop(server);

            #[cfg(test)]
            let min_frequency = self
                .client_options
                .heartbeat_freq_test
                .unwrap_or(MIN_HEARTBEAT_FREQUENCY);

            #[cfg(not(test))]
            let min_frequency = MIN_HEARTBEAT_FREQUENCY;

            RUNTIME.delay_for(min_frequency).await;
            topology_check_requests_subscriber
                .wait_for_message(heartbeat_frequency - min_frequency)
                .await;
        }
    }

    /// Checks the the server by running an `isMaster` command. If an I/O error occurs, the
    /// connection will replaced with a new one.
    ///
    /// Returns true if the topology has changed and false otherwise.
    async fn check_server(&mut self, topology: &Topology, server: &Server) -> bool {
        let mut retried = false;
        let check_result = match self.perform_is_master().await {
            Ok(reply) => Ok(reply),
            Err(e) => {
                let previous_description = topology.get_server_description(&server.address).await;
                if e.is_network_error()
                    && previous_description
                        .map(|sd| sd.is_available())
                        .unwrap_or(false)
                {
                    self.handle_error(e, topology, server).await;
                    retried = true;
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
                topology.update(server, server_description).await
            }
            Err(e) => self.handle_error(e, topology, server).await || retried,
        }
    }

    async fn perform_is_master(&mut self) -> Result<IsMasterReply> {
        let result = match self.connection {
            Some(ref mut conn) => {
                let mut command =
                    Command::new("isMaster".into(), "admin".into(), doc! { "isMaster": 1 });
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

        if result.is_err() {
            self.connection.take();
        }

        result
    }

    async fn handle_error(&mut self, error: Error, topology: &Topology, server: &Server) -> bool {
        topology.handle_monitor_error(error, server).await
    }
}

/// Monitor that listens for updates to a given server generated from operation execution.
struct UpdateMonitor {
    server: Weak<Server>,
    topology: WeakTopology,
    update_receiver: ServerUpdateReceiver,
}

impl UpdateMonitor {
    async fn execute(mut self) {
        // If the pool encounters an error establishing a connection, it will
        // notify the update receiver and need to be handled.
        while let Some(update) = self.update_receiver.recv().await {
            let topology = match self.topology.upgrade() {
                Some(it) => it,
                _ => return,
            };
            let server = match self.server.upgrade() {
                Some(it) => it,
                _ => return,
            };

            match update.into_message() {
                ServerUpdate::Error {
                    error,
                    error_generation,
                } => {
                    topology
                        .handle_application_error(
                            error,
                            HandshakePhase::BeforeCompletion {
                                generation: error_generation,
                            },
                            &server,
                        )
                        .await;
                }
            }
        }
    }
}
