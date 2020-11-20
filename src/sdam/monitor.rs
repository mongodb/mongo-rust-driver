use std::{sync::Weak, time::Duration};

use super::{
    description::server::{ServerDescription, ServerType},
    state::{server::Server, Topology, WeakTopology},
};
use crate::{
    bson::doc,
    client::options::ServerApi,
    cmap::{is_master, Command, Connection, Handshaker},
    error::Result,
    is_master::IsMasterReply,
    options::StreamAddress,
    RUNTIME,
};

pub(super) const DEFAULT_HEARTBEAT_FREQUENCY: Duration = Duration::from_secs(10);

pub(crate) const MIN_HEARTBEAT_FREQUENCY: Duration = Duration::from_millis(500);

pub(super) struct Monitor {
    address: StreamAddress,
    connection: Option<Connection>,
    handshaker: Handshaker,
    server: Weak<Server>,
    server_type: ServerType,
    topology: WeakTopology,
}

impl Monitor {
    /// Starts a monitoring thread associated with a given Server. A weak reference is used to
    /// ensure that the monitoring thread doesn't keep the server alive after it's been removed
    /// from the topology or the client has been dropped.
    pub(super) fn start(address: StreamAddress, server: Weak<Server>, topology: WeakTopology) {
        let handshaker = Handshaker::new(Some(topology.client_options().clone().into()));
        let mut monitor = Self {
            address,
            connection: None,
            server,
            server_type: ServerType::Unknown,
            topology,
            handshaker,
        };

        RUNTIME.execute(async move {
            monitor.execute().await;
        });
    }

    async fn execute(&mut self) {
        let heartbeat_frequency = self
            .topology
            .client_options()
            .heartbeat_freq
            .unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY);

        while self.topology.is_alive() {
            if self.server.upgrade().is_none() {
                break;
            }

            let topology = match self.topology.upgrade() {
                Some(topology) => topology,
                None => break,
            };

            if self.check_if_topology_changed(&topology).await {
                topology.notify_topology_changed();
            }

            let mut topology_check_requests_subscriber =
                topology.subscribe_to_topology_check_requests().await;

            let min_frequency = self
                .topology
                .client_options()
                .heartbeat_freq_test
                .unwrap_or(MIN_HEARTBEAT_FREQUENCY);
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
    async fn check_if_topology_changed(&mut self, topology: &Topology) -> bool {
        // Send an isMaster to the server.
        let server_description = self.check_server().await;
        self.server_type = server_description.server_type;

        topology.update(server_description).await
    }

    async fn check_server(&mut self) -> ServerDescription {
        let address = self.address.clone();

        match self.perform_is_master().await {
            Ok(reply) => ServerDescription::new(address, Some(Ok(reply))),
            Err(e) => {
                self.clear_connection_pool();

                if self.server_type == ServerType::Unknown {
                    return ServerDescription::new(address, Some(Err(e)));
                }

                ServerDescription::new(address, Some(self.perform_is_master().await))
            }
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
                if let Some(ref server_api) = self.topology.client_options().server_api {
                    command.set_server_api(server_api);
                }
                is_master(command, conn).await
            }
            None => {
                let mut connection = Connection::connect_monitoring(
                    self.address.clone(),
                    self.topology.client_options().connect_timeout,
                    self.topology.client_options().tls_options(),
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
            server.clear_connection_pool();
        }
    }
}
