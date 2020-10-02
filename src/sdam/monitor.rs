use std::{sync::Weak, time::Duration};

use time::PreciseTime;

use super::{
    description::server::{ServerDescription, ServerType},
    state::{server::Server, Topology, WeakTopology},
};
use crate::{
    bson::doc,
    cmap::{Command, Connection},
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
    server: Weak<Server>,
    server_type: ServerType,
    topology: WeakTopology,
}

impl Monitor {
    /// Starts a monitoring thread associated with a given Server. A weak reference is used to
    /// ensure that the monitoring thread doesn't keep the server alive after it's been removed
    /// from the topology or the client has been dropped.
    pub(super) fn start(address: StreamAddress, server: Weak<Server>, topology: WeakTopology) {
        let mut monitor = Self {
            address,
            connection: None,
            server,
            server_type: ServerType::Unknown,
            topology,
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
                self.clear_connection_pool().await;

                if self.server_type == ServerType::Unknown {
                    return ServerDescription::new(address, Some(Err(e)));
                }

                ServerDescription::new(address, Some(self.perform_is_master().await))
            }
        }
    }

    async fn perform_is_master(&mut self) -> Result<IsMasterReply> {
        let connection = self.resolve_connection().await?;
        let result = is_master(connection).await;

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

    async fn resolve_connection(&mut self) -> Result<&mut Connection> {
        if let Some(ref mut connection) = self.connection {
            return Ok(connection);
        }

        let connection = Connection::connect_monitoring(
            self.address.clone(),
            self.topology.client_options().connect_timeout,
            self.topology.client_options().tls_options(),
        )
        .await?;

        // Since the connection was not `Some` above, this will always insert the new connection and
        // return a reference to it.
        Ok(self.connection.get_or_insert(connection))
    }

    async fn clear_connection_pool(&self) {
        if let Some(server) = self.server.upgrade() {
            server.clear_connection_pool();
        }
    }
}

async fn is_master(connection: &mut Connection) -> Result<IsMasterReply> {
    let command = Command::new_read(
        "isMaster".into(),
        "admin".into(),
        None,
        doc! { "isMaster": 1 },
    );

    let start_time = PreciseTime::now();
    let command_response = connection.send_command(command, None).await?;
    let end_time = PreciseTime::now();

    command_response.validate()?;
    let is_master_response = command_response.body()?;
    Ok(IsMasterReply {
        command_response: is_master_response,
        round_trip_time: Some(start_time.to(end_time).to_std().unwrap()),
        cluster_time: command_response.cluster_time().cloned(),
    })
}
