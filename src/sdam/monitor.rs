use std::{sync::Weak, time::Duration};

use bson::doc;
use lazy_static::lazy_static;
use time::PreciseTime;

use super::{
    description::server::{ServerDescription, ServerType},
    state::server::Server,
};
use crate::{
    cmap::{Command, Connection},
    error::Result,
    is_master::IsMasterReply,
    options::{ClientOptions, StreamAddress},
    sdam::update_topology,
};

const DEFAULT_HEARTBEAT_FREQUENCY: Duration = Duration::from_secs(10);

lazy_static! {
    // Unfortunately, the `time` crate has not yet updated to make the `Duration` constructors `const`, so we have to use lazy_static.
    pub(crate) static ref MIN_HEARTBEAT_FREQUENCY: time::Duration = time::Duration::milliseconds(500);
}

pub(super) struct Monitor {
    address: StreamAddress,
    connection: Option<Connection>,
    server: Weak<Server>,
    server_type: ServerType,
    options: ClientOptions,
}

impl Monitor {
    /// Starts a monitoring thread associated with a given Server. A weak reference is used to
    /// ensure that the monitoring thread doesn't keep the server alive after it's been removed
    /// from the topology or the client has been dropped.
    pub(super) fn start(
        address: StreamAddress,
        server: Weak<Server>,
        options: ClientOptions,
    ) -> Result<()> {
        let mut monitor = Self {
            address,
            connection: None,
            server,
            server_type: ServerType::Unknown,
            options,
        };

        std::thread::spawn(move || {
            monitor.execute();
        });

        Ok(())
    }

    fn execute(&mut self) {
        let heartbeat_frequency = self
            .options
            .heartbeat_freq
            .unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY);

        loop {
            self.check_server_and_update_topology();

            let last_check = PreciseTime::now();

            let timed_out = match self.server.upgrade() {
                Some(server) => server.wait_timeout(heartbeat_frequency),
                None => return,
            };

            if !timed_out {
                let duration_since_last_check = last_check.to(PreciseTime::now());

                if duration_since_last_check < *MIN_HEARTBEAT_FREQUENCY {
                    let remaining_time = *MIN_HEARTBEAT_FREQUENCY - duration_since_last_check;

                    // Since MIN_HEARTBEAT_FREQUENCY is 500 and `duration_since_last_check` is
                    // less than it but still positive, we can be sure
                    // that the time::Duration can be successfully
                    // converted to a std::time::Duration. However, in the case of some
                    // bug causing this not to be true, rather than panicking the monitoring
                    // thread, we instead just don't sleep and proceed
                    // to checking the server a bit early.
                    if let Ok(remaining_time) = remaining_time.to_std() {
                        std::thread::sleep(remaining_time);
                    }
                }
            }
        }
    }

    /// Checks the the server by running an `isMaster` command. If an I/O error occurs, the
    /// connection will replaced with a new one.
    fn check_server_and_update_topology(&mut self) {
        // If the server has been dropped, terminate the monitoring thread.
        let server = match self.server.upgrade() {
            Some(server) => server,
            None => return,
        };

        // If the topology has been dropped, terminate the monitoring thread.
        let topology = match server.topology() {
            Some(topology) => topology,
            None => return,
        };

        // Send an isMaster to the server.
        let server_description = self.check_server();
        self.server_type = server_description.server_type;

        update_topology(topology, server_description);
    }

    fn check_server(&mut self) -> ServerDescription {
        let address = self.address.clone();

        match self.perform_is_master() {
            Ok(reply) => ServerDescription::new(address, Some(Ok(reply))),
            Err(e) => {
                self.clear_connection_pool();

                if self.server_type == ServerType::Unknown {
                    return ServerDescription::new(address, Some(Err(e)));
                }

                ServerDescription::new(address, Some(self.perform_is_master()))
            }
        }
    }

    fn perform_is_master(&mut self) -> Result<IsMasterReply> {
        let connection = self.resolve_connection()?;
        let result = is_master(connection);

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

    fn resolve_connection(&mut self) -> Result<&mut Connection> {
        if let Some(ref mut connection) = self.connection {
            return Ok(connection);
        }

        let connection = Connection::new_monitoring(
            self.address.clone(),
            self.options.connect_timeout,
            self.options.tls_options(),
        )?;

        // Since the connection was not `Some` above, this will always insert the new connection and
        // return a reference to it.
        Ok(self.connection.get_or_insert(connection))
    }

    fn clear_connection_pool(&self) {
        if let Some(server) = self.server.upgrade() {
            server.clear_connection_pool();
        }
    }
}

fn is_master(connection: &mut Connection) -> Result<IsMasterReply> {
    let command = Command::new_read(
        "isMaster".into(),
        "admin".into(),
        None,
        doc! { "isMaster": 1 },
    );

    let start_time = PreciseTime::now();
    let command_response = connection.send_command(command, None)?;
    let end_time = PreciseTime::now();

    let command_response = command_response.body()?;

    Ok(IsMasterReply {
        command_response,
        round_trip_time: Some(start_time.to(end_time).to_std().unwrap()),
    })
}
