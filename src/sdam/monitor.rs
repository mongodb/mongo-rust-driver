use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use bson::{bson, doc};
use time::PreciseTime;

use super::{
    description::server::{ServerDescription, ServerType},
    state::server::Server,
};
use crate::{
    cmap::{Command, Connection},
    error::Result,
    is_master::IsMasterReply,
    sdam::update_topology,
};

const DEFAULT_HEARTBEAT_FREQUENCY: Duration = Duration::from_secs(10);

/// Starts a monitoring thread associated with a given Server. A weak reference is used to ensure
/// that the monitoring thread doesn't keep the server alive after it's been removed from the
/// topology or the client has been dropped.
pub(super) fn monitor_server(
    mut conn: Connection,
    server: Weak<Server>,
    heartbeat_frequency: Option<Duration>,
) {
    std::thread::spawn(move || {
        let mut server_type = ServerType::Unknown;
        let heartbeat_frequency = heartbeat_frequency.unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY);

        loop {
            server_type = match monitor_server_check(&mut conn, server_type, &server) {
                Some(server_type) => server_type,
                None => return,
            };

            match server.upgrade() {
                Some(server) => server.wait_timeout(heartbeat_frequency),
                None => return,
            }
        }
    });
}

fn monitor_server_check(
    conn: &mut Connection,
    mut server_type: ServerType,
    server: &Weak<Server>,
) -> Option<ServerType> {
    // If the server has been dropped, terminate the monitoring thread.
    let server = match server.upgrade() {
        Some(server) => server,
        None => return None,
    };

    // If the topology has been dropped, terminate the monitoring thread.
    let topology = match server.topology() {
        Some(topology) => topology,
        None => return None,
    };

    // Send an isMaster to the server.
    let server_description = check_server(conn, server_type, &server);
    server_type = server_description.server_type;

    update_topology(topology, server_description);

    Some(server_type)
}

fn check_server(
    conn: &mut Connection,
    server_type: ServerType,
    server: &Arc<Server>,
) -> ServerDescription {
    let address = conn.address().clone();

    match is_master(conn) {
        Ok(reply) => return ServerDescription::new(address, Some(Ok(reply))),
        Err(e) => {
            server.clear_connection_pool();

            if server_type == ServerType::Unknown {
                return ServerDescription::new(address, Some(Err(e)));
            }
        }
    }

    ServerDescription::new(address, Some(is_master(conn)))
}

fn is_master(conn: &mut Connection) -> Result<IsMasterReply> {
    let command = Command::new_read(
        "isMaster".into(),
        "admin".into(),
        None,
        doc! { "isMaster": 1 },
    );

    let start_time = PreciseTime::now();
    let command_response = conn.send_command(command)?;
    let end_time = PreciseTime::now();

    let command_response = command_response.body()?;

    Ok(IsMasterReply {
        command_response,
        // TODO RUST-193: Round-trip time
        round_trip_time: Some(start_time.to(end_time).to_std().unwrap()),
    })
}
