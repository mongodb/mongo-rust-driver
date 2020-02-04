use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use bson::{bson, doc};
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

/// Starts a monitoring thread associated with a given Server. A weak reference is used to ensure
/// that the monitoring thread doesn't keep the server alive after it's been removed from the
/// topology or the client has been dropped.
pub(super) fn monitor_server(address: StreamAddress, server: Weak<Server>, options: ClientOptions) {
    std::thread::spawn(move || {
        let mut server_type = ServerType::Unknown;
        let heartbeat_frequency = options
            .heartbeat_freq
            .unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY);

        let make_connection = || {
            Connection::new(
                0,
                address.clone(),
                0,
                options.connect_timeout,
                options.tls_options(),
                options.cmap_event_handler.clone(),
            )
            .unwrap()
        };

        let mut conn = make_connection();

        loop {
            server_type =
                match monitor_server_check(&mut conn, server_type, &server, &make_connection) {
                    Some(server_type) => server_type,
                    None => return,
                };

            let last_check = PreciseTime::now();

            let timed_out = match server.upgrade() {
                Some(server) => server.wait_timeout(heartbeat_frequency),
                None => return,
            };

            if !timed_out {
                let duration_since_last_check = last_check.to(PreciseTime::now());

                if duration_since_last_check < *MIN_HEARTBEAT_FREQUENCY {
                    let remaining_time = *MIN_HEARTBEAT_FREQUENCY - duration_since_last_check;

                    // Since MIN_HEARTBEAT_FREQUENCY is 500 and `duration_since_last_check` is less
                    // than it but still positive, we can be sure that the time::Duration can be
                    // successfully converted to a std::time::Duration. However, in the case of some
                    // bug causing this not to be true, rather than panicking the monitoring thread,
                    // we instead just don't sleep and proceed to checking the server a bit early.
                    if let Ok(remaining_time) = remaining_time.to_std() {
                        std::thread::sleep(remaining_time);
                    }
                }
            }
        }
    });
}

fn monitor_server_check(
    conn: &mut Connection,
    mut server_type: ServerType,
    server: &Weak<Server>,
    make_connection: impl Fn() -> Connection,
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
    let server_description = check_server(conn, server_type, &server, make_connection);
    server_type = server_description.server_type;

    update_topology(topology, server_description);

    Some(server_type)
}

fn check_server(
    conn: &mut Connection,
    server_type: ServerType,
    server: &Arc<Server>,
    make_connection: impl Fn() -> Connection,
) -> ServerDescription {
    let address = conn.address().clone();

    match is_master(conn, &make_connection) {
        Ok(reply) => return ServerDescription::new(address, Some(Ok(reply))),
        Err(e) => {
            server.clear_connection_pool();

            if server_type == ServerType::Unknown {
                return ServerDescription::new(address, Some(Err(e)));
            }
        }
    }

    ServerDescription::new(address, Some(is_master(conn, make_connection)))
}

fn is_master(
    conn: &mut Connection,
    make_connection: impl Fn() -> Connection,
) -> Result<IsMasterReply> {
    let result = is_master_inner(conn);

    if result
        .as_ref()
        .err()
        .map(|e| e.kind.is_network_error())
        .unwrap_or(false)
    {
        std::mem::replace(conn, make_connection());
    }

    result
}

fn is_master_inner(conn: &mut Connection) -> Result<IsMasterReply> {
    let command = Command::new_read(
        "isMaster".into(),
        "admin".into(),
        None,
        doc! { "isMaster": 1 },
    );

    let start_time = PreciseTime::now();
    let command_response = conn.send_command(command, None)?;
    let end_time = PreciseTime::now();

    let command_response = command_response.body()?;

    Ok(IsMasterReply {
        command_response,
        // TODO RUST-193: Round-trip time
        round_trip_time: Some(start_time.to(end_time).to_std().unwrap()),
    })
}
