use std::{
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use bson::{doc, Document};

use super::{
    description::server::ServerDescription,
    state::{server::Server, Topology, WeakTopology},
    ServerUpdate,
    ServerUpdateReceiver,
    TopologyUpdateRequestReceiver,
    TopologyUpdater,
    TopologyWatcher,
};
use crate::{
    cmap::{Connection, Handshaker},
    error::{Error, Result},
    event::sdam::{
        SdamEventHandler,
        ServerHeartbeatFailedEvent,
        ServerHeartbeatStartedEvent,
        ServerHeartbeatSucceededEvent,
    },
    hello::{hello_command, run_hello, HelloReply},
    options::{ClientOptions, ServerAddress},
    runtime,
};

pub(super) const DEFAULT_HEARTBEAT_FREQUENCY: Duration = Duration::from_secs(10);

pub(crate) const MIN_HEARTBEAT_FREQUENCY: Duration = Duration::from_millis(500);

/// Monitor that performs regular heartbeats to determine server status.
pub(crate) struct Monitor {
    address: ServerAddress,
    connection: Option<Connection>,
    handshaker: Handshaker,
    topology_updater: TopologyUpdater,
    topology_watcher: TopologyWatcher,
    update_request_receiver: TopologyUpdateRequestReceiver,
    client_options: ClientOptions,
}

impl Monitor {
    pub(crate) fn start(
        address: ServerAddress,
        topology_updater: TopologyUpdater,
        topology_watcher: TopologyWatcher,
        update_request_receiver: TopologyUpdateRequestReceiver,
        client_options: ClientOptions,
    ) {
        let handshaker = Handshaker::new(Some(client_options.clone().into()));
        let monitor = Self {
            address,
            client_options,
            handshaker,
            topology_updater,
            topology_watcher,
            update_request_receiver,
            connection: None,
        };
        runtime::execute(monitor.execute())
    }

    async fn execute(mut self) {
        println!("{}: starting monitor", self.address);
        let heartbeat_frequency = self
            .client_options
            .heartbeat_freq
            .unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY);

        while self.topology_watcher.is_alive() {
            println!("{}: topology alive, starting check", self.address);
            self.check_server().await;
            println!("{}: check complete", self.address);

            #[cfg(test)]
            let min_frequency = self
                .client_options
                .test_options
                .as_ref()
                .and_then(|to| to.heartbeat_freq)
                .unwrap_or(MIN_HEARTBEAT_FREQUENCY);

            #[cfg(not(test))]
            let min_frequency = MIN_HEARTBEAT_FREQUENCY;

            runtime::delay_for(min_frequency).await;
            self.update_request_receiver
                .wait_for_update_request(heartbeat_frequency - min_frequency)
                .await;
        }
    }

    /// Checks the the server by running a hello command. If an I/O error occurs, the
    /// connection will replaced with a new one.
    ///
    /// Returns true if the topology has changed and false otherwise.
    async fn check_server(&mut self) -> bool {
        self.update_request_receiver.clear_update_requests();
        let mut retried = false;
        let check_result = match self.perform_hello().await {
            Ok(reply) => Ok(reply),
            Err(e) => {
                let previous_description = self.topology_watcher.server_description(&self.address);
                if e.is_network_error()
                    && previous_description
                        .map(|sd| sd.is_available())
                        .unwrap_or(false)
                {
                    self.handle_error(e).await;
                    retried = true;
                    self.perform_hello().await
                } else {
                    Err(e)
                }
            }
        };

        match check_result {
            Ok(reply) => {
                let server_description =
                    ServerDescription::new(self.address.clone(), Some(Ok(reply)));
                self.topology_updater.update(server_description).await
            }
            Err(e) => self.handle_error(e).await || retried,
        }
    }

    async fn perform_hello(&mut self) -> Result<HelloReply> {
        self.emit_event(|handler| {
            let event = ServerHeartbeatStartedEvent {
                server_address: self.address.clone(),
            };
            handler.handle_server_heartbeat_started_event(event);
        });

        let (duration, result) = match self.connection {
            Some(ref mut conn) => {
                let command = hello_command(
                    self.client_options.server_api.as_ref(),
                    self.client_options.load_balanced,
                    Some(conn.stream_description()?.hello_ok),
                );
                let start = Instant::now();
                (
                    start.elapsed(),
                    run_hello(conn, command, None, &self.client_options.sdam_event_handler).await,
                )
            }
            None => {
                let mut connection = Connection::connect_monitoring(
                    self.address.clone(),
                    self.client_options.connect_timeout,
                    self.client_options.tls_options(),
                )
                .await?;

                let start = Instant::now();
                let res = self
                    .handshaker
                    .handshake(
                        &mut connection,
                        None,
                        &self.client_options.sdam_event_handler,
                    )
                    .await
                    .map(|r| r.hello_reply);

                self.connection = Some(connection);
                (start.elapsed(), res)
            }
        };

        match result {
            Ok(ref r) => {
                self.emit_event(|handler| {
                    let mut reply = r
                        .raw_command_response
                        .to_document()
                        .unwrap_or_else(|e| doc! { "deserialization error": e.to_string() });
                    // if this hello call is part of a handshake, remove speculative authentication
                    // information before publishing an event
                    reply.remove("speculativeAuthenticate");
                    let event = ServerHeartbeatSucceededEvent {
                        duration,
                        reply,
                        server_address: self.address.clone(),
                    };
                    handler.handle_server_heartbeat_succeeded_event(event);
                });
            }
            Err(ref e) => {
                self.connection.take();
                self.emit_event(|handler| {
                    let event = ServerHeartbeatFailedEvent {
                        duration,
                        failure: e.clone(),
                        server_address: self.address.clone(),
                    };
                    handler.handle_server_heartbeat_failed_event(event);
                });
            }
        }

        result
    }

    async fn handle_error(&mut self, error: Error) -> bool {
        self.topology_updater
            .handle_monitor_error(self.address.clone(), error)
            .await
    }

    fn emit_event<F>(&self, emit: F)
    where
        F: FnOnce(&Arc<dyn SdamEventHandler>),
    {
        if let Some(ref handler) = self.client_options.sdam_event_handler {
            emit(handler)
        }
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
        todo!()
        // // If the pool encounters an error establishing a connection, it will
        // // notify the update receiver and need to be handled.
        // while let Some(update) = self.update_receiver.recv().await {
        //     let topology = match self.topology.upgrade() {
        //         Some(it) => it,
        //         _ => return,
        //     };
        //     let server = match self.server.upgrade() {
        //         Some(it) => it,
        //         _ => return,
        //     };

        //     // This needs to borrow the message rather than taking it so the update isn't sent
        //     // until after the topology has processed the error.
        //     match update.message() {
        //         ServerUpdate::Error { error } => {
        //             topology
        //                 .handle_application_error(
        //                     error.cause.clone(),
        //                     error.handshake_phase.clone(),
        //                     &server,
        //                 )
        //                 .await;
        //         }
        //     }
        //     drop(update);
        // }
    }
}
