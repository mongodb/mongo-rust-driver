use std::time::{Duration, Instant};

use bson::doc;
use tokio::sync::watch;

use super::{
    description::server::{ServerDescription, TopologyVersion},
    topology::SdamEventEmitter,
    TopologyCheckRequestReceiver,
    TopologyUpdater,
    TopologyWatcher,
};
use crate::{
    cmap::{Connection, Handshaker},
    error::{Error, Result},
    event::sdam::{
        SdamEvent,
        ServerHeartbeatFailedEvent,
        ServerHeartbeatStartedEvent,
        ServerHeartbeatSucceededEvent,
    },
    hello::{hello_command, run_hello, AwaitableHelloOptions, HelloReply},
    options::{ClientOptions, ServerAddress},
    runtime::{self, WorkerHandleListener},
};

pub(crate) const DEFAULT_HEARTBEAT_FREQUENCY: Duration = Duration::from_secs(10);
pub(crate) const MIN_HEARTBEAT_FREQUENCY: Duration = Duration::from_millis(500);

/// Monitor that performs regular heartbeats to determine server status.
pub(crate) struct Monitor {
    address: ServerAddress,
    connection: Option<Connection>,
    handshaker: Handshaker,
    topology_updater: TopologyUpdater,
    topology_watcher: TopologyWatcher,
    sdam_event_emitter: Option<SdamEventEmitter>,
    update_request_receiver: TopologyCheckRequestReceiver,
    client_options: ClientOptions,

    /// The most recent topology version returned by the server in a hello response.
    /// If some, indicates that this monitor should use the streaming protocol. If none, it should
    /// use the polling protocol.
    topology_version: Option<TopologyVersion>,

    /// Handle to the RTT monitor, used to get the latest known round trip time for a given server.
    rtt_monitor_handle: watch::Receiver<RttInfo>,

    /// Handle to the `Server` instance in the `Topology`. This is used to detect when a server has
    /// been removed from the topology and no longer needs to be monitored.
    server_handle_listener: WorkerHandleListener,
}

impl Monitor {
    pub(crate) fn start(
        address: ServerAddress,
        topology_updater: TopologyUpdater,
        topology_watcher: TopologyWatcher,
        sdam_event_emitter: Option<SdamEventEmitter>,
        update_request_receiver: TopologyCheckRequestReceiver,
        handle_listener: WorkerHandleListener,
        client_options: ClientOptions,
    ) {
        let handshaker = Handshaker::new(Some(client_options.clone().into()));

        let (rtt_monitor, rtt_monitor_handle) = RttMonitor::new(
            address.clone(),
            topology_watcher.clone(),
            handshaker.clone(),
            client_options.clone(),
        );
        let monitor = Self {
            address,
            client_options,
            handshaker,
            topology_updater,
            topology_watcher,
            sdam_event_emitter,
            update_request_receiver,
            rtt_monitor_handle,
            server_handle_listener: handle_listener,
            connection: None,
            topology_version: None,
        };

        runtime::execute(monitor.execute());
        runtime::execute(rtt_monitor.execute());
    }

    async fn execute(mut self) {
        let heartbeat_frequency = self
            .client_options
            .heartbeat_freq
            .unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY);

        while self.server_handle_listener.check_if_alive() {
            let check_succeeded = self.check_server().await;

            // In the streaming protocol, we read from the socket continuously
            // rather than polling at specific intervals, unless the most recent check
            // failed.
            //
            // We only go to sleep when using the polling protocol (i.e. server never returned a
            // topologyVersion) or when the most recent check failed.
            if self.topology_version.is_none() || !check_succeeded {
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
                    .wait_for_check_request(heartbeat_frequency - min_frequency)
                    .await;
            }
        }
    }

    /// Checks the the server by running a hello command. If an I/O error occurs, the
    /// connection will replaced with a new one.
    ///
    /// Returns whether the check succeeded or not.
    async fn check_server(&mut self) -> bool {
        let check_result = match self.perform_hello().await {
            HelloResult::Err(e) => {
                let previous_description = self.topology_watcher.server_description(&self.address);
                if e.is_network_error()
                    && previous_description
                        .map(|sd| sd.is_available())
                        .unwrap_or(false)
                {
                    self.handle_error(e).await;
                    self.perform_hello().await
                } else {
                    HelloResult::Err(e)
                }
            }
            other => other,
        };

        // Per the server monitoring spec, we ignore any check requests that came in while we were
        // performing a check.
        self.update_request_receiver.clear_all_requests();

        match check_result {
            HelloResult::Ok(reply) => {
                let server_description = ServerDescription::new(
                    self.address.clone(),
                    Some(Ok(reply)),
                    self.rtt_monitor_handle.borrow().average,
                );
                self.topology_updater.update(server_description).await;
                true
            }
            HelloResult::Err(e) => {
                self.handle_error(e).await;
                false
            }
            HelloResult::Cancelled { .. } => false,
        }
    }

    async fn perform_hello(&mut self) -> HelloResult {
        self.emit_event(|| {
            SdamEvent::ServerHeartbeatStarted(ServerHeartbeatStartedEvent {
                server_address: self.address.clone(),
            })
        })
        .await;

        let start = Instant::now();
        let execute_hello = async {
            match self.connection {
                Some(ref mut conn) => {
                    // If the server indicated there was moreToCome, just read from the socket.
                    if conn.is_streaming() {
                        conn.receive_message()
                            .await
                            .and_then(|r| r.into_hello_reply(None))
                    // Otherwise, send a regular hello command.
                    } else {
                        let heartbeat_frequency = self
                            .client_options
                            .heartbeat_freq
                            .unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY);

                        // If the initial handshake returned a topology version, send it back to the
                        // server to begin streaming responses.
                        let opts = self.topology_version.map(|tv| AwaitableHelloOptions {
                            topology_version: tv,
                            max_await_time: heartbeat_frequency,
                        });

                        let command = hello_command(
                            self.client_options.server_api.as_ref(),
                            self.client_options.load_balanced,
                            Some(conn.stream_description()?.hello_ok),
                            opts,
                        );

                        run_hello(conn, command).await
                    }
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
                        .map(|r| r.hello_reply);

                    self.connection = Some(connection);

                    res
                }
            }
        };

        // Begin executing the hello, listening for a cancellation request.
        let result = tokio::select! {
            result = execute_hello => match result {
                Ok(reply) => HelloResult::Ok(reply),
                Err(e) => HelloResult::Err(e)
            },
            Some(err) = self.update_request_receiver.listen_for_cancellation() => {
                HelloResult::Cancelled { reason: err }
            }
        };
        let duration = start.elapsed();

        match result {
            HelloResult::Ok(ref r) => {
                self.emit_event(|| {
                    let mut reply = r
                        .raw_command_response
                        .to_document()
                        .unwrap_or_else(|e| doc! { "deserialization error": e.to_string() });
                    // if this hello call is part of a handshake, remove speculative authentication
                    // information before publishing an event
                    reply.remove("speculativeAuthenticate");
                    SdamEvent::ServerHeartbeatSucceeded(ServerHeartbeatSucceededEvent {
                        duration,
                        reply,
                        server_address: self.address.clone(),
                    })
                })
                .await;

                // If the response included a topology version, cache it so that we can return it in
                // the next hello.
                self.topology_version = r.command_response.topology_version;
            }
            HelloResult::Err(ref e) | HelloResult::Cancelled { reason: ref e } => {
                // Per the spec, cancelled requests and errors both require the monitoring
                // connection to be closed.
                self.connection.take();
                self.emit_event(|| {
                    SdamEvent::ServerHeartbeatFailed(ServerHeartbeatFailedEvent {
                        duration,
                        failure: e.clone(),
                        server_address: self.address.clone(),
                    })
                })
                .await;
            }
        }

        result
    }

    async fn handle_error(&mut self, error: Error) -> bool {
        self.topology_updater
            .handle_monitor_error(self.address.clone(), error)
            .await
    }

    async fn emit_event<F>(&self, event: F)
    where
        F: FnOnce() -> SdamEvent,
    {
        if let Some(ref emitter) = self.sdam_event_emitter {
            emitter.emit(event()).await
        }
    }
}

struct RttMonitor {
    sender: watch::Sender<RttInfo>,
    connection: Option<Connection>,
    topology: TopologyWatcher,
    address: ServerAddress,
    client_options: ClientOptions,
    handshaker: Handshaker,
}

#[derive(Debug, Clone, Copy)]
struct RttInfo {
    average: Option<Duration>,
}

impl RttMonitor {
    fn new(
        address: ServerAddress,
        topology: TopologyWatcher,
        handshaker: Handshaker,
        client_options: ClientOptions,
    ) -> (Self, watch::Receiver<RttInfo>) {
        let (sender, receiver) = watch::channel(RttInfo { average: None });
        let monitor = Self {
            address,
            connection: None,
            topology,
            client_options,
            handshaker,
            sender,
        };
        (monitor, receiver)
    }

    async fn execute(mut self) {
        // keep executing until either the topology is closed or server monitor is done (i.e. the
        // sender is closed)
        while self.topology.is_alive() && !self.sender.is_closed() {
            let start = Instant::now();
            let result = async {
                match self.connection {
                    Some(ref mut conn) => {
                        let command = hello_command(
                            self.client_options.server_api.as_ref(),
                            self.client_options.load_balanced,
                            Some(conn.stream_description()?.hello_ok),
                            None,
                        );
                        conn.send_command(command, None).await?;
                    }
                    None => {
                        let mut connection = Connection::connect_monitoring(
                            self.address.clone(),
                            self.client_options.connect_timeout,
                            self.client_options.tls_options(),
                        )
                        .await?;
                        let _ = self.handshaker.handshake(&mut connection).await?;
                        self.connection = Some(connection);
                    }
                };
                Result::Ok(())
            }
            .await;
            let rtt = start.elapsed();

            match result {
                Ok(_) => {
                    let new_rtt = match self.sender.borrow().average {
                        Some(old_rtt) => RttInfo {
                            average: Some((rtt / 5) + (old_rtt * 4 / 5)),
                        },
                        None => RttInfo { average: Some(rtt) },
                    };

                    let _ = self.sender.send(new_rtt);
                }
                Err(_) => {
                    self.connection.take();
                }
            };

            runtime::delay_for(
                self.client_options
                    .heartbeat_freq
                    .unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY),
            )
            .await;
        }
    }
}

#[allow(clippy::large_enum_variant)] // The Ok branch is bigger but more common
#[derive(Debug, Clone)]
enum HelloResult {
    Ok(HelloReply),
    Err(Error),
    Cancelled { reason: Error },
}
