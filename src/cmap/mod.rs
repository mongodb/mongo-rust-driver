#[cfg(test)]
mod test;

pub(crate) mod conn;
mod connection_requester;
mod establish;
mod manager;
pub(crate) mod options;
mod worker;

use std::{sync::Arc, time::Duration};

use derivative::Derivative;

pub use self::conn::ConnectionInfo;
pub(crate) use self::conn::{Command, CommandResponse, Connection, StreamDescription};
use self::options::ConnectionPoolOptions;
use crate::{
    error::{ErrorKind, Result},
    event::cmap::{
        CmapEventHandler, ConnectionCheckoutFailedEvent, ConnectionCheckoutFailedReason,
        ConnectionCheckoutStartedEvent, PoolCreatedEvent,
    },
    options::StreamAddress,
    runtime::HttpClient,
};
use connection_requester::ConnectionRequester;
use manager::PoolManager;
use worker::ConnectionPoolWorker;

const DEFAULT_MAX_POOL_SIZE: u32 = 100;

/// A pool of connections implementing the CMAP spec. All state is kept internally in an `Arc`, and
/// internal state that is mutable is additionally wrapped by a lock.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(crate) struct ConnectionPool {
    address: StreamAddress,
    manager: PoolManager,
    connection_requester: ConnectionRequester,
    wait_queue_timeout: Option<Duration>,

    #[derivative(Debug = "ignore")]
    event_handler: Option<Arc<dyn CmapEventHandler>>,
}

impl ConnectionPool {
    pub(crate) fn new(
        address: StreamAddress,
        http_client: HttpClient,
        options: Option<ConnectionPoolOptions>,
    ) -> Self {
        let (manager, connection_requester) =
            ConnectionPoolWorker::start(address.clone(), http_client, options.clone());

        let event_handler = options.as_ref().and_then(|opts| opts.event_handler.clone());
        let wait_queue_timeout = options.as_ref().and_then(|opts| opts.wait_queue_timeout);

        if let Some(ref handler) = event_handler {
            handler.handle_pool_created_event(PoolCreatedEvent {
                address: address.clone(),
                options,
            });
        };

        Self {
            address,
            manager,
            connection_requester,
            wait_queue_timeout,
            event_handler,
        }
    }

    fn emit_event<F>(&self, emit: F)
    where
        F: FnOnce(&Arc<dyn CmapEventHandler>),
    {
        if let Some(ref handler) = self.event_handler {
            emit(handler);
        }
    }

    /// Checks out a connection from the pool. This method will block until this thread is at the
    /// front of the wait queue, and then will block again if no available connections are in the
    /// pool and the total number of connections is not less than the max pool size. If the method
    /// blocks for longer than `wait_queue_timeout` waiting for an available connection or to
    /// start establishing a new one, a `WaitQueueTimeoutError` will be returned.
    pub(crate) async fn check_out(&self) -> Result<Connection> {
        self.emit_event(|handler| {
            let event = ConnectionCheckoutStartedEvent {
                address: self.address.clone(),
            };

            handler.handle_connection_checkout_started_event(event);
        });

        let conn = self
            .connection_requester
            .request(self.wait_queue_timeout)
            .await;

        match conn {
            Ok(ref conn) => {
                self.emit_event(|handler| {
                    handler.handle_connection_checked_out_event(conn.checked_out_event());
                });
            }
            Err(ref e) => {
                let failure_reason =
                    if let ErrorKind::WaitQueueTimeoutError { .. } = e.kind.as_ref() {
                        ConnectionCheckoutFailedReason::Timeout
                    } else {
                        ConnectionCheckoutFailedReason::ConnectionError
                    };

                self.emit_event(|handler| {
                    handler.handle_connection_checkout_failed_event(ConnectionCheckoutFailedEvent {
                        address: self.address.clone(),
                        reason: failure_reason,
                    })
                });
            }
        }

        conn
    }

    /// Increments the generation of the pool. Rather than eagerly removing stale connections from
    /// the pool, they are left for the background thread to clean up.
    pub(crate) fn clear(&self) {
        self.manager.clear();
    }
}
