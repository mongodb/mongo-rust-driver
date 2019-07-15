use std::sync::{Arc, RwLock};

use serde::Deserialize;

use crate::event::cmap::*;

struct EventHandler {
    pool_created_events: Arc<RwLock<Vec<PoolCreatedEvent>>>,
    pool_cleared_events: Arc<RwLock<Vec<PoolClearedEvent>>>,
    pool_closed_events: Arc<RwLock<Vec<PoolClosedEvent>>>,
    connection_created_events: Arc<RwLock<Vec<ConnectionCreatedEvent>>>,
    connection_ready_events: Arc<RwLock<Vec<ConnectionReadyEvent>>>,
    connection_closed_events: Arc<RwLock<Vec<ConnectionClosedEvent>>>,
    connection_checkout_started_events: Arc<RwLock<Vec<ConnectionCheckoutStartedEvent>>>,
    connection_checkout_failed_events: Arc<RwLock<Vec<ConnectionCheckoutFailedEvent>>>,
    connection_checked_out_events: Arc<RwLock<Vec<ConnectionCheckedOutEvent>>>,
    connection_checked_in_events: Arc<RwLock<Vec<ConnectionCheckedInEvent>>>,
}

impl CmapEventHandler for EventHandler {
    fn handle_pool_created_event(&self, event: PoolCreatedEvent) {
        self.pool_created_events.write().unwrap().push(event);
    }

    fn handle_pool_cleared_event(&self, event: PoolClearedEvent) {
        self.pool_cleared_events.write().unwrap().push(event);
    }

    fn handle_pool_closed_event(&self, event: PoolClosedEvent) {
        self.pool_closed_events.write().unwrap().push(event);
    }

    fn handle_connection_created_event(&self, event: ConnectionCreatedEvent) {
        self.connection_created_events.write().unwrap().push(event);
    }

    fn handle_connection_ready_event(&self, event: ConnectionReadyEvent) {
        self.connection_ready_events.write().unwrap().push(event);
    }

    fn handle_connection_closed_event(&self, event: ConnectionClosedEvent) {
        self.connection_closed_events.write().unwrap().push(event);
    }

    fn handle_connection_checkout_started_event(&self, event: ConnectionCheckoutStartedEvent) {
        self.connection_checkout_started_events
            .write()
            .unwrap()
            .push(event);
    }

    fn handle_connection_checkout_failed_event(&self, event: ConnectionCheckoutFailedEvent) {
        self.connection_checkout_failed_events
            .write()
            .unwrap()
            .push(event);
    }

    fn handle_connection_checked_out_event(&self, event: ConnectionCheckedOutEvent) {
        self.connection_checked_out_events
            .write()
            .unwrap()
            .push(event);
    }

    fn handle_connection_checked_in_event(&self, event: ConnectionCheckedInEvent) {
        self.connection_checked_in_events
            .write()
            .unwrap()
            .push(event);
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestFile {
    version: u8,
    description: String,
    pool_options: Option<ConnectionPoolOptions>,
    operations: Vec<Operation>,
    error: Option<Error>,
    events: Vec<Event>,
    #[serde(default)]
    ignore: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "name")]
enum Operation {
    #[serde(rename = "start")]
    Start { target: String },

    #[serde(rename = "wait")]
    Wait { ms: u64 },

    #[serde(rename = "waitForThread")]
    WaitForThread { target: String },

    #[serde(rename = "waitForEvent")]
    WaitForEvent { event: String, count: u8 },

    #[serde(rename = "checkOut")]
    CheckOut { label: Option<String> },

    #[serde(rename = "checkIn")]
    CheckIn { connection: String },

    #[serde(rename = "clear")]
    Clear,

    #[serde(rename = "close")]
    Close,
}

#[derive(Debug, Deserialize)]
struct Error {
    #[serde(rename = "type")]
    type_: String,
    message: String,
    address: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum Event {
    ConnectionPoolCreated(PoolCreatedEvent),
    ConnectionPoolCleared(PoolClearedEvent),
    ConnectionPoolClosed(PoolClosedEvent),
    ConnectionCreated(ConnectionCreatedEvent),
    ConnectionReady(ConnectionReadyEvent),
    ConnectionClosed(ConnectionClosedEvent),
    ConnectionCheckOutStarted(ConnectionCheckoutStartedEvent),
    ConnectionCheckOutFailed(ConnectionCheckoutFailedEvent),
    ConnectionCheckedOut(ConnectionCheckedOutEvent),
    ConnectionCheckedIn(ConnectionCheckedInEvent),
}

fn run_cmap_test(test_file: TestFile) {}

#[test]
fn cmap_spec_tests() {
    crate::test::run(&["connection-monitoring-and-pooling"], run_cmap_test);
}
