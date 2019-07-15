use std::sync::{Arc, RwLock};

use bson::Document;
use serde::Deserialize;

use crate::event::cmap::*;

struct Test {
    file: TestFile,
    event_handler: EventHandler,
}

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
struct TestFile {
    version: u8,
    description: String,
    pool_options: Option<Document>,
    operations: Vec<Operation>,
    error: Error,
    events: Vec<Event>,
    ignore: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "name")]
enum Operation {
    Start { target: String },
    Wait { ms: u64 },
    WaitForThread { target: String },
    WaitForEvent { event: String, count: u8 },
    CheckOut { label: Option<String> },
    CheckIn { connection: String },
    Clear,
    Close,
}

#[derive(Debug, Deserialize)]
struct Error {
    #[serde(rename = "type")]
    type_: String,
    message: String,
    address: String,
}

#[derive(Debug, Deserialize)]
struct Event {
    #[serde(rename = "type")]
    type_: String,
    address: String,
    connection_id: u32,
    options: Option<Document>,
    reason: String,
}

impl Test {
    fn run(&self) {}
}
