use std::sync::{Arc, RwLock};

use serde::Deserialize;

use crate::event::cmap::*;

#[derive(Clone, Debug, Default)]
pub struct EventHandler {
    events: Arc<RwLock<Vec<Event>>>,
}

impl CmapEventHandler for EventHandler {
    fn handle_pool_created_event(&self, event: PoolCreatedEvent) {
        self.events.write().unwrap().push(event.into());
    }

    fn handle_pool_cleared_event(&self, event: PoolClearedEvent) {
        self.events.write().unwrap().push(event.into());
    }

    fn handle_pool_closed_event(&self, event: PoolClosedEvent) {
        self.events.write().unwrap().push(event.into());
    }

    fn handle_connection_created_event(&self, event: ConnectionCreatedEvent) {
        self.events.write().unwrap().push(event.into());
    }

    fn handle_connection_ready_event(&self, event: ConnectionReadyEvent) {
        self.events.write().unwrap().push(event.into());
    }

    fn handle_connection_closed_event(&self, event: ConnectionClosedEvent) {
        self.events.write().unwrap().push(event.into());
    }

    fn handle_connection_checkout_started_event(&self, event: ConnectionCheckoutStartedEvent) {
        self.events.write().unwrap().push(event.into());
    }

    fn handle_connection_checkout_failed_event(&self, event: ConnectionCheckoutFailedEvent) {
        self.events.write().unwrap().push(event.into());
    }

    fn handle_connection_checked_out_event(&self, event: ConnectionCheckedOutEvent) {
        self.events.write().unwrap().push(event.into());
    }

    fn handle_connection_checked_in_event(&self, event: ConnectionCheckedInEvent) {
        self.events.write().unwrap().push(event.into());
    }
}

#[derive(Debug, Deserialize, From)]
#[serde(tag = "type")]
pub enum Event {
    ConnectionPoolCreated(PoolCreatedEvent),
    ConnectionPoolClosed(PoolClosedEvent),
    ConnectionCreated(ConnectionCreatedEvent),
    ConnectionReady(ConnectionReadyEvent),
    ConnectionClosed(ConnectionClosedEvent),
    ConnectionCheckOutStarted(ConnectionCheckoutStartedEvent),
    ConnectionCheckOutFailed(ConnectionCheckoutFailedEvent),
    ConnectionCheckedOut(ConnectionCheckedOutEvent),
    ConnectionPoolCleared(PoolClearedEvent),
    ConnectionCheckedIn(ConnectionCheckedInEvent),
}
