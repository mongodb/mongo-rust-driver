use std::sync::{Arc, RwLock};

use serde::Deserialize;

use crate::event::cmap::*;

#[derive(Clone, Debug)]
pub struct EventHandler {
    ignore: Vec<String>,
    pub events: Arc<RwLock<Vec<Event>>>,
}

impl EventHandler {
    pub fn new(ignore: Vec<String>) -> Self {
        Self {
            ignore,
            events: Default::default(),
        }
    }

    fn handle<E: Into<Event>>(&self, event: E) {
        let event = event.into();

        if !self.ignore.iter().any(|e| e == event.name()) {
            self.events.write().unwrap().push(event.into());
        }
    }
}

impl CmapEventHandler for EventHandler {
    fn handle_pool_created_event(&self, event: PoolCreatedEvent) {
        self.handle(event);
    }

    fn handle_pool_cleared_event(&self, event: PoolClearedEvent) {
        self.handle(event);
    }

    fn handle_pool_closed_event(&self, event: PoolClosedEvent) {
        self.handle(event);
    }

    fn handle_connection_created_event(&self, event: ConnectionCreatedEvent) {
        self.handle(event);
    }

    fn handle_connection_ready_event(&self, event: ConnectionReadyEvent) {
        self.handle(event);
    }

    fn handle_connection_closed_event(&self, event: ConnectionClosedEvent) {
        self.handle(event);
    }

    fn handle_connection_checkout_started_event(&self, event: ConnectionCheckoutStartedEvent) {
        self.handle(event);
    }

    fn handle_connection_checkout_failed_event(&self, event: ConnectionCheckoutFailedEvent) {
        self.handle(event);
    }

    fn handle_connection_checked_out_event(&self, event: ConnectionCheckedOutEvent) {
        self.handle(event);
    }

    fn handle_connection_checked_in_event(&self, event: ConnectionCheckedInEvent) {
        self.handle(event);
    }
}

#[derive(Debug, Deserialize, From, PartialEq)]
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

impl Event {
    pub fn name(&self) -> &'static str {
        match self {
            Event::ConnectionPoolCreated(_) => "ConnectionPoolCreated",
            Event::ConnectionPoolClosed(_) => "ConnectionPoolClosed",
            Event::ConnectionCreated(_) => "ConnectionCreated",
            Event::ConnectionReady(_) => "ConnectionReady",
            Event::ConnectionClosed(_) => "ConnectionClosed",
            Event::ConnectionCheckOutStarted(_) => "ConnectionCheckOutStarted",
            Event::ConnectionCheckOutFailed(_) => "ConnectionCheckOutFailed",
            Event::ConnectionCheckedOut(_) => "ConnectionCheckedOut",
            Event::ConnectionPoolCleared(_) => "ConnectionPoolCleared",
            Event::ConnectionCheckedIn(_) => "ConnectionCheckedIn",
        }
    }
}
