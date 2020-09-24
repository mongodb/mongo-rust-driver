use std::sync::{Arc, RwLock};

use serde::{de::Unexpected, Deserialize, Deserializer};

use crate::{event::cmap::*, options::StreamAddress};

#[derive(Clone, Debug)]
pub struct EventHandler {
    pub events: Arc<RwLock<Vec<Event>>>,
}

impl EventHandler {
    pub fn new() -> Self {
        Self {
            events: Default::default(),
        }
    }

    fn handle<E: Into<Event>>(&self, event: E) {
        let event = event.into();
        self.events.write().unwrap().push(event);
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

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Deserialize, From, PartialEq)]
#[serde(tag = "type")]
pub enum Event {
    #[serde(deserialize_with = "self::deserialize_pool_created")]
    ConnectionPoolCreated(PoolCreatedEvent),
    ConnectionPoolClosed(PoolClosedEvent),
    ConnectionCreated(ConnectionCreatedEvent),
    ConnectionReady(ConnectionReadyEvent),
    ConnectionClosed(ConnectionClosedEvent),
    ConnectionCheckOutStarted(ConnectionCheckoutStartedEvent),
    #[serde(deserialize_with = "self::deserialize_checkout_failed")]
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

#[derive(Debug, Deserialize)]
struct PoolCreatedEventHelper {
    #[serde(default)]
    pub options: Option<PoolOptionsHelper>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PoolOptionsHelper {
    Number(u64),
    Options(ConnectionPoolOptions),
}

fn deserialize_pool_created<'de, D>(deserializer: D) -> Result<PoolCreatedEvent, D::Error>
where
    D: Deserializer<'de>,
{
    let helper = PoolCreatedEventHelper::deserialize(deserializer)?;

    // The CMAP spec tests use "42" as a placeholder in the expected events to indicate that the
    // driver should assert that a value is present without any constraints on the value itself.
    // This idiom is used for the connection pool creation options even when no options are
    // specified, meaning that there isn't any useful assertion we can do based on this value.
    // Because of this, we deserialize the value `42` into `None` for the options, which prevents
    // deserialization failure due to an unexpected type. For other integer values, we raise an
    // error indicating that we expect `42` instead.
    let options = match helper.options {
        Some(PoolOptionsHelper::Options(opts)) => Some(opts),
        Some(PoolOptionsHelper::Number(42)) | None => None,
        Some(PoolOptionsHelper::Number(other)) => {
            return Err(serde::de::Error::invalid_value(
                Unexpected::Unsigned(other),
                &"42",
            ));
        }
    };

    Ok(PoolCreatedEvent {
        address: StreamAddress {
            hostname: Default::default(),
            port: None,
        },
        options,
    })
}

#[derive(Debug, Deserialize)]
struct ConnectionCheckoutFailedHelper {
    pub reason: CheckoutFailedReasonHelper,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum CheckoutFailedReasonHelper {
    Timeout,
    ConnectionError,
    PoolClosed,
}

fn deserialize_checkout_failed<'de, D>(
    deserializer: D,
) -> Result<ConnectionCheckoutFailedEvent, D::Error>
where
    D: Deserializer<'de>,
{
    let helper = ConnectionCheckoutFailedHelper::deserialize(deserializer)?;

    // The driver doesn't have a concept of a "closed pool", instead having the pool closed when the
    // pool is dropped. Because of this, the driver doesn't implement the "poolClosed" reason for a
    // connection checkout failure. While we skip over the corresponding tests in our spec test
    // runner, we still need to be able to deserialize the "poolClosed" reason to avoid the test
    // harness panicking, so we arbitrarily map the "poolClosed" to "connectionError".
    let reason = match helper.reason {
        CheckoutFailedReasonHelper::PoolClosed | CheckoutFailedReasonHelper::ConnectionError => {
            ConnectionCheckoutFailedReason::ConnectionError
        }
        CheckoutFailedReasonHelper::Timeout => ConnectionCheckoutFailedReason::Timeout,
    };

    Ok(ConnectionCheckoutFailedEvent {
        address: StreamAddress {
            hostname: Default::default(),
            port: None,
        },
        reason,
    })
}
