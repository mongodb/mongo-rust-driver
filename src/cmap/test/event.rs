use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use serde::{de::Unexpected, Deserialize, Deserializer, Serialize};

use crate::{event::cmap::*, options::ServerAddress, test::util::EventSubscriber};
use tokio::sync::broadcast::error::SendError;

#[derive(Clone, Debug)]
pub struct TestEventHandler {
    pub(crate) events: Arc<RwLock<Vec<CmapEvent>>>,
    channel_sender: tokio::sync::broadcast::Sender<CmapEvent>,
}

impl TestEventHandler {
    pub fn new() -> Self {
        let (channel_sender, _) = tokio::sync::broadcast::channel(500);
        Self {
            events: Default::default(),
            channel_sender,
        }
    }

    fn handle<E: Into<CmapEvent>>(&self, event: E) {
        let event = event.into();
        // this only errors if no receivers are listening which isn't a concern here.
        let _: std::result::Result<usize, SendError<CmapEvent>> =
            self.channel_sender.send(event.clone());
        self.events.write().unwrap().push(event);
    }

    pub(crate) fn subscribe(&self) -> EventSubscriber<'_, TestEventHandler, CmapEvent> {
        EventSubscriber::new(self, self.channel_sender.subscribe())
    }
}

#[allow(deprecated)]
impl CmapEventHandler for TestEventHandler {
    fn handle_pool_created_event(&self, event: PoolCreatedEvent) {
        self.handle(event);
    }

    fn handle_pool_ready_event(&self, event: PoolReadyEvent) {
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

impl Serialize for CmapEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::PoolCreated(event) => event.serialize(serializer),
            Self::PoolClosed(event) => event.serialize(serializer),
            Self::PoolReady(event) => event.serialize(serializer),
            Self::ConnectionCreated(event) => event.serialize(serializer),
            Self::ConnectionReady(event) => event.serialize(serializer),
            Self::ConnectionClosed(event) => event.serialize(serializer),
            Self::ConnectionCheckoutStarted(event) => event.serialize(serializer),
            Self::ConnectionCheckoutFailed(event) => event.serialize(serializer),
            Self::ConnectionCheckedOut(event) => event.serialize(serializer),
            Self::PoolCleared(event) => event.serialize(serializer),
            Self::ConnectionCheckedIn(event) => event.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for CmapEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "type")]
        // clippy doesn't like that all variants start with the same name, but we use these
        // to match the test file names.
        #[allow(clippy::enum_variant_names)]
        enum EventHelper {
            #[serde(deserialize_with = "self::deserialize_pool_created")]
            ConnectionPoolCreated(PoolCreatedEvent),
            ConnectionPoolClosed(PoolClosedEvent),
            ConnectionPoolReady(PoolReadyEvent),
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

        let helper = EventHelper::deserialize(deserializer)?;
        let event = match helper {
            EventHelper::ConnectionPoolCreated(e) => CmapEvent::PoolCreated(e),
            EventHelper::ConnectionPoolClosed(e) => CmapEvent::PoolClosed(e),
            EventHelper::ConnectionPoolReady(e) => CmapEvent::PoolReady(e),
            EventHelper::ConnectionCreated(e) => CmapEvent::ConnectionCreated(e),
            EventHelper::ConnectionReady(e) => CmapEvent::ConnectionReady(e),
            EventHelper::ConnectionClosed(e) => CmapEvent::ConnectionClosed(e),
            EventHelper::ConnectionCheckOutStarted(e) => CmapEvent::ConnectionCheckoutStarted(e),
            EventHelper::ConnectionCheckOutFailed(e) => CmapEvent::ConnectionCheckoutFailed(e),
            EventHelper::ConnectionCheckedOut(e) => CmapEvent::ConnectionCheckedOut(e),
            EventHelper::ConnectionPoolCleared(e) => CmapEvent::PoolCleared(e),
            EventHelper::ConnectionCheckedIn(e) => CmapEvent::ConnectionCheckedIn(e),
        };
        Ok(event)
    }
}

impl CmapEvent {
    pub fn name(&self) -> &'static str {
        match self {
            CmapEvent::PoolCreated(_) => "ConnectionPoolCreated",
            CmapEvent::PoolReady(_) => "ConnectionPoolReady",
            CmapEvent::PoolClosed(_) => "ConnectionPoolClosed",
            CmapEvent::ConnectionCreated(_) => "ConnectionCreated",
            CmapEvent::ConnectionReady(_) => "ConnectionReady",
            CmapEvent::ConnectionClosed(_) => "ConnectionClosed",
            CmapEvent::ConnectionCheckoutStarted(_) => "ConnectionCheckOutStarted",
            CmapEvent::ConnectionCheckoutFailed(_) => "ConnectionCheckOutFailed",
            CmapEvent::ConnectionCheckedOut(_) => "ConnectionCheckedOut",
            CmapEvent::PoolCleared(_) => "ConnectionPoolCleared",
            CmapEvent::ConnectionCheckedIn(_) => "ConnectionCheckedIn",
        }
    }

    // The names in drivers-atlas-testing tests are slightly different than those used in spec
    // tests.
    pub fn planned_maintenance_testing_name(&self) -> &'static str {
        match self {
            CmapEvent::PoolCreated(_) => "PoolCreatedEvent",
            CmapEvent::PoolReady(_) => "PoolReadyEvent",
            CmapEvent::PoolCleared(_) => "PoolClearedEvent",
            CmapEvent::PoolClosed(_) => "PoolClosedEvent",
            CmapEvent::ConnectionCreated(_) => "ConnectionCreatedEvent",
            CmapEvent::ConnectionReady(_) => "ConnectionReadyEvent",
            CmapEvent::ConnectionClosed(_) => "ConnectionClosedEvent",
            CmapEvent::ConnectionCheckoutStarted(_) => "ConnectionCheckOutStartedEvent",
            CmapEvent::ConnectionCheckoutFailed(_) => "ConnectionCheckOutFailedEvent",
            CmapEvent::ConnectionCheckedOut(_) => "ConnectionCheckedOutEvent",
            CmapEvent::ConnectionCheckedIn(_) => "ConnectionCheckedInEvent",
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
        address: ServerAddress::Tcp {
            host: Default::default(),
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
        address: ServerAddress::Tcp {
            host: Default::default(),
            port: None,
        },
        reason,
        #[cfg(feature = "tracing-unstable")]
        error: None,
        duration: Duration::ZERO,
    })
}
