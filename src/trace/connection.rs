use bson::oid::ObjectId;

use crate::{
    event::cmap::{
        CmapEventHandler,
        ConnectionCheckedInEvent,
        ConnectionCheckedOutEvent,
        ConnectionCheckoutFailedEvent,
        ConnectionCheckoutFailedReason,
        ConnectionCheckoutStartedEvent,
        ConnectionClosedEvent,
        ConnectionClosedReason,
        ConnectionCreatedEvent,
        ConnectionReadyEvent,
        PoolClearedEvent,
        PoolClosedEvent,
        PoolCreatedEvent,
        PoolReadyEvent,
    },
    trace::{TracingRepresentation, CONNECTION_TRACING_EVENT_TARGET},
};

#[derive(Clone)]
pub(crate) struct ConnectionTracingEventEmitter {
    topology_id: ObjectId,
}

impl ConnectionTracingEventEmitter {
    pub(crate) fn new(topology_id: ObjectId) -> ConnectionTracingEventEmitter {
        Self { topology_id }
    }
}

impl CmapEventHandler for ConnectionTracingEventEmitter {
    fn handle_pool_created_event(&self, event: PoolCreatedEvent) {
        let options_ref = event.options.as_ref();
        tracing::debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            serverHost = event.address.host(),
            serverPort = event.address.port(),
            maxIdleTimeMS = options_ref.and_then(|o| o.max_idle_time.map(|m| m.as_millis())),
            maxPoolSize = options_ref.and_then(|o| o.max_pool_size),
            minPoolSize = options_ref.and_then(|o| o.min_pool_size),
            "Connection pool created",
        );
    }

    fn handle_pool_ready_event(&self, event: PoolReadyEvent) {
        tracing::debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            serverHost = event.address.host(),
            serverPort = event.address.port(),
            "Connection pool ready",
        );
    }

    fn handle_pool_cleared_event(&self, event: PoolClearedEvent) {
        tracing::debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            serverHost = event.address.host(),
            serverPort = event.address.port(),
            serviceId = event
                .service_id
                .map(|id| id.tracing_representation())
                .as_deref(),
            "Connection pool cleared",
        );
    }

    fn handle_pool_closed_event(&self, event: PoolClosedEvent) {
        tracing::debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            serverHost = event.address.host(),
            serverPort = event.address.port(),
            "Connection pool closed",
        );
    }

    fn handle_connection_created_event(&self, event: ConnectionCreatedEvent) {
        tracing::debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            serverHost = event.address.host(),
            serverPort = event.address.port(),
            driverConnectionId = event.connection_id,
            "Connection created",
        );
    }

    fn handle_connection_ready_event(&self, event: ConnectionReadyEvent) {
        tracing::debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            serverHost = event.address.host(),
            serverPort = event.address.port(),
            driverConnectionId = event.connection_id,
            "Connection ready",
        );
    }

    fn handle_connection_closed_event(&self, event: ConnectionClosedEvent) {
        tracing::debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            serverHost = event.address.host(),
            serverPort = event.address.port(),
            driverConnectionId = event.connection_id,
            reason = event.reason.tracing_representation().as_str(),
            "Connection closed",
        );
    }

    fn handle_connection_checkout_started_event(&self, event: ConnectionCheckoutStartedEvent) {
        tracing::debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            serverHost = event.address.host(),
            serverPort = event.address.port(),
            "Connection checkout started",
        );
    }

    fn handle_connection_checkout_failed_event(&self, event: ConnectionCheckoutFailedEvent) {
        tracing::debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            serverHost = event.address.host(),
            serverPort = event.address.port(),
            reason = event.reason.tracing_representation().as_str(),
            "Connection checkout failed",
        );
    }

    fn handle_connection_checked_out_event(&self, event: ConnectionCheckedOutEvent) {
        tracing::debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            serverHost = event.address.host(),
            serverPort = event.address.port(),
            driverConnectionId = event.connection_id,
            "Connection checked out",
        );
    }

    fn handle_connection_checked_in_event(&self, event: ConnectionCheckedInEvent) {
        tracing::debug!(
            target: CONNECTION_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            serverHost = event.address.host(),
            serverPort = event.address.port(),
            driverConnectionId = event.connection_id,
            "Connection checked in",
        );
    }
}

impl TracingRepresentation for ConnectionClosedReason {
    fn tracing_representation(self) -> String {
        match self {
            ConnectionClosedReason::Stale => {
                "Connection became stale because the pool was cleared".to_string()
            }
            ConnectionClosedReason::Idle => "Connection has been available but unused for longer \
                                             than the configured max idle time"
                .to_string(),
            ConnectionClosedReason::Error => {
                "An error occurred while using the connection".to_string()
            }
            ConnectionClosedReason::Dropped => {
                "Connection was dropped during an operation".to_string()
            }
            ConnectionClosedReason::PoolClosed => "Connection pool was closed".to_string(),
        }
    }
}

impl TracingRepresentation for ConnectionCheckoutFailedReason {
    fn tracing_representation(self) -> String {
        match self {
            ConnectionCheckoutFailedReason::Timeout => {
                "Wait queue timeout elapsed without a connection becoming available".to_string()
            }
            ConnectionCheckoutFailedReason::ConnectionError => {
                "An error occurred while trying to establish a connection".to_string()
            }
        }
    }
}
