use bson::oid::ObjectId;

use crate::{
    event::sdam::{
        SdamEventHandler,
        ServerClosedEvent,
        ServerDescriptionChangedEvent,
        ServerHeartbeatFailedEvent,
        ServerHeartbeatStartedEvent,
        ServerHeartbeatSucceededEvent,
        ServerOpeningEvent,
        TopologyClosedEvent,
        TopologyDescription,
        TopologyDescriptionChangedEvent,
        TopologyOpeningEvent,
    },
    trace::serialize_command_or_reply,
};

use super::{
    trace_or_log_enabled,
    TracingOrLogLevel,
    TracingRepresentation,
    DEFAULT_MAX_DOCUMENT_LENGTH_BYTES,
    TOPOLOGY_TRACING_EVENT_TARGET,
};

impl TracingRepresentation for TopologyDescription {
    type Representation = String;

    fn tracing_representation(&self) -> Self::Representation {
        self.to_string()
    }
}

pub(crate) struct TopologyTracingEventEmitter {
    max_document_length_bytes: usize,
    topology_id: ObjectId,
}

impl TopologyTracingEventEmitter {
    pub(crate) fn new(
        max_document_length_bytes: Option<usize>,
        topology_id: ObjectId,
    ) -> TopologyTracingEventEmitter {
        TopologyTracingEventEmitter {
            max_document_length_bytes: max_document_length_bytes
                .unwrap_or(DEFAULT_MAX_DOCUMENT_LENGTH_BYTES),
            topology_id,
        }
    }
}

impl SdamEventHandler for TopologyTracingEventEmitter {
    fn handle_server_description_changed_event(&self, _event: ServerDescriptionChangedEvent) {
        // this is tentatively a no-op based on my proposal to not do separate "topology changed"
        // and "server changed" log messages due to the redundancy, but that could change
        // based on the spec review process.
    }

    fn handle_server_opening_event(&self, event: ServerOpeningEvent) {
        if trace_or_log_enabled!(
            target: TOPOLOGY_TRACING_EVENT_TARGET,
            TracingOrLogLevel::Debug
        ) {
            tracing::debug!(
                target: TOPOLOGY_TRACING_EVENT_TARGET,
                topologyId = self.topology_id.tracing_representation(),
                serverHost = event.address.host().as_ref(),
                serverPort = event.address.port_tracing_representation(),
                "Starting server monitoring"
            )
        }
    }

    fn handle_server_closed_event(&self, event: ServerClosedEvent) {
        if trace_or_log_enabled!(
            target: TOPOLOGY_TRACING_EVENT_TARGET,
            TracingOrLogLevel::Debug
        ) {
            tracing::debug!(
                target: TOPOLOGY_TRACING_EVENT_TARGET,
                topologyId = self.topology_id.tracing_representation(),
                serverHost = event.address.host().as_ref(),
                serverPort = event.address.port_tracing_representation(),
                "Stopped server monitoring"
            )
        }
    }

    fn handle_topology_description_changed_event(&self, event: TopologyDescriptionChangedEvent) {
        if trace_or_log_enabled!(
            target: TOPOLOGY_TRACING_EVENT_TARGET,
            TracingOrLogLevel::Debug
        ) {
            tracing::debug!(
                target: TOPOLOGY_TRACING_EVENT_TARGET,
                topologyId = self.topology_id.tracing_representation(),
                previousDescription = event.previous_description.tracing_representation(),
                newDescription = event.new_description.tracing_representation(),
                "Topology description changed"
            )
        }
    }

    fn handle_topology_opening_event(&self, _event: TopologyOpeningEvent) {
        if trace_or_log_enabled!(
            target: TOPOLOGY_TRACING_EVENT_TARGET,
            TracingOrLogLevel::Debug
        ) {
            tracing::debug!(
                target: TOPOLOGY_TRACING_EVENT_TARGET,
                topologyId = self.topology_id.tracing_representation(),
                "Starting topology monitoring"
            )
        }
    }

    fn handle_topology_closed_event(&self, _event: TopologyClosedEvent) {
        if trace_or_log_enabled!(
            target: TOPOLOGY_TRACING_EVENT_TARGET,
            TracingOrLogLevel::Debug
        ) {
            tracing::debug!(
                target: TOPOLOGY_TRACING_EVENT_TARGET,
                topologyId = self.topology_id.tracing_representation(),
                "Stopped topology monitoring"
            )
        }
    }

    fn handle_server_heartbeat_started_event(&self, event: ServerHeartbeatStartedEvent) {
        if trace_or_log_enabled!(
            target: TOPOLOGY_TRACING_EVENT_TARGET,
            TracingOrLogLevel::Debug
        ) {
            tracing::debug!(
                target: TOPOLOGY_TRACING_EVENT_TARGET,
                topologyId = self.topology_id.tracing_representation(),
                serverHost = event.server_address.host().as_ref(),
                serverPort = event.server_address.port_tracing_representation(),
                driverConnectionId = event.driver_connection_id,
                serverConnectionId = event.server_connection_id,
                awaited = event.awaited,
                "Server heartbeat started"
            )
        }
    }

    fn handle_server_heartbeat_succeeded_event(&self, event: ServerHeartbeatSucceededEvent) {
        if trace_or_log_enabled!(
            target: TOPOLOGY_TRACING_EVENT_TARGET,
            TracingOrLogLevel::Debug
        ) {
            tracing::debug!(
                target: TOPOLOGY_TRACING_EVENT_TARGET,
                topologyId = self.topology_id.tracing_representation(),
                serverHost = event.server_address.host().as_ref(),
                serverPort = event.server_address.port_tracing_representation(),
                driverConnectionId = event.driver_connection_id,
                serverConnectionId = event.server_connection_id,
                awaited = event.awaited,
                reply = serialize_command_or_reply(event.reply, self.max_document_length_bytes),
                durationMS = event.duration.as_millis(),
                "Server heartbeat succeeded"
            )
        }
    }

    fn handle_server_heartbeat_failed_event(&self, event: ServerHeartbeatFailedEvent) {
        if trace_or_log_enabled!(
            target: TOPOLOGY_TRACING_EVENT_TARGET,
            TracingOrLogLevel::Debug
        ) {
            tracing::debug!(
                target: TOPOLOGY_TRACING_EVENT_TARGET,
                topologyId = self.topology_id.tracing_representation(),
                serverHost = event.server_address.host().as_ref(),
                serverPort = event.server_address.port_tracing_representation(),
                driverConnectionId = event.driver_connection_id,
                serverConnectionId = event.server_connection_id,
                awaited = event.awaited,
                failure = event.failure.tracing_representation(),
                durationMS = event.duration.as_millis(),
                "Server heartbeat failed"
            )
        }
    }
}
