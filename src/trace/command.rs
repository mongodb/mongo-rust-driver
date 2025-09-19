use crate::bson::oid::ObjectId;

use crate::{
    event::command::CommandEvent,
    trace::{serialize_command_or_reply, TracingRepresentation, COMMAND_TRACING_EVENT_TARGET},
};

use super::DEFAULT_MAX_DOCUMENT_LENGTH_BYTES;

/// Type responsible for listening for command monitoring events and converting them to
/// and emitting them as tracing events.
pub(crate) struct CommandTracingEventEmitter {
    max_document_length_bytes: usize,
    topology_id: ObjectId,
}

impl CommandTracingEventEmitter {
    pub(crate) fn new(
        max_document_length_bytes: Option<usize>,
        topology_id: ObjectId,
    ) -> CommandTracingEventEmitter {
        CommandTracingEventEmitter {
            max_document_length_bytes: max_document_length_bytes
                .unwrap_or(DEFAULT_MAX_DOCUMENT_LENGTH_BYTES),
            topology_id,
        }
    }

    pub(crate) fn handle(&self, event: CommandEvent) {
        match event {
            CommandEvent::Started(event) => {
                tracing::debug!(
                    target: COMMAND_TRACING_EVENT_TARGET,
                    topologyId = self.topology_id.tracing_representation(),
                    command = serialize_command_or_reply(event.command, self.max_document_length_bytes),
                    databaseName = event.db,
                    commandName = event.command_name,
                    requestId = event.request_id,
                    driverConnectionId = event.connection.id,
                    serverConnectionId = event.connection.server_id,
                    serverHost = event.connection.address.host().as_ref(),
                    serverPort = event.connection.address.port_tracing_representation(),
                    serviceId = event.service_id.map(|id| id.tracing_representation()),
                    "Command started"
                );
            }
            CommandEvent::Succeeded(event) => {
                tracing::debug!(
                    target: COMMAND_TRACING_EVENT_TARGET,
                    topologyId = self.topology_id.tracing_representation(),
                    reply = serialize_command_or_reply(event.reply, self.max_document_length_bytes),
                    commandName = event.command_name,
                    requestId = event.request_id,
                    driverConnectionId = event.connection.id,
                    serverConnectionId = event.connection.server_id,
                    serverHost = event.connection.address.host().as_ref(),
                    serverPort = event.connection.address.port_tracing_representation(),
                    serviceId = event.service_id.map(|id| id.tracing_representation()),
                    durationMS = event.duration.as_millis(),
                    "Command succeeded"
                );
            }
            CommandEvent::Failed(event) => {
                tracing::debug!(
                    target: COMMAND_TRACING_EVENT_TARGET,
                    topologyId = self.topology_id.tracing_representation(),
                    failure = event.failure.tracing_representation(self.max_document_length_bytes),
                    commandName = event.command_name,
                    requestId = event.request_id,
                    driverConnectionId = event.connection.id,
                    serverConnectionId = event.connection.server_id,
                    serverHost = event.connection.address.host().as_ref(),
                    serverPort = event.connection.address.port_tracing_representation(),
                    serviceId = event.service_id.map(|id| id.tracing_representation()),
                    durationMS = event.duration.as_millis(),
                    "Command failed"
                );
            }
        }
    }
}
