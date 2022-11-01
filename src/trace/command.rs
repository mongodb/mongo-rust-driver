use bson::oid::ObjectId;

use crate::{
    event::command::{
        CommandEventHandler,
        CommandFailedEvent,
        CommandStartedEvent,
        CommandSucceededEvent,
    },
    trace::{TracingRepresentation, COMMAND_TRACING_EVENT_TARGET},
};

pub(crate) const DEFAULT_MAX_DOCUMENT_LENGTH_BYTES: usize = 1000;

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
}

impl CommandEventHandler for CommandTracingEventEmitter {
    fn handle_command_started_event(&self, event: CommandStartedEvent) {
        tracing::debug!(
            target: COMMAND_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            command = serialize_command_or_reply(event.command, self.max_document_length_bytes),
            databaseName = event.db,
            commandName = event.command_name,
            requestId = event.request_id,
            driverConnectionId = event.connection.id,
            serverConnectionId = event.connection.server_id,
            serverHost = event.connection.address.host(),
            serverPort = event.connection.address.port_tracing_representation(),
            serviceId = event.service_id.map(|id| id.tracing_representation()),
            "Command started"
        );
    }

    fn handle_command_succeeded_event(&self, event: CommandSucceededEvent) {
        tracing::debug!(
            target: COMMAND_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            reply = serialize_command_or_reply(event.reply, self.max_document_length_bytes),
            commandName = event.command_name,
            requestId = event.request_id,
            driverConnectionId = event.connection.id,
            serverConnectionId = event.connection.server_id,
            serverHost = event.connection.address.host(),
            serverPort = event.connection.address.port_tracing_representation(),
            serviceId = event.service_id.map(|id| id.tracing_representation()),
            durationMS = event.duration.as_millis(),
            "Command succeeded"
        );
    }

    fn handle_command_failed_event(&self, event: CommandFailedEvent) {
        tracing::debug!(
            target: COMMAND_TRACING_EVENT_TARGET,
            topologyId = self.topology_id.tracing_representation(),
            failure = event.failure.tracing_representation(),
            commandName = event.command_name,
            requestId = event.request_id,
            driverConnectionId = event.connection.id,
            serverConnectionId = event.connection.server_id,
            serverHost = event.connection.address.host(),
            serverPort = event.connection.address.port_tracing_representation(),
            serviceId = event.service_id.map(|id| id.tracing_representation()),
            durationMS = event.duration.as_millis(),
            "Command failed"
        );
    }
}

fn serialize_command_or_reply(doc: bson::Document, max_length_bytes: usize) -> String {
    let mut ext_json = doc.tracing_representation();
    truncate_on_char_boundary(&mut ext_json, max_length_bytes);
    ext_json
}

// TODO: subject to change based on what exact version of truncation we decide to go with.
/// Truncates the given string at the closest UTF-8 character boundary >= the provided length.
/// If the new length is >= the current length, does nothing.
pub(crate) fn truncate_on_char_boundary(s: &mut String, new_len: usize) {
    let original_len = s.len();
    if original_len > new_len {
        // to avoid generating invalid UTF-8, find the first index >= max_length_bytes that is
        // the end of a character.
        // TODO: RUST-1496 we should use ceil_char_boundary here but it's currently nightly-only.
        // see: https://doc.rust-lang.org/std/string/struct.String.html#method.ceil_char_boundary
        let mut truncate_index = new_len;
        // is_char_boundary returns true when the provided value == the length of the string, so
        // if we reach the end of the string this loop will terminate.
        while !s.is_char_boundary(truncate_index) {
            truncate_index += 1;
        }
        s.truncate(truncate_index);
        // due to the "rounding up" behavior we might not actually end up truncating anything.
        // if we did, spec requires we add a trailing "...".
        if truncate_index < original_len {
            s.push_str("...")
        }
    }
}
