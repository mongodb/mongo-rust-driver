use crate::{
    event::command::{
        CommandEventHandler,
        CommandFailedEvent,
        CommandStartedEvent,
        CommandSucceededEvent,
    },
    trace::{tracing_debug, TracingRepresentation, COMMAND_TRACING_EVENT_TARGET},
};

pub(crate) const DEFAULT_MAX_DOCUMENT_LENGTH_BYTES: usize = 1000;

/// Type responsible for listening for command monitoring events and converting them to
/// and emitting them as tracing events.
pub(crate) struct CommandTracingEventEmitter {
    max_document_length_bytes: usize,
    /// We always store this so we don't have to branch on whether or not we're in a test
    /// whenever we emit a tracing event. However, it is only actually added to the events
    /// for tests.
    #[allow(dead_code)]
    client_id: Option<String>,
}

impl CommandTracingEventEmitter {
    pub(crate) fn new(
        max_document_length_bytes: Option<usize>,
        client_id: Option<String>,
    ) -> CommandTracingEventEmitter {
        CommandTracingEventEmitter {
            max_document_length_bytes: max_document_length_bytes
                .unwrap_or(DEFAULT_MAX_DOCUMENT_LENGTH_BYTES),
            client_id,
        }
    }
}

impl CommandEventHandler for CommandTracingEventEmitter {
    fn handle_command_started_event(&self, event: CommandStartedEvent) {
        tracing_debug!(
            target: COMMAND_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            command = serialize_command_or_reply(event.command, self.max_document_length_bytes).as_str(),
            database_name = event.db.as_str(),
            command_name = event.command_name.as_str(),
            request_id = event.request_id,
            driver_connection_id = event.connection.id,
            server_connection_id = event.connection.server_id,
            server_host = event.connection.address.host(),
            server_port = event.connection.address.port(),
            service_id = event
                .service_id
                .map(|id| id.tracing_representation())
                .as_deref(),
            "Command started"
        );
    }

    fn handle_command_succeeded_event(&self, event: CommandSucceededEvent) {
        tracing_debug!(
            target: COMMAND_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            reply = serialize_command_or_reply(event.reply, self.max_document_length_bytes).as_str(),
            command_name = event.command_name.as_str(),
            request_id = event.request_id,
            driver_connection_id = event.connection.id,
            server_connection_id = event.connection.server_id,
            server_host = event.connection.address.host(),
            server_port = event.connection.address.port(),
            service_id = event
                .service_id
                .map(|id| id.tracing_representation())
                .as_deref(),
            duration_ms = event.duration.as_millis(),
            "Command succeeded"
        );
    }

    fn handle_command_failed_event(&self, event: CommandFailedEvent) {
        tracing_debug!(
            target: COMMAND_TRACING_EVENT_TARGET,
            client_id: self.client_id.as_ref(),
            failure = event.failure.tracing_representation(),
            command_name = event.command_name.as_str(),
            request_id = event.request_id,
            driver_connection_id = event.connection.id,
            server_connection_id = event.connection.server_id,
            server_host = event.connection.address.host(),
            server_port = event.connection.address.port(),
            service_id = event
                .service_id
                .map(|id| id.tracing_representation())
                .as_deref(),
            duration_ms = event.duration.as_millis(),
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
        // TODO: eventually we should use ceil_char_boundary here but it's currently nightly-only.
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
