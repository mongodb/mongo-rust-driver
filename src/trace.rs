use crate::event::command::{
    CommandEventHandler,
    CommandFailedEvent,
    CommandStartedEvent,
    CommandSucceededEvent,
};
use bson::Bson;

pub(crate) struct CommandTracingEventEmitter {
    max_document_length_bytes: usize,
}

impl CommandTracingEventEmitter {
    pub(crate) fn new(max_document_length_bytes: Option<usize>) -> CommandTracingEventEmitter {
        CommandTracingEventEmitter {
            max_document_length_bytes: max_document_length_bytes.unwrap_or(1000),
        }
    }

    fn serialize_command_or_reply(&self, doc: bson::Document) -> String {
        let mut ext_json = doc.tracing_representation();
        truncate_on_char_boundary(&mut ext_json, self.max_document_length_bytes);
        ext_json
    }
}

impl CommandEventHandler for CommandTracingEventEmitter {
    fn handle_command_started_event(&self, event: CommandStartedEvent) {
        tracing::debug!(
            target: COMMAND_TRACING_EVENT_TARGET,
            command = self.serialize_command_or_reply(event.command).as_str(),
            database_name = event.db.as_str(),
            command_name = event.command_name.as_str(),
            request_id = event.request_id,
            driver_connection_id = event.connection.id,
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
        tracing::debug!(
            target: COMMAND_TRACING_EVENT_TARGET,
            reply = self.serialize_command_or_reply(event.reply).as_str(),
            command_name = event.command_name.as_str(),
            request_id = event.request_id,
            driver_connection_id = event.connection.id,
            server_host = event.connection.address.host(),
            server_port = event.connection.address.port(),
            service_id = event
                .service_id
                .map(|id| id.tracing_representation())
                .as_deref(),
            duration_ms = event.duration.as_millis().tracing_representation().as_str(),
            "Command succeeded"
        );
    }

    fn handle_command_failed_event(&self, event: CommandFailedEvent) {
        tracing::debug!(
            target: COMMAND_TRACING_EVENT_TARGET,
            failure = event.failure.tracing_representation().as_str(),
            command_name = event.command_name.as_str(),
            request_id = event.request_id,
            driver_connection_id = event.connection.id,
            server_host = event.connection.address.host(),
            server_port = event.connection.address.port(),
            service_id = event
                .service_id
                .map(|id| id.tracing_representation())
                .as_deref(),
            duration_ms = event.duration.as_millis().tracing_representation().as_str(),
            "Command failed"
        );
    }
}

pub(crate) const COMMAND_TRACING_EVENT_TARGET: &str = "mongodb::command";

trait TracingRepresentation {
    fn tracing_representation(self) -> String;
}

impl TracingRepresentation for bson::oid::ObjectId {
    fn tracing_representation(self) -> String {
        self.to_hex()
    }
}

impl TracingRepresentation for u128 {
    fn tracing_representation(self) -> String {
        self.to_string()
    }
}

impl TracingRepresentation for bson::Document {
    fn tracing_representation(self) -> String {
        Bson::Document(self).into_canonical_extjson().to_string()
    }
}

impl TracingRepresentation for crate::error::Error {
    fn tracing_representation(self) -> String {
        self.to_string()
    }
}

/// Truncates the given string at the closest UTF-8 character boundary >= the provided length.
/// If the new length is >= the current length, does nothing.
pub(crate) fn truncate_on_char_boundary(s: &mut String, new_len: usize) {
    if s.len() > new_len {
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
    }
}

macro_rules! debug_tracing_or_log_enabled {
    () => {
        tracing::enabled!(tracing::Level::DEBUG) || log::log_enabled!(log::Level::Debug)
    };
}
pub(crate) use debug_tracing_or_log_enabled;
