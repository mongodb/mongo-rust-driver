use std::time::Duration;

use crate::{client::options::ServerAddress, hello::HelloReply, sdam::ServerType};

/// Contains information about a given server in a format digestible by a connection.
#[derive(Debug, Default, Clone)]
pub(crate) struct StreamDescription {
    /// The address of the server.
    pub(crate) server_address: ServerAddress,

    /// The type of the server when the handshake occurred.
    pub(crate) initial_server_type: ServerType,

    /// The maximum wire version that the server understands.
    pub(crate) max_wire_version: Option<i32>,

    /// The minimum wire version that the server understands.
    #[allow(dead_code)]
    pub(crate) min_wire_version: Option<i32>,

    /// The supported authentication mechanisms that the server understands.
    pub(crate) sasl_supported_mechs: Option<Vec<String>>,

    /// How long sessions started on this server will stay alive without
    /// without executing an operation before the server kills them.
    pub(crate) logical_session_timeout: Option<Duration>,

    /// The maximum size of writes (excluding command overhead) that should be sent to the server.
    pub(crate) max_bson_object_size: i64,

    /// The maximum number of inserts, updates, or deletes that
    /// can be included in a write batch.  If more than this number of writes are included, the
    /// server cannot guarantee space in the response document to reply to the batch.
    pub(crate) max_write_batch_size: i64,

    /// Whether the server associated with this connection supports the `hello` command.
    pub(crate) hello_ok: bool,

    /// The maximum permitted size of a BSON wire protocol message.
    pub(crate) max_message_size_bytes: i32,
}

impl StreamDescription {
    /// Constructs a new StreamDescription from a `HelloReply`.
    pub(crate) fn from_hello_reply(reply: &HelloReply) -> Self {
        Self {
            server_address: reply.server_address.clone(),
            initial_server_type: reply.command_response.server_type(),
            max_wire_version: reply.command_response.max_wire_version,
            min_wire_version: reply.command_response.min_wire_version,
            sasl_supported_mechs: reply.command_response.sasl_supported_mechs.clone(),
            logical_session_timeout: reply
                .command_response
                .logical_session_timeout_minutes
                .map(|mins| Duration::from_secs(mins as u64 * 60)),
            max_bson_object_size: reply.command_response.max_bson_object_size,
            max_write_batch_size: reply.command_response.max_write_batch_size,
            hello_ok: reply.command_response.hello_ok.unwrap_or(false),
            max_message_size_bytes: reply.command_response.max_message_size_bytes,
        }
    }

    /// Whether this StreamDescription supports retryable writes.
    pub(crate) fn supports_retryable_writes(&self) -> bool {
        self.initial_server_type != ServerType::Standalone
            && self.logical_session_timeout.is_some()
            && self.max_wire_version.map_or(false, |version| version >= 6)
    }

    /// Gets a description of a stream for a 4.2 connection.
    #[cfg(test)]
    pub(crate) fn new_testing() -> Self {
        Self::with_wire_version(8)
    }

    /// Gets a description of a stream for a 4.2 connection.
    #[cfg(test)]
    pub(crate) fn with_wire_version(max_wire_version: i32) -> Self {
        Self {
            server_address: Default::default(),
            initial_server_type: Default::default(),
            max_wire_version: Some(max_wire_version),
            min_wire_version: Some(max_wire_version),
            sasl_supported_mechs: Default::default(),
            logical_session_timeout: Some(Duration::from_secs(30 * 60)),
            max_bson_object_size: 16 * 1024 * 1024,
            max_write_batch_size: 100_000,
            hello_ok: false,
            max_message_size_bytes: Default::default(),
        }
    }
}
