use crate::{is_master::IsMasterReply, sdam::ServerType};

/// Contains information about a given server in a format digestible by a connection.
#[derive(Debug, Default)]
pub(crate) struct StreamDescription {
    /// The type of the server.
    pub(crate) server_type: ServerType,

    /// The maximum wire version that the server understands.
    pub(crate) max_wire_version: Option<i32>,

    /// The minimum wire version that the server understands.
    pub(crate) min_wire_version: Option<i32>,

    /// The supported authentication mechanisms that the server understands.
    pub(crate) sasl_supported_mechs: Option<Vec<String>>,
}

impl StreamDescription {
    /// Constructs a new StreamDescription from an IsMasterReply.
    pub(crate) fn from_is_master(reply: IsMasterReply) -> Self {
        Self {
            server_type: reply.command_response.server_type(),
            max_wire_version: reply.command_response.max_wire_version,
            min_wire_version: reply.command_response.min_wire_version,
            sasl_supported_mechs: reply.command_response.sasl_supported_mechs,
            // TODO RUST-204: Add "saslSupportedMechs" if applicable.
        }
    }

    /// Gets a description of a stream for a 4.2 connection.
    #[cfg(test)]
    pub(crate) fn new_testing() -> Self {
        Self {
            server_type: Default::default(),
            max_wire_version: Some(8),
            min_wire_version: Some(8),
            sasl_supported_mechs: Default::default(),
        }
    }
}
