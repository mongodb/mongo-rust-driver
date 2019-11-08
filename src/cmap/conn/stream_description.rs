use crate::is_master::IsMasterReply;

/// Contains information about a given server in a format digestible by a connection.
#[derive(Debug, Default)]
pub(crate) struct StreamDescription {
    /// The maximum wire version that the server understands.
    pub max_wire_version: Option<i32>,

    /// The minimum wire version that the server understands.
    pub min_wire_version: Option<i32>,
    // TODO RUST-204: Add "saslSupportedMechs" if applicable.
}

impl StreamDescription {
    /// Constructs a new StreamDescription from an IsMasterReply.
    pub(crate) fn from_is_master(reply: IsMasterReply) -> Self {
        Self {
            max_wire_version: reply.command_response.max_wire_version,
            min_wire_version: reply.command_response.min_wire_version,
            // TODO RUST-204: Add "saslSupportedMechs" if applicable.
        }
    }

    /// Gets a description of a stream for a 4.2 connection.
    /// This should be used for test purposes only.
    pub(crate) fn new_testing() -> Self {
        Self {
            max_wire_version: Some(8),
            min_wire_version: Some(8),
        }
    }
}
