mod handshake;

use self::handshake::Handshaker;
use super::{options::ConnectionPoolOptions, Connection};
use crate::error::Result;

/// Contains the logic to establish a connection, including handshaking, authenticating, and
/// potentially more.
#[derive(Debug)]
pub(super) struct ConnectionEstablisher {
    /// Contains the logic for handshaking a connection.
    handshaker: Handshaker,
}

impl ConnectionEstablisher {
    /// Creates a new ConnectionEstablisher from the given options.
    pub(super) fn new(options: Option<&ConnectionPoolOptions>) -> Self {
        let handshaker = Handshaker::new(options);

        Self { handshaker }
    }

    /// Establishes a connection.
    pub(super) fn establish_connection(&self, connection: &mut Connection) -> Result<()> {
        let _response = self.handshaker.handshake(connection)?;

        // TODO RUST-204: Authenticate connection if applicable.

        Ok(())
    }
}
