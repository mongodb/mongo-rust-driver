mod handshake;

use self::handshake::Handshaker;
use super::{options::ConnectionPoolOptions, Connection};
use crate::{client::auth::Credential, error::Result};

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
    pub(super) fn establish_connection(
        &self,
        connection: &mut Connection,
        credential: Option<&Credential>,
    ) -> Result<()> {
        self.handshaker.handshake(connection)?;

        if let Some(credential) = credential {
            credential.authenticate_stream(connection)?;
        }

        Ok(())
    }
}
