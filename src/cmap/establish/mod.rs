mod handshake;

use self::handshake::Handshaker;
use super::{options::ConnectionPoolOptions, Connection};
use crate::{client::auth::Credential, error::Result, runtime::HttpClient};

/// Contains the logic to establish a connection, including handshaking, authenticating, and
/// potentially more.
#[derive(Debug)]
pub(super) struct ConnectionEstablisher {
    /// Contains the logic for handshaking a connection.
    handshaker: Handshaker,
    http_client: HttpClient,
    credential: Option<Credential>,
}

impl ConnectionEstablisher {
    /// Creates a new ConnectionEstablisher from the given options.
    pub(super) fn new(http_client: HttpClient, options: Option<&ConnectionPoolOptions>) -> Self {
        let handshaker = Handshaker::new(options);

        Self {
            handshaker,
            http_client,
            credential: options.and_then(|options| options.credential.clone()),
        }
    }

    /// Establishes a connection.
    pub(super) async fn establish_connection(&self, connection: &mut Connection) -> Result<()> {
        self.handshaker.handshake(connection).await?;

        if let Some(ref credential) = self.credential {
            credential
                .authenticate_stream(connection, &self.http_client)
                .await?;
        }

        Ok(())
    }
}
