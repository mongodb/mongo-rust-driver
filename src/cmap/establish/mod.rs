pub(super) mod handshake;
#[cfg(test)]
mod test;

use self::handshake::Handshaker;
use super::{conn::PendingConnection, options::ConnectionPoolOptions, Connection};
use crate::{
    client::{auth::Credential, options::ServerApi},
    error::Result,
    runtime::HttpClient,
};

/// Contains the logic to establish a connection, including handshaking, authenticating, and
/// potentially more.
#[derive(Clone, Debug)]
pub(super) struct ConnectionEstablisher {
    /// Contains the logic for handshaking a connection.
    handshaker: Handshaker,
    http_client: HttpClient,
    credential: Option<Credential>,
    server_api: Option<ServerApi>,
}

impl ConnectionEstablisher {
    /// Creates a new ConnectionEstablisher from the given options.
    pub(super) fn new(http_client: HttpClient, options: Option<&ConnectionPoolOptions>) -> Self {
        let handshaker = Handshaker::new(options.cloned().map(Into::into));

        Self {
            handshaker,
            http_client,
            credential: options.and_then(|options| options.credential.clone()),
            server_api: options.and_then(|options| options.server_api.clone()),
        }
    }

    /// Establishes a connection.
    pub(super) async fn establish_connection(
        &self,
        pending_connection: PendingConnection,
    ) -> Result<Connection> {
        let service_generations = pending_connection.service_generations.clone();
        let mut connection = Connection::connect(pending_connection).await?;
        
        let handshake = self
            .handshaker
            .handshake(&mut connection)
            .await?;

        // If the handshake response had a `serviceId` field, this is a connection to a load
        // balancer and must derive its generation from the service_generations map.
        if let Some(service_id) = handshake.is_master_reply.command_response.service_id {
            connection.generation = *service_generations.get(&service_id).unwrap_or(&0);
            connection.service_id = Some(service_id);
        }

        if let Some(ref credential) = self.credential {
            credential
                .authenticate_stream(
                    &mut connection,
                    &self.http_client,
                    self.server_api.as_ref(),
                    handshake.first_round,
                )
                .await?;
        }

        Ok(connection)
    }
}
