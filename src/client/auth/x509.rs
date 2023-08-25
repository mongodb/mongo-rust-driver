use crate::{
    bson::{doc, Document},
    client::options::ServerApi,
    cmap::{Command, Connection, RawCommandResponse},
    error::{Error, Result},
    options::Credential,
};

/// Constructs the first client message in the X.509 handshake for speculative authentication
pub(crate) fn build_speculative_client_first(credential: &Credential) -> Command {
    self::build_client_first(credential, None)
}

/// Constructs the first client message in the X.509 handshake.
pub(crate) fn build_client_first(
    credential: &Credential,
    server_api: Option<&ServerApi>,
) -> Command {
    let mut auth_command_doc = doc! {
        "authenticate": 1,
        "mechanism": "MONGODB-X509",
    };

    if let Some(ref username) = credential.username {
        auth_command_doc.insert("username", username);
    }

    let mut command = Command::new("authenticate".into(), "$external".into(), auth_command_doc);
    if let Some(server_api) = server_api {
        command.set_server_api(server_api);
    }

    command
}

/// Sends the first client message in the X.509 handshake.
pub(crate) async fn send_client_first(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
) -> Result<RawCommandResponse> {
    let command = build_client_first(credential, server_api);

    conn.send_command(command, None).await
}

/// Performs X.509 authentication for a given stream.
pub(super) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
    server_first: impl Into<Option<Document>>,
) -> Result<()> {
    let server_response: Document = match server_first.into() {
        Some(_) => return Ok(()),
        None => send_client_first(conn, credential, server_api)
            .await?
            .auth_response_body("MONGODB-X509")?,
    };

    if server_response
        .get("ok")
        .and_then(crate::bson_util::get_int)
        != Some(1)
    {
        return Err(Error::authentication_error(
            "MONGODB-X509",
            "Authentication failed",
        ));
    }

    Ok(())
}
