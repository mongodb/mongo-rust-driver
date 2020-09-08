use crate::{
    bson::{doc, Document},
    cmap::{Command, CommandResponse, Connection},
    error::{Error, Result},
    options::Credential,
};

/// Constructs the first client message in the X.509 handshake.
pub(crate) fn build_client_first(credential: &Credential) -> Command {
    let mut auth_command_doc = doc! {
        "authenticate": 1,
        "mechanism": "MONGODB-X509",
    };

    if let Some(ref username) = credential.username {
        auth_command_doc.insert("username", username);
    }

    Command::new("authenticate".into(), "$external".into(), auth_command_doc)
}

/// Sends the first client message in the X.509 handshake.
pub(crate) async fn send_client_first(
    conn: &mut Connection,
    credential: &Credential,
) -> Result<CommandResponse> {
    let command = build_client_first(credential);

    conn.send_command(command, None).await
}

/// Performs X.509 authentication for a given stream.
pub(super) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    server_first: impl Into<Option<Document>>,
) -> Result<()> {
    let server_response = match server_first.into() {
        Some(server_first) => server_first,
        None => send_client_first(conn, credential).await?.raw_response,
    };

    if server_response.get_str("dbname") != Ok("$external") {
        return Err(Error::authentication_error(
            "MONGODB-X509",
            "Authentication failed",
        ));
    }

    Ok(())
}
