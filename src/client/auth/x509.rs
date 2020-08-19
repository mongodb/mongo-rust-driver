use crate::{
    bson::doc,
    cmap::{Command, Connection},
    error::{Error, Result},
    options::Credential,
};

pub(super) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
) -> Result<()> {
    let mut auth_command_doc = doc! {
        "authenticate": 1,
        "mechanism": "MONGODB-X509",
    };

    if let Some(ref username) = credential.username {
        auth_command_doc.insert("username", username);
    }

    let auth_command = Command::new("authenticate".into(), "$external".into(), auth_command_doc);

    let server_response = conn.send_command(auth_command, None).await?;

    if !server_response.is_success()
        || server_response.raw_response.get_str("dbname") != Ok("$external")
    {
        return Err(Error::authentication_error(
            "MONGODB-X509",
            "Authentication failed",
        ));
    }

    Ok(())
}
