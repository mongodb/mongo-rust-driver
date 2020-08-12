use serde::Deserialize;

use crate::{
    bson::{doc, Bson},
    cmap::{Command, Connection},
    error::{Error, Result},
    options::Credential,
};

#[derive(Deserialize)]
struct ServerResponse {
    #[serde(rename = "dbname")]
    db_name: String,
    ok: Bson,
}

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

    let ServerResponse { db_name, ok } =
        bson::from_bson(Bson::Document(server_response.raw_response))
            .map_err(|_| Error::invalid_authentication_response("MONGODB-X509"))?;

    if db_name != "$external" || crate::bson_util::get_int(&ok) != Some(1) {
        return Err(Error::invalid_authentication_response("MONGODB-X509"));
    }

    Ok(())
}
