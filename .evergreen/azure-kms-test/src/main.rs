use mongodb::{
    bson::doc,
    client_encryption::{AzureMasterKey, ClientEncryption, MasterKey},
    error::Result,
    mongocrypt::ctx::KmsProvider,
    Client,
    Namespace,
};

use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let c = ClientEncryption::new(
        Client::with_uri_str("mongodb://localhost:27017").await?,
        Namespace::new("keyvault", "datakeys"),
        [(KmsProvider::azure(), doc! {}, None)],
    )?;

    let key_name = env::var("KEY_NAME").expect("KEY_NAME environment variable should be set");
    let key_vault_endpoint = env::var("KEY_VAULT_ENDPOINT")
        .expect("KEY_VAULT_ENDPOINT environment variable should be set");

    c.create_data_key(MasterKey::Azure(
        AzureMasterKey::builder()
            .key_vault_endpoint(key_vault_endpoint)
            .key_name(key_name)
            .build(),
    ))
    .await?;

    println!("Azure KMS integration test passed!");

    Ok(())
}
