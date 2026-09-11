//! Prose test 19. Azure IMDS Credentials Integration Test

use mongocrypt::ctx::KmsProvider;

use crate::{
    bson::doc,
    client_encryption::{AzureMasterKey, ClientEncryption},
    error::Result,
    test::get_var,
    Client,
};

use super::KV_NAMESPACE;

async fn try_create_data_key(key_name: &str, key_vault_endpoint: &str) -> Result<()> {
    let util_client = Client::for_test().await.into_client();
    let client_encryption = ClientEncryption::new(
        util_client,
        KV_NAMESPACE.clone(),
        [(KmsProvider::azure(), doc! {}, None)],
    )?;

    let master_key = AzureMasterKey::builder()
        .key_name(key_name)
        .key_vault_endpoint(key_vault_endpoint)
        .build();
    client_encryption
        .create_data_key(master_key)
        .await
        .map(|_| ())
}

#[tokio::test]
async fn failure() {
    try_create_data_key(
        "KEY-NAME",
        "https://keyvault-drivers-2411.vault.azure.net/keys/",
    )
    .await
    .unwrap_err();
}

#[tokio::test]
async fn success() {
    try_create_data_key(&get_var("KEY_NAME"), &get_var("KEY_VAULT_ENDPOINT"))
        .await
        .unwrap();
}
