//! Prose test 19. Azure IMDS Credentials Integration Test

use mongocrypt::ctx::KmsProvider;

use crate::{
    bson::doc,
    client_encryption::{AzureMasterKey, ClientEncryption},
    error::Result,
    Client,
};

use super::KV_NAMESPACE;

async fn try_create_data_key() -> Result<()> {
    let util_client = Client::for_test().await.into_client();
    let client_encryption = ClientEncryption::new(
        util_client,
        KV_NAMESPACE.clone(),
        [(KmsProvider::azure(), doc! {}, None)],
    )?;

    client_encryption
        .create_data_key(
            AzureMasterKey::builder()
                .key_vault_endpoint("https://keyvault-drivers-2411.vault.azure.net/keys/")
                .key_name("KEY-NAME")
                .build(),
        )
        .await
        .map(|_| ())
}

#[tokio::test]
async fn failure() {
    try_create_data_key().await.unwrap_err();
}

#[tokio::test]
async fn success_skip_ci() {
    try_create_data_key().await.unwrap();
}
