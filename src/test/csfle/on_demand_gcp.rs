//! Prose test 17. On-demand GCP Credentials

use mongocrypt::ctx::KmsProvider;

use crate::{
    bson::doc,
    client_encryption::{ClientEncryption, GcpMasterKey},
    error::{ErrorKind, Result},
    Client,
};

use super::KV_NAMESPACE;

async fn try_create_data_key() -> Result<()> {
    let util_client = Client::for_test().await.into_client();
    let client_encryption = ClientEncryption::new(
        util_client,
        KV_NAMESPACE.clone(),
        [(KmsProvider::gcp(), doc! {}, None)],
    )?;

    client_encryption
        .create_data_key(
            GcpMasterKey::builder()
                .project_id("devprod-drivers")
                .location("global")
                .key_ring("key-ring-csfle")
                .key_name("key-name-csfle")
                .build(),
        )
        .await
        .map(|_| ())
}

#[tokio::test]
async fn success_skip_ci() {
    try_create_data_key().await.unwrap();
}

#[tokio::test]
async fn failure() {
    let error = try_create_data_key().await.unwrap_err();
    match *error.kind {
        ErrorKind::Encryption(e) => {
            assert!(matches!(e.kind, mongocrypt::error::ErrorKind::Kms));
            assert!(e.message.unwrap().contains("GCP credentials"));
        }
        other => panic!("Expected encryption error, got {:?}", other),
    }
}
