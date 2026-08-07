//! Prose test 15. On-demand AWS Credentials

use mongocrypt::ctx::KmsProvider;

use crate::{bson::doc, client_encryption::ClientEncryption, error::Result, Client};

use super::{AWS_MASTER_KEY, KV_NAMESPACE};

async fn try_create_data_key() -> Result<()> {
    let ce = ClientEncryption::new(
        Client::for_test().await.into_client(),
        KV_NAMESPACE.clone(),
        [(KmsProvider::aws(), doc! {}, None)],
    )?;
    ce.create_data_key(AWS_MASTER_KEY.clone()).await.map(|_| ())
}

#[tokio::test]
async fn success() {
    assert!(std::env::var("AWS_ACCESS_KEY_ID").is_ok());
    assert!(std::env::var("AWS_SECRET_ACCESS_KEY").is_ok());
    try_create_data_key().await.unwrap();
}

#[tokio::test]
async fn failure() {
    assert!(std::env::var("AWS_ACCESS_KEY_ID").is_err());
    assert!(std::env::var("AWS_SECRET_ACCESS_KEY").is_err());
    try_create_data_key().await.unwrap_err();
}
