use bson::{doc, Document};
use mongocrypt::ctx::KmsProvider;

use crate::{
    coll::options::CollectionOptions,
    options::WriteConcern,
    test::{csfle::AWS_KMS, util::TestClientBuilder},
    Client,
};

use super::test_file::Test;

pub(crate) async fn populate_key_vault(client: &Client, kv_data: Option<&Vec<Document>>) {
    if let Some(kv_data) = kv_data {
        let datakeys = client
            .database("keyvault")
            .collection_with_options::<Document>(
                "datakeys",
                CollectionOptions::builder()
                    .write_concern(WriteConcern::majority())
                    .build(),
            );
        datakeys.drop().await.unwrap();
        if !kv_data.is_empty() {
            datakeys.insert_many(kv_data).await.unwrap();
        }
    }
}

pub(crate) fn set_auto_enc(builder: TestClientBuilder, test: &Test) -> TestClientBuilder {
    let mut enc_opts = if let Some(o) = test
        .client_options
        .as_ref()
        .and_then(|o| o.auto_encrypt_opts.as_ref())
    {
        o.clone()
    } else {
        return builder;
    };

    let kms_providers = &mut enc_opts.kms_providers;
    kms_providers.set_test_options();

    let aws_id = std::env::var("CSFLE_AWS_TEMP_ACCESS_KEY_ID").ok();
    let aws_key = std::env::var("CSFLE_AWS_TEMP_SECRET_ACCESS_KEY").ok();
    let aws_token = std::env::var("CSFLE_AWS_TEMP_SESSION_TOKEN").ok();

    if kms_providers
        .credentials()
        .contains_key(&KmsProvider::other("awsTemporary"))
    {
        kms_providers.set(
            KmsProvider::aws(),
            doc! {
                "accessKeyId": aws_id.unwrap(),
                "secretAccessKey": aws_key.unwrap(),
                "sessionToken": aws_token.unwrap(),
            },
            AWS_KMS.clone().2,
        );
        kms_providers.clear(&KmsProvider::other("awsTemporary"));
    } else if kms_providers
        .credentials()
        .contains_key(&KmsProvider::other("awsTemporaryNoSessionToken"))
    {
        kms_providers.set(
            KmsProvider::aws(),
            doc! {
                "accessKeyId": aws_id.unwrap(),
                "secretAccessKey": aws_key.unwrap(),
            },
            AWS_KMS.clone().2,
        );
        kms_providers.clear(&KmsProvider::other("awsTemporaryNoSessionToken"));
    }
    builder.encrypted_options(enc_opts)
}
