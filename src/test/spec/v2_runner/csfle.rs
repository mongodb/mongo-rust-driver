use bson::{doc, Document};
use mongocrypt::ctx::KmsProvider;

use crate::{
    coll::options::CollectionOptions,
    options::WriteConcern,
    test::{util::TestClientBuilder, KMS_PROVIDERS_MAP},
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
    for prov in [
        KmsProvider::Aws,
        KmsProvider::Azure,
        KmsProvider::Gcp,
        KmsProvider::Kmip,
    ] {
        if kms_providers.credentials().contains_key(&prov) {
            let opts = KMS_PROVIDERS_MAP.get(&prov).unwrap().clone();
            kms_providers.set(prov, opts.0, opts.1);
        }
    }
    let aws_tls = KMS_PROVIDERS_MAP
        .get(&KmsProvider::Aws)
        .and_then(|(_, t)| t.as_ref());
    let aws_id = std::env::var("CSFLE_AWS_TEMP_ACCESS_KEY_ID").ok();
    let aws_key = std::env::var("CSFLE_AWS_TEMP_SECRET_ACCESS_KEY").ok();
    let aws_token = std::env::var("CSFLE_AWS_TEMP_SESSION_TOKEN").ok();
    if kms_providers
        .credentials()
        .contains_key(&KmsProvider::Other("awsTemporary".to_string()))
    {
        kms_providers.set(
            KmsProvider::Aws,
            doc! {
                "accessKeyId": aws_id.unwrap(),
                "secretAccessKey": aws_key.unwrap(),
                "sessionToken": aws_token.unwrap(),
            },
            aws_tls.cloned(),
        );
        kms_providers.clear(&KmsProvider::Other("awsTemporary".to_string()));
    } else if kms_providers
        .credentials()
        .contains_key(&KmsProvider::Other(
            "awsTemporaryNoSessionToken".to_string(),
        ))
    {
        kms_providers.set(
            KmsProvider::Aws,
            doc! {
                "accessKeyId": aws_id.unwrap(),
                "secretAccessKey": aws_key.unwrap(),
            },
            aws_tls.cloned(),
        );
        kms_providers.clear(&KmsProvider::Other(
            "awsTemporaryNoSessionToken".to_string(),
        ));
    }
    builder.encrypted_options(enc_opts)
}
