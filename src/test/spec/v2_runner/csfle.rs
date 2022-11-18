use bson::Document;
use mongocrypt::ctx::KmsProvider;

use crate::{test::{util::TestClientBuilder, KMS_PROVIDERS_MAP}, Client, coll::options::CollectionOptions, options::WriteConcern};

use super::test_file::Test;

pub(crate) async fn populate_key_vault(client: &Client, kv_data: Option<&Vec<Document>>) {
    if let Some(kv_data) = kv_data {
        let datakeys = client.database("keyvault").collection_with_options::<Document>("datakeys", CollectionOptions::builder().write_concern(WriteConcern::MAJORITY).build());
        datakeys.drop(None).await.unwrap();
        if !kv_data.is_empty() {
            datakeys.insert_many(kv_data, None).await.unwrap();
        }
    }
}

pub(crate) fn set_auto_enc(builder: TestClientBuilder, test: &Test) -> TestClientBuilder {
    let mut enc_opts = if let Some(o) = test.client_options.as_ref().and_then(|o| o.auto_encrypt_opts.as_ref()) {
        o.clone()
    } else {
        return builder;
    };
    // dbg! handle awsTemporary / awsTemporaryNoSessionToken
    for prov in [KmsProvider::Aws, KmsProvider::Azure, KmsProvider::Gcp, KmsProvider::Kmip] {
        if enc_opts.kms_providers.credentials().contains_key(&prov) {
            let opts = KMS_PROVIDERS_MAP.get(&prov).unwrap().clone();
            enc_opts.kms_providers.set(prov, opts.0, opts.1);
        }
    }
    builder.encrypted_options(enc_opts)
}