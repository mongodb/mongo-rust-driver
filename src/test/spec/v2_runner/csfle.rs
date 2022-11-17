use std::collections::HashMap;

use bson::Document;
use mongocrypt::ctx::KmsProvider;
use serde::Deserialize;

use crate::{Namespace, test::util::TestClientBuilder, Client, coll::options::CollectionOptions, options::WriteConcern};

use super::test_file::Test;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AutoEncryptOpts {
    pub(crate) kms_providers: HashMap<KmsProvider, Document>,
    pub(crate) schema_map: Option<Document>,
    pub(crate) key_vault_namespace: Option<Namespace>,
    pub(crate) bypass_auto_encryption: Option<bool>,
    pub(crate) encrypted_fields_map: Option<Document>,
}

pub(crate) async fn populate_key_vault(client: &Client, kv_data: Option<&Vec<Document>>) {
    if let Some(kv_data) = kv_data {
        let datakeys = client.database("keyvault").collection_with_options::<Document>("datakeys", CollectionOptions::builder().write_concern(WriteConcern::MAJORITY).build());
        datakeys.drop(None).await.unwrap();
        datakeys.insert_many(kv_data, None).await.unwrap();
    }
}

pub(crate) fn set_auto_enc(builder: &mut TestClientBuilder, test: &Test) {
    let enc_opts = if let Some(o) = test.client_options.as_ref().and_then(|o| o.auto_encrypt_opts.as_ref()) {
        o
    } else {
        return;
    };
    let kv_namespace = enc_opts.key_vault_namespace.clone().unwrap_or_else(|| Namespace::from_str("keyvault.datakeys").unwrap());
}