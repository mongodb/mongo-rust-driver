use std::{collections::HashMap, path::PathBuf};

use bson::{Document, doc};
use mongocrypt::ctx::{KmsProvider, Algorithm};
use serde::de::DeserializeOwned;

use crate::{error::{Result, Error}, options::{ReadConcern, WriteConcern}, client_encryption::{ClientEncryption, ClientEncryptionOptions, DataKeyOptions, MasterKey, EncryptOptions, EncryptKey}, Namespace, client::{options::{AutoEncryptionOptions, TlsOptions}, csfle::options::{KmsProviders, KmsProvidersTlsOptions}}, Client};

use super::{TestClient, CLIENT_OPTIONS, LOCK};

async fn new_client() -> TestClient {
    let mut options = CLIENT_OPTIONS.get().await.clone();
    options.read_concern = Some(ReadConcern::MAJORITY);
    options.write_concern = Some(WriteConcern::MAJORITY);
    TestClient::with_options(options).await
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_key_material() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    let client = new_client().await;
    let datakeys = client.database("keyvault").collection::<Document>("datakeys");
    datakeys.drop(None).await?;
    let mut kms_providers = HashMap::new();
    kms_providers.insert(KmsProvider::Local, doc! { "key": "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk" });
    let enc = ClientEncryption::new(
        ClientEncryptionOptions::builder()
            .key_vault_client(client.into_client())
            .key_vault_namespace(Namespace::from_str("keyvault.datakeys").unwrap())
            .kms_providers(kms_providers)
            .build()
    )?;

    let key = base64::decode("xPTAjBRG5JiPm+d3fj6XLi2q5DMXUS/f1f+SMAlhhwkhDRL0kr8r9GDLIGTAGlvC+HVjSIgdL+RKwZCvpXSyxTICWSXTUYsWYPyu3IoHbuBZdmw2faM3WhcRIgbMReU5").unwrap();
    let id = enc.create_data_key(KmsProvider::Local, DataKeyOptions::builder()
        .master_key(MasterKey::Local)
        .key_material(key)
        .build()
    ).await?;
    let mut key_doc = datakeys.find_one(doc! { "_id": id.clone() }, None).await?.unwrap();
    datakeys.delete_one(doc! { "_id": id}, None).await?;
    let new_key_id = bson::Binary::from_uuid(bson::Uuid::from_bytes([0; 16]));
    key_doc.insert("_id", new_key_id.clone());
    datakeys.insert_one(key_doc, None).await?;

    let encrypted = enc.encrypt(bson::RawBson::String("test".to_string()), EncryptOptions::builder()
        .key(EncryptKey::Id(new_key_id))
        .algorithm(Algorithm::AeadAes256CbcHmacSha512Deterministic)
        .build()
    ).await?;
    let expected = base64::decode("AQAAAAAAAAAAAAAAAAAAAAACz0ZOLuuhEYi807ZXTdhbqhLaS2/t9wLifJnnNYwiw79d75QYIZ6M/aYC1h9nCzCjZ7pGUpAuNnkUhnIXM3PjrA==").unwrap();
    assert_eq!(encrypted.bytes, expected);

    Ok(())
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn data_key_double_encryption() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    let client = TestClient::new().await;
    client.database("keyvault").collection::<Document>("datakeys").drop(None).await?;
    client.database("db").collection::<Document>("coll").drop(None).await?;

    /* KMIP server:
        pip3 install pykmip
        python3 ./csfle/kms_kmip_server.py
     */
    let kms_providers: KmsProviders = from_json(&std::env::var("KMS_PROVIDERS").unwrap())?;
    let cert_dir = PathBuf::from(std::env::var("CSFLE_TLS_CERT_DIR").unwrap());
    let kmip_opts = TlsOptions::builder()
        .ca_file_path(cert_dir.join("ca.pem"))
        .cert_key_file_path(cert_dir.join("client.pem"))
        .build();
    let tls_options: KmsProvidersTlsOptions = [(KmsProvider::Kmip, kmip_opts)].into_iter().collect();
    let crypt_lib_path = std::env::var("CSFLE_SHARED_LIB_PATH").unwrap();
    let schema_map: HashMap<String, Document> = [
        ("db.coll".to_string(), doc! {
            "bsonType": "object",
            "properties": {
                "encrypted_placeholder": {
                    "encrypt": {
                        "keyId": "/placeholder",
                        "bsonType": "string",
                        "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                    }
                }
            }
        })
    ].into_iter().collect();
    let enc_opts = AutoEncryptionOptions::builder()
        .key_vault_namespace(Namespace::from_str("keyvault.datakeys").unwrap())
        .kms_providers(kms_providers)
        .schema_map(schema_map)
        .tls_options(tls_options)
        .extra_options(doc! { "cryptSharedLibPath": crypt_lib_path })
        .build();
    let _client_encrypted = Client::with_encryption_options(CLIENT_OPTIONS.get().await.clone(), enc_opts).await?;

    Ok(())
}

fn from_json<T: DeserializeOwned>(text: &str) -> Result<T> {
    let json: serde_json::Value = serde_json::from_str(text).map_err(|e| Error::invalid_argument(format!("json parse failure: {}", e)))?;
    let bson: bson::Bson = json.try_into().map_err(|e| Error::invalid_argument(format!("bson parse failure: {}", e)))?;
    Ok(bson::from_bson(bson)?)
}