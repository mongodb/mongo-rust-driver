use std::{collections::HashMap, path::PathBuf};

use bson::{Document, doc, spec::BinarySubtype, Bson, RawBson};
use futures_util::TryStreamExt;
use mongocrypt::ctx::{KmsProvider, Algorithm};
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{options::{ReadConcern, WriteConcern}, client_encryption::{ClientEncryption, ClientEncryptionOptions, DataKeyOptions, MasterKey, EncryptOptions, EncryptKey}, Namespace, client::{options::{AutoEncryptionOptions, TlsOptions}, csfle::options::{KmsProviders, KmsProvidersTlsOptions}}, Client};

use super::{TestClient, CLIENT_OPTIONS, LOCK, EventClient};

#[derive(Error, Debug)]
enum TestError {
    #[error("mongodb error: {0}")]
    Mongodb(#[from] crate::error::Error),
    #[error("serde_json error: {0}")]
    SerdeJson(#[from] serde_json::error::Error),
    #[error("bson error: {0}")]
    BsonExtjson(#[from] bson::extjson::de::Error),
    #[error("bson error: {0}")]
    Bson(#[from] bson::de::Error),
    #[error("access error: {0}")]
    ValueAccess(#[from] bson::document::ValueAccessError),
}

type Result<T> = std::result::Result<T, TestError>;

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
    let id = enc.create_data_key(&KmsProvider::Local, DataKeyOptions::builder()
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

    // Setup: drop stale data.
    let client = EventClient::new().await;
    client.database("keyvault").collection::<Document>("datakeys").drop(None).await?;
    client.database("db").collection::<Document>("coll").drop(None).await?;

    /* KMIP server:
        pip3 install pykmip
        # in drivers-evergreen-tools/.evergreen
        python3 ./csfle/kms_kmip_server.py
     */
    // Setup: build options for test environment.
    let kv_namespace = Namespace::from_str("keyvault.datakeys").unwrap();
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

    // Setup: client with auto encryption.
    let auto_enc_opts = AutoEncryptionOptions::builder()
        .key_vault_namespace(kv_namespace.clone())
        .kms_providers(kms_providers.clone())
        .schema_map(schema_map)
        .tls_options(tls_options.clone())
        .extra_options(doc! { "cryptSharedLibPath": crypt_lib_path })
        .build();
    let client_encrypted = Client::with_encryption_options(CLIENT_OPTIONS.get().await.clone(), auto_enc_opts).await?;

    // Setup: manual encryption.
    let enc_opts = ClientEncryptionOptions::builder()
        .key_vault_namespace(kv_namespace)
        .key_vault_client(client.clone().into_client())
        .kms_providers(kms_providers)
        .tls_options(tls_options)
        .build();
    let client_encryption = ClientEncryption::new(enc_opts)?;

    // Testing each provider:
    let provider_keys = [
        (
            KmsProvider::Aws,
            MasterKey::Aws {
                region: "us-east-1".to_string(),
                key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0".to_string(),
                endpoint: None,
            }
        ),
        (
            KmsProvider::Azure,
            MasterKey::Azure {
                key_vault_endpoint: "key-vault-csfle.vault.azure.net".to_string(),
                key_name: "key-name-csfle".to_string(),
                key_version: None,
            }
        ),
        (
            KmsProvider::Gcp,
            MasterKey::Gcp {
                project_id: "devprod-drivers".to_string(),
                location: "global".to_string(),
                key_ring: "key-ring-csfle".to_string(),
                key_name: "key-name-csfle".to_string(),
                key_version: None,
                endpoint: None,
            }
        ),
        (KmsProvider::Local, MasterKey::Local),
        (KmsProvider::Kmip, MasterKey::Kmip { key_id: None, endpoint: None }),
    ];
    for (provider, master_key) in provider_keys {
        // Create a data key
        let datakey_id = client_encryption.create_data_key(&provider, DataKeyOptions::builder()
            .key_alt_names(vec![format!("{}_altname", provider.name())])
            .master_key(master_key)  // varies by provider
        .build()).await?;
        assert_eq!(datakey_id.subtype, BinarySubtype::Uuid);
        let docs: Vec<_> = client.database("keyvault").collection::<Document>("datakeys").find(doc! { "_id": datakey_id.clone() }, None).await?.try_collect().await?;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get_document("masterKey")?.get_str("provider")?, provider.name());
        let events = client.get_command_started_events(&["insert"]); 
        let found = try_any(&events, |ev| {
            let cmd = &ev.command;
            if cmd.get_document("writeConcern")?.get_str("w")? != "majority" {
                return Ok(false);
            }
            Ok(any(cmd.get_array("documents")?, |doc| {
                matches!(
                    doc.as_document().and_then(|d| d.get("_id")),
                    Some(Bson::Binary(id)) if id == &datakey_id
                )
            }))
        })?;
        assert!(found, "no valid event found in {:?}", events);

        // Manually encrypt a value and automatically decrypt it.
        let encrypted = client_encryption.encrypt(
            RawBson::String(format!("hello {}", provider.name())),
            EncryptOptions::builder()
                .key(EncryptKey::Id(datakey_id))
                .algorithm(Algorithm::AeadAes256CbcHmacSha512Deterministic)
                .build(),
        ).await?;
        assert_eq!(encrypted.subtype, BinarySubtype::Encrypted);
        let coll = client_encrypted.database("db").collection::<Document>("coll");
        coll.insert_one(doc! { "_id": provider.name(), "value": encrypted.clone() }, None).await?;
        let found = coll.find_one(doc! { "_id": provider.name() }, None).await?;
        assert_eq!(
            found.as_ref().and_then(|doc| doc.get("value")),
            Some(&Bson::String(format!("hello {}", provider.name()))),
        );

        // Manually encrypt a value via key alt name.
        let other_encrypted = client_encryption.encrypt(
            RawBson::String(format!("hello {}", provider.name())),
            EncryptOptions::builder()
                .key(EncryptKey::AltName(format!("{}_altname", provider.name())))
                .algorithm(Algorithm::AeadAes256CbcHmacSha512Deterministic)
                .build(),
        ).await?;
        assert_eq!(other_encrypted.subtype, BinarySubtype::Encrypted);
        assert_eq!(other_encrypted.bytes, encrypted.bytes);

        // Attempt to auto-encrypt an already encrypted field.
        let result = coll.insert_one(doc! { "encrypted_placeholder": encrypted }, None).await;
        assert!(result.is_err());
    }

    Ok(())
}

fn from_json<T: DeserializeOwned>(text: &str) -> Result<T> {
    let json: serde_json::Value = serde_json::from_str(text)?;
    let bson: bson::Bson = json.try_into()?;
    Ok(bson::from_bson(bson)?)
}

fn any<T>(values: &[T], mut pred: impl FnMut(&T) -> bool) -> bool {
    for value in values {
        if pred(value) {
            return true
        }
    }
    false
}

fn try_any<T>(values: &[T], mut pred: impl FnMut(&T) -> Result<bool>) -> Result<bool> {
    for value in values {
        if pred(value)? {
            return Ok(true)
        }
    }
    Ok(false)
}