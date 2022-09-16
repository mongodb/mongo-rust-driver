use std::{collections::HashMap, path::PathBuf};

use bson::{Document, doc, spec::BinarySubtype, Bson, RawBson};
use futures_util::TryStreamExt;
use mongocrypt::ctx::{KmsProvider, Algorithm};
use serde::de::DeserializeOwned;
use lazy_static::lazy_static;

use crate::{options::{ReadConcern, WriteConcern}, client_encryption::{ClientEncryption, ClientEncryptionOptions, DataKeyOptions, MasterKey, EncryptOptions, EncryptKey}, Namespace, client::{options::{AutoEncryptionOptions, TlsOptions}, csfle::options::{KmsProviders, KmsProvidersTlsOptions}, auth::Credential}, Client, db::options::CreateCollectionOptions, coll::options::CollectionOptions, Collection};

use super::{CLIENT_OPTIONS, LOCK, EventClient};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn init_client() -> Result<(EventClient, Collection<Document>)> {
    let client = EventClient::new().await;
    let datakeys = client.database("keyvault").collection_with_options::<Document>(
        "datakeys",
        CollectionOptions::builder()
            .read_concern(ReadConcern::MAJORITY)
            .write_concern(WriteConcern::MAJORITY)
            .build(),
    );
    datakeys.drop(None).await?;
    client.database("db").collection::<Document>("coll").drop(None).await?;
    Ok((client, datakeys))
}

lazy_static! {
    static ref KMS_PROVIDERS: KmsProviders = from_json(&std::env::var("KMS_PROVIDERS").unwrap()).unwrap();
    static ref LOCAL_KMS: KmsProviders = {
        let mut out = KMS_PROVIDERS.clone();
        out.retain(|k, _| *k == KmsProvider::Local);
        out
    };
    static ref EXTRA_OPTIONS: Document = doc! { "cryptSharedLibPath": std::env::var("CSFLE_SHARED_LIB_PATH").unwrap() };
    static ref KV_NAMESPACE: Namespace = Namespace::from_str("keyvault.datakeys").unwrap();
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_key_material() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    let (client, datakeys) = init_client().await?;
    let enc = ClientEncryption::new(
        ClientEncryptionOptions::builder()
            .key_vault_client(client.into_client())
            .key_vault_namespace(KV_NAMESPACE.clone())
            .kms_providers(LOCAL_KMS.clone())
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
    let (client, _) = init_client().await?;

    /* KMIP server:
        pip3 install pykmip
        # in drivers-evergreen-tools/.evergreen
        python3 ./csfle/kms_kmip_server.py
     */
    // Setup: build options for test environment.
    let cert_dir = PathBuf::from(std::env::var("CSFLE_TLS_CERT_DIR").unwrap());
    let kmip_opts = TlsOptions::builder()
        .ca_file_path(cert_dir.join("ca.pem"))
        .cert_key_file_path(cert_dir.join("client.pem"))
        .build();
    let tls_options: KmsProvidersTlsOptions = [(KmsProvider::Kmip, kmip_opts)].into_iter().collect();
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
        .key_vault_namespace(KV_NAMESPACE.clone())
        .kms_providers(KMS_PROVIDERS.clone())
        .schema_map(schema_map)
        .tls_options(tls_options.clone())
        .extra_options(EXTRA_OPTIONS.clone())
        .build();
    let client_encrypted = Client::with_encryption_options(CLIENT_OPTIONS.get().await.clone(), auto_enc_opts).await?;

    // Setup: manual encryption.
    let enc_opts = ClientEncryptionOptions::builder()
        .key_vault_namespace(KV_NAMESPACE.clone())
        .key_vault_client(client.clone().into_client())
        .kms_providers(KMS_PROVIDERS.clone())
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn external_key_vault() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    for with_external_key_vault in [false, true] {
        // Setup: initialize db.
        let (client, datakeys) = init_client().await?;
        datakeys.insert_one(load_testdata("external-key.json")?, None).await?;
    
        // Setup: test options.
        let schema_map: HashMap<String, Document> = [
            ("db.coll".to_string(), load_testdata("external-schema.json")?)
        ].into_iter().collect();
        let kv_client = if with_external_key_vault {
            let mut opts = CLIENT_OPTIONS.get().await.clone();
            opts.credential = Some(
                Credential::builder()
                    .username("fake-user".to_string())
                    .password("fake-pwd".to_string())
                    .build()
            );
            Some(Client::with_options(opts)?)
        } else {
            None
        };
    
        // Setup: encrypted client.
        let auto_enc_opts = AutoEncryptionOptions::builder()
            .key_vault_namespace(KV_NAMESPACE.clone())
            .key_vault_client(kv_client.clone())
            .kms_providers(LOCAL_KMS.clone())
            .schema_map(schema_map)
            .extra_options(EXTRA_OPTIONS.clone())
            .build();
        let client_encrypted = Client::with_encryption_options(CLIENT_OPTIONS.get().await.clone(), auto_enc_opts).await?;

        // Setup: manual encryption.
        let enc_opts = ClientEncryptionOptions::builder()
            .key_vault_namespace(KV_NAMESPACE.clone())
            .key_vault_client(kv_client.unwrap_or_else(|| client.into_client()))
            .kms_providers(LOCAL_KMS.clone())
            .build();
        let client_encryption = ClientEncryption::new(enc_opts)?;

        // Test: encrypted client.
        let result = client_encrypted.database("db").collection::<Document>("coll").insert_one(doc! { "encrypted": "test" }, None).await;
        if with_external_key_vault {
            assert!(result.is_err());
        } else {
            assert!(result.is_ok(), "unexpected error: {}", result.err().unwrap());
        }
        // Test: manual encryption.
        let key_id = bson::Binary {
            subtype: BinarySubtype::Uuid,
            bytes: base64::decode("LOCALAAAAAAAAAAAAAAAAA==")?,
        };
        let result = client_encryption.encrypt(
            RawBson::String("test".to_string()),
            EncryptOptions::builder()
                .key(EncryptKey::Id(key_id))
                .algorithm(Algorithm::AeadAes256CbcHmacSha512Deterministic)
                .build(),
        ).await;
        if with_external_key_vault {
            assert!(result.is_err());
        } else {
            assert!(result.is_ok(), "unexpected error: {}", result.err().unwrap());
        }
    }

    Ok(())
}

fn load_testdata(name: &str) -> Result<Document> {
    let path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "src/test/csfle_data",
        name,
    ].iter().collect();
    let text = std::fs::read_to_string(path)?;
    from_json(&text)
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn bson_size_limits() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    // Setup: db initialization.
    let (client, datakeys) = init_client().await?;
    client.database("db").create_collection("coll", CreateCollectionOptions::builder()
        .validator(doc! { "$jsonSchema": load_testdata("limits-schema.json")? })
        .build()
    ).await?;
    datakeys.insert_one(load_testdata("limits-key.json")?, None).await?;

    // Setup: encrypted client.
    let auto_enc_opts = AutoEncryptionOptions::builder()
        .key_vault_namespace(KV_NAMESPACE.clone())
        .kms_providers(LOCAL_KMS.clone())
        .extra_options(EXTRA_OPTIONS.clone())
        .build();
    let client_encrypted = Client::with_encryption_options(CLIENT_OPTIONS.get().await.clone(), auto_enc_opts).await?;
    let coll = client_encrypted.database("db").collection::<Document>("coll");

    // Tests
    let mut value = String::new();
    // Manually push characters; constructing the array directly explodes the stack.
    for _ in 0..2097152 {
        value.push('a');
    }
    coll.insert_one(
        doc! {
            "_id": "over_2mib_under_16mib",
            "unencrypted": value,
        },
        None,
    ).await?;

    let mut doc: Document = load_testdata("limits-doc.json")?;
    doc.insert("_id", "encryption_exceeds_2mib");
    let mut value = String::new();
    for _ in 0..(2097152 - 2000) {
        value.push('a');
    }
    doc.insert("unencrypted", value);
    coll.insert_one(doc, None).await?;

    // TODO(RUST-583) Test bulk write limit behavior

    let mut doc: Document = load_testdata("limits-doc.json")?;
    doc.insert("_id", "encryption_exceeds_16mib");
    let mut value = String::new();
    for _ in 0..(16777216 - 2000) {
        value.push('a');
    }
    doc.insert("unencrypted", value);
    let result = coll.insert_one(doc, None).await;
    assert!(result.is_err());

    Ok(())
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn views_prohibited() -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    // Setup: db initialization.
    let (client, _) = init_client().await?;
    client.database("db").collection::<Document>("view").drop(None).await?;
    client.database("db").run_command(doc! { "create": "view", "viewOn": "coll" }, None).await?;

    // Setup: encrypted client.
    let auto_enc_opts = AutoEncryptionOptions::builder()
        .key_vault_namespace(KV_NAMESPACE.clone())
        .kms_providers(LOCAL_KMS.clone())
        .extra_options(EXTRA_OPTIONS.clone())
        .build();
    let client_encrypted = Client::with_encryption_options(CLIENT_OPTIONS.get().await.clone(), auto_enc_opts).await?;

    // Test: auto encryption fails on a view
    let result = client_encrypted.database("db").collection::<Document>("view").insert_one(doc! { }, None).await;
    let err = result.unwrap_err();
    assert!(err.to_string().contains("cannot auto encrypt a view"), "unexpected error: {}", err);

    Ok(())
}