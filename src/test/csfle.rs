use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use bson::{
    doc,
    spec::{BinarySubtype, ElementType},
    Binary,
    Bson,
    Document,
    RawBson,
};
use futures_util::TryStreamExt;
use lazy_static::lazy_static;
use mongocrypt::ctx::{Algorithm, KmsProvider};

use crate::{
    client::{
        auth::Credential,
        csfle::options::{KmsProviders, KmsProvidersTlsOptions},
        options::{AutoEncryptionOptions, TlsOptions},
    },
    client_encryption::{
        ClientEncryption,
        ClientEncryptionOptions,
        DataKeyOptions,
        EncryptKey,
        EncryptOptions,
        MasterKey,
    },
    coll::options::{
        CollectionOptions,
        CreateIndexOptions,
        DropCollectionOptions,
        InsertOneOptions,
    },
    db::options::CreateCollectionOptions,
    error::{ErrorKind, WriteError, WriteFailure},
    event::command::{
        CommandEventHandler,
        CommandFailedEvent,
        CommandStartedEvent,
        CommandSucceededEvent,
    },
    options::{IndexOptions, ReadConcern, WriteConcern},
    test::{Event, EventHandler, SdamEvent},
    Client,
    Collection,
    IndexModel,
    Namespace,
};

use super::{
    log_uncaptured,
    EventClient,
    FailCommandOptions,
    FailPoint,
    FailPointMode,
    TestClient,
    CLIENT_OPTIONS,
    LOCK,
};

type Result<T> = anyhow::Result<T>;

async fn init_client() -> Result<(EventClient, Collection<Document>)> {
    let client = EventClient::new().await;
    let datakeys = client
        .database("keyvault")
        .collection_with_options::<Document>(
            "datakeys",
            CollectionOptions::builder()
                .read_concern(ReadConcern::MAJORITY)
                .write_concern(WriteConcern::MAJORITY)
                .build(),
        );
    datakeys.drop(None).await?;
    client
        .database("db")
        .collection::<Document>("coll")
        .drop(None)
        .await?;
    Ok((client, datakeys))
}

lazy_static! {
    static ref KMS_PROVIDERS: KmsProviders = serde_json::from_str(&std::env::var("KMS_PROVIDERS").unwrap()).unwrap();
    static ref LOCAL_KMS: KmsProviders = {
        let mut out = KMS_PROVIDERS.clone();
        out.retain(|k, _| *k == KmsProvider::Local);
        out
    };
    static ref EXTRA_OPTIONS: Document = doc! {
        "cryptSharedLibPath": std::env::var("CSFLE_SHARED_LIB_PATH").unwrap(),
        "mongocryptdBypassSpawn": true,
    };
    static ref KV_NAMESPACE: Namespace = Namespace::from_str("keyvault.datakeys").unwrap();
    static ref KMIP_TLS_OPTIONS: KmsProvidersTlsOptions = {
            /* If these options are used, the test will need a running KMIP server:
                pip3 install pykmip
                # in drivers-evergreen-tools/.evergreen
                python3 ./csfle/kms_kmip_server.py
            */
        let cert_dir = PathBuf::from(std::env::var("CSFLE_TLS_CERT_DIR").unwrap());
        let kmip_opts = TlsOptions::builder()
            .ca_file_path(cert_dir.join("ca.pem"))
            .cert_key_file_path(cert_dir.join("client.pem"))
            .build();
        let tls_options: KmsProvidersTlsOptions = [(KmsProvider::Kmip, kmip_opts)].into_iter().collect();
        tls_options
    };
    static ref DISABLE_CRYPT_SHARED: bool = std::env::var("DISABLE_CRYPT_SHARED")
        .map_or(false, |s| s == "true");
}

fn check_env(name: &str, kmip: bool) -> bool {
    if std::env::var("KMS_PROVIDERS").is_err() {
        log_uncaptured(format!(
            "skipping csfle test {}: no kms providers configured",
            name
        ));
        return false;
    }
    if kmip {
        #[cfg(not(feature = "openssl-tls"))]
        {
            // rustls is incompatible with the driver-tools kmip server.
            log_uncaptured(format!("skipping {}: KMIP requires openssl", name));
            return false;
        }
    }
    true
}

// Prose test 1. Custom Key Material Test
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_key_material() -> Result<()> {
    if !check_env("custom_key_material", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let (client, datakeys) = init_client().await?;
    let enc = ClientEncryption::new(
        ClientEncryptionOptions::builder()
            .key_vault_client(client.into_client())
            .key_vault_namespace(KV_NAMESPACE.clone())
            .kms_providers(LOCAL_KMS.clone())
            .build(),
    )?;

    let key = base64::decode(
        "xPTAjBRG5JiPm+d3fj6XLi2q5DMXUS/f1f+SMAlhhwkhDRL0kr8r9GDLIGTAGlvC+HVjSIgdL+RKw\
         ZCvpXSyxTICWSXTUYsWYPyu3IoHbuBZdmw2faM3WhcRIgbMReU5").unwrap();
    let id = enc
        .create_data_key(
            &KmsProvider::Local,
            DataKeyOptions::builder()
                .master_key(MasterKey::Local)
                .key_material(key)
                .build(),
        )
        .await?;
    let mut key_doc = datakeys
        .find_one(doc! { "_id": id.clone() }, None)
        .await?
        .unwrap();
    datakeys.delete_one(doc! { "_id": id}, None).await?;
    let new_key_id = bson::Binary::from_uuid(bson::Uuid::from_bytes([0; 16]));
    key_doc.insert("_id", new_key_id.clone());
    datakeys.insert_one(key_doc, None).await?;

    let encrypted = enc
        .encrypt(
            bson::RawBson::String("test".to_string()),
            EncryptOptions::builder()
                .key(EncryptKey::Id(new_key_id))
                .algorithm(Algorithm::AeadAes256CbcHmacSha512Deterministic)
                .build(),
        )
        .await?;
    let expected = base64::decode(
        "AQAAAAAAAAAAAAAAAAAAAAACz0ZOLuuhEYi807ZXTdhbqhLaS2/t9wLifJnnNYwiw79d75QYIZ6M/\
         aYC1h9nCzCjZ7pGUpAuNnkUhnIXM3PjrA==",
    )
    .unwrap();
    assert_eq!(encrypted.bytes, expected);

    Ok(())
}

// Prose test 2. Data Key and Double Encryption
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn data_key_double_encryption() -> Result<()> {
    if !check_env("data_key_double_encryption", true) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    // Setup: drop stale data.
    let (client, _) = init_client().await?;

    // Setup: client with auto encryption.
    let schema_map: HashMap<String, Document> = [(
        "db.coll".to_string(),
        doc! {
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
        },
    )]
    .into_iter()
    .collect();
    let auto_enc_opts = AutoEncryptionOptions::builder()
        .key_vault_namespace(KV_NAMESPACE.clone())
        .kms_providers(KMS_PROVIDERS.clone())
        .schema_map(schema_map)
        .tls_options(KMIP_TLS_OPTIONS.clone())
        .extra_options(EXTRA_OPTIONS.clone())
        .disable_crypt_shared(DISABLE_CRYPT_SHARED.clone())
        .build();
    let client_encrypted =
        Client::with_encryption_options(CLIENT_OPTIONS.get().await.clone(), auto_enc_opts).await?;

    // Setup: manual encryption.
    let enc_opts = ClientEncryptionOptions::builder()
        .key_vault_namespace(KV_NAMESPACE.clone())
        .key_vault_client(client.clone().into_client())
        .kms_providers(KMS_PROVIDERS.clone())
        .tls_options(KMIP_TLS_OPTIONS.clone())
        .build();
    let client_encryption = ClientEncryption::new(enc_opts)?;

    // Testing each provider:
    let mut events = client.subscribe_to_events();
    let provider_keys = [
        (
            KmsProvider::Aws,
            MasterKey::Aws {
                region: "us-east-1".to_string(),
                key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                    .to_string(),
                endpoint: None,
            },
        ),
        (
            KmsProvider::Azure,
            MasterKey::Azure {
                key_vault_endpoint: "key-vault-csfle.vault.azure.net".to_string(),
                key_name: "key-name-csfle".to_string(),
                key_version: None,
            },
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
            },
        ),
        (KmsProvider::Local, MasterKey::Local),
        (
            KmsProvider::Kmip,
            MasterKey::Kmip {
                key_id: None,
                endpoint: None,
            },
        ),
    ];
    for (provider, master_key) in provider_keys {
        // Create a data key
        let datakey_id = client_encryption
            .create_data_key(
                &provider,
                DataKeyOptions::builder()
                    .key_alt_names(vec![format!("{}_altname", provider.name())])
                    .master_key(master_key)  // varies by provider
                .build(),
            )
            .await?;
        assert_eq!(datakey_id.subtype, BinarySubtype::Uuid);
        let docs: Vec<_> = client
            .database("keyvault")
            .collection::<Document>("datakeys")
            .find(doc! { "_id": datakey_id.clone() }, None)
            .await?
            .try_collect()
            .await?;
        assert_eq!(docs.len(), 1);
        assert_eq!(
            docs[0].get_document("masterKey")?.get_str("provider")?,
            provider.name()
        );
        let found = events
            .wait_for_event(
                Duration::from_millis(500),
                ok_pred(|ev| {
                    let ev = match ev.as_command_started_event() {
                        Some(e) => e,
                        None => return Ok(false),
                    };
                    if ev.command_name != "insert" {
                        return Ok(false);
                    }
                    let cmd = &ev.command;
                    if cmd.get_document("writeConcern")?.get_str("w")? != "majority" {
                        return Ok(false);
                    }
                    Ok(cmd.get_array("documents")?.iter().any(|doc| {
                        matches!(
                            doc.as_document().and_then(|d| d.get("_id")),
                            Some(Bson::Binary(id)) if id == &datakey_id
                        )
                    }))
                }),
            )
            .await;
        assert!(found.is_some(), "no valid event found in {:?}", events);

        // Manually encrypt a value and automatically decrypt it.
        let encrypted = client_encryption
            .encrypt(
                RawBson::String(format!("hello {}", provider.name())),
                EncryptOptions::builder()
                    .key(EncryptKey::Id(datakey_id))
                    .algorithm(Algorithm::AeadAes256CbcHmacSha512Deterministic)
                    .build(),
            )
            .await?;
        assert_eq!(encrypted.subtype, BinarySubtype::Encrypted);
        let coll = client_encrypted
            .database("db")
            .collection::<Document>("coll");
        coll.insert_one(
            doc! { "_id": provider.name(), "value": encrypted.clone() },
            None,
        )
        .await?;
        let found = coll.find_one(doc! { "_id": provider.name() }, None).await?;
        assert_eq!(
            found.as_ref().and_then(|doc| doc.get("value")),
            Some(&Bson::String(format!("hello {}", provider.name()))),
        );

        // Manually encrypt a value via key alt name.
        let other_encrypted = client_encryption
            .encrypt(
                RawBson::String(format!("hello {}", provider.name())),
                EncryptOptions::builder()
                    .key(EncryptKey::AltName(format!("{}_altname", provider.name())))
                    .algorithm(Algorithm::AeadAes256CbcHmacSha512Deterministic)
                    .build(),
            )
            .await?;
        assert_eq!(other_encrypted.subtype, BinarySubtype::Encrypted);
        assert_eq!(other_encrypted.bytes, encrypted.bytes);

        // Attempt to auto-encrypt an already encrypted field.
        let result = coll
            .insert_one(doc! { "encrypted_placeholder": encrypted }, None)
            .await;
        let err = result.unwrap_err();
        assert!(
            matches!(*err.kind, ErrorKind::Csfle(..)),
            "unexpected error: {}",
            err
        );
    }

    Ok(())
}

fn ok_pred(mut f: impl FnMut(&Event) -> Result<bool>) -> impl FnMut(&Event) -> bool {
    move |ev| f(ev).unwrap_or(false)
}

// TODO RUST-1225: replace this with built-in BSON support.
fn base64_uuid(bytes: impl AsRef<str>) -> Result<bson::Binary> {
    Ok(bson::Binary {
        subtype: BinarySubtype::Uuid,
        bytes: base64::decode(bytes.as_ref())?,
    })
}

// Prose test 3. External Key Vault Test
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn external_key_vault() -> Result<()> {
    if !check_env("external_key_vault", true) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    for with_external_key_vault in [false, true] {
        // Setup: initialize db.
        let (client, datakeys) = init_client().await?;
        datakeys
            .insert_one(load_testdata("external-key.json")?, None)
            .await?;

        // Setup: test options.
        let schema_map: HashMap<String, Document> = [(
            "db.coll".to_string(),
            load_testdata("external-schema.json")?,
        )]
        .into_iter()
        .collect();
        let kv_client = if with_external_key_vault {
            let mut opts = CLIENT_OPTIONS.get().await.clone();
            opts.credential = Some(
                Credential::builder()
                    .username("fake-user".to_string())
                    .password("fake-pwd".to_string())
                    .build(),
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
            .disable_crypt_shared(DISABLE_CRYPT_SHARED.clone())
            .build();
        let client_encrypted =
            Client::with_encryption_options(CLIENT_OPTIONS.get().await.clone(), auto_enc_opts)
                .await?;

        // Setup: manual encryption.
        let enc_opts = ClientEncryptionOptions::builder()
            .key_vault_namespace(KV_NAMESPACE.clone())
            .key_vault_client(kv_client.unwrap_or_else(|| client.into_client()))
            .kms_providers(LOCAL_KMS.clone())
            .build();
        let client_encryption = ClientEncryption::new(enc_opts)?;

        // Test: encrypted client.
        let result = client_encrypted
            .database("db")
            .collection::<Document>("coll")
            .insert_one(doc! { "encrypted": "test" }, None)
            .await;
        if with_external_key_vault {
            let err = result.unwrap_err();
            assert!(err.is_auth_error(), "unexpected error: {}", err);
        } else {
            assert!(
                result.is_ok(),
                "unexpected error: {}",
                result.err().unwrap()
            );
        }
        // Test: manual encryption.
        let result = client_encryption
            .encrypt(
                RawBson::String("test".to_string()),
                EncryptOptions::builder()
                    .key(EncryptKey::Id(base64_uuid("LOCALAAAAAAAAAAAAAAAAA==")?))
                    .algorithm(Algorithm::AeadAes256CbcHmacSha512Deterministic)
                    .build(),
            )
            .await;
        if with_external_key_vault {
            let err = result.unwrap_err();
            assert!(err.is_auth_error(), "unexpected error: {}", err);
        } else {
            assert!(
                result.is_ok(),
                "unexpected error: {}",
                result.err().unwrap()
            );
        }
    }

    Ok(())
}

fn load_testdata_raw(name: &str) -> Result<String> {
    let path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "src/test/spec/json/client-side-encryption/testdata",
        name,
    ]
    .iter()
    .collect();
    Ok(std::fs::read_to_string(path)?)
}

fn load_testdata(name: &str) -> Result<Document> {
    Ok(serde_json::from_str(&load_testdata_raw(name)?)?)
}

// Prose test 4. BSON Size Limits and Batch Splitting
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn bson_size_limits() -> Result<()> {
    if !check_env("bson_size_limits", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    // Setup: db initialization.
    let (client, datakeys) = init_client().await?;
    client
        .database("db")
        .create_collection(
            "coll",
            CreateCollectionOptions::builder()
                .validator(doc! { "$jsonSchema": load_testdata("limits-schema.json")? })
                .build(),
        )
        .await?;
    datakeys
        .insert_one(load_testdata("limits-key.json")?, None)
        .await?;

    // Setup: encrypted client.
    let mut opts = CLIENT_OPTIONS.get().await.clone();
    let handler = Arc::new(EventHandler::new());
    let mut events = handler.subscribe();
    opts.command_event_handler = Some(handler.clone());
    let auto_enc_opts = AutoEncryptionOptions::builder()
        .key_vault_namespace(KV_NAMESPACE.clone())
        .kms_providers(LOCAL_KMS.clone())
        .extra_options(EXTRA_OPTIONS.clone())
        .disable_crypt_shared(DISABLE_CRYPT_SHARED.clone())
        .build();
    let client_encrypted = Client::with_encryption_options(opts, auto_enc_opts).await?;
    let coll = client_encrypted
        .database("db")
        .collection::<Document>("coll");

    // Tests
    // Test operation 1
    coll.insert_one(
        doc! {
            "_id": "over_2mib_under_16mib",
            "unencrypted": "a".repeat(2097152),
        },
        None,
    )
    .await?;

    // Test operation 2
    let mut doc: Document = load_testdata("limits-doc.json")?;
    doc.insert("_id", "encryption_exceeds_2mib");
    doc.insert("unencrypted", "a".repeat(2_097_152 - 2_000));
    coll.insert_one(doc, None).await?;

    // Test operation 3
    let value = "a".repeat(2_097_152);
    events.clear_events(Duration::from_millis(500)).await;
    coll.insert_many(
        vec![
            doc! {
                "_id": "over_2mib_1",
                "unencrypted": value.clone(),
            },
            doc! {
                "_id": "over_2mib_2",
                "unencrypted": value,
            },
        ],
        None,
    )
    .await?;
    let inserts = events
        .collect_events(Duration::from_millis(500), |ev| {
            let ev = match ev.as_command_started_event() {
                Some(e) => e,
                None => return false,
            };
            ev.command_name == "insert"
        })
        .await;
    assert_eq!(2, inserts.len());

    // Test operation 4
    let mut doc = load_testdata("limits-doc.json")?;
    doc.insert("_id", "encryption_exceeds_2mib_1");
    doc.insert("unencrypted", "a".repeat(2_097_152 - 2_000));
    let mut doc2 = doc.clone();
    doc2.insert("_id", "encryption_exceeds_2mib_2");
    events.clear_events(Duration::from_millis(500)).await;
    coll.insert_many(vec![doc, doc2], None).await?;
    let inserts = events
        .collect_events(Duration::from_millis(500), |ev| {
            let ev = match ev.as_command_started_event() {
                Some(e) => e,
                None => return false,
            };
            ev.command_name == "insert"
        })
        .await;
    assert_eq!(2, inserts.len());

    // Test operation 5
    let doc = doc! {
        "_id": "under_16mib",
        "unencrypted": "a".repeat(16_777_216 - 2_000),
    };
    coll.insert_one(doc, None).await?;

    // Test operation 6
    let mut doc: Document = load_testdata("limits-doc.json")?;
    doc.insert("_id", "encryption_exceeds_16mib");
    doc.insert("unencrypted", "a".repeat(16_777_216 - 2_000));
    let result = coll.insert_one(doc, None).await;
    let err = result.unwrap_err();
    assert!(
        matches!(*err.kind, ErrorKind::Write(_)),
        "unexpected error: {}",
        err
    );

    Ok(())
}

// Prose test 5. Views Are Prohibited
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn views_prohibited() -> Result<()> {
    if !check_env("views_prohibited", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    // Setup: db initialization.
    let (client, _) = init_client().await?;
    client
        .database("db")
        .collection::<Document>("view")
        .drop(None)
        .await?;
    client
        .database("db")
        .create_collection(
            "view",
            CreateCollectionOptions::builder()
                .view_on("coll".to_string())
                .build(),
        )
        .await?;

    // Setup: encrypted client.
    let auto_enc_opts = AutoEncryptionOptions::builder()
        .key_vault_namespace(KV_NAMESPACE.clone())
        .kms_providers(LOCAL_KMS.clone())
        .extra_options(EXTRA_OPTIONS.clone())
        .disable_crypt_shared(DISABLE_CRYPT_SHARED.clone())
        .build();
    let client_encrypted =
        Client::with_encryption_options(CLIENT_OPTIONS.get().await.clone(), auto_enc_opts).await?;

    // Test: auto encryption fails on a view
    let result = client_encrypted
        .database("db")
        .collection::<Document>("view")
        .insert_one(doc! {}, None)
        .await;
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("cannot auto encrypt a view"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

macro_rules! failure {
    ($($arg:tt)*) => {{
        crate::error::Error::internal(format!($($arg)*)).into()
    }}
}

// TODO RUST-36: use the full corpus with decimal128.
fn load_corpus_nodecimal128(name: &str) -> Result<Document> {
    let json: serde_json::Value = serde_json::from_str(&load_testdata_raw(name)?)?;
    let mut new_obj = serde_json::Map::new();
    let decimal = serde_json::Value::String("decimal".to_string());
    for (name, value) in json.as_object().expect("expected object") {
        if value["type"] == decimal {
            continue;
        }
        new_obj.insert(name.clone(), value.clone());
    }
    let bson: bson::Bson = serde_json::Value::Object(new_obj).try_into()?;
    match bson {
        bson::Bson::Document(d) => Ok(d),
        _ => Err(failure!("expected document, got {:?}", bson)),
    }
}

// Prose test 6. Corpus Test (collection schema)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn corpus_coll_schema() -> Result<()> {
    if !check_env("corpus_coll_schema", true) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;
    run_corpus_test(false).await?;
    Ok(())
}

// Prose test 6. Corpus Test (local schema)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn corpus_local_schema() -> Result<()> {
    if !check_env("corpus_local_schema", true) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;
    run_corpus_test(true).await?;
    Ok(())
}

async fn run_corpus_test(local_schema: bool) -> Result<()> {
    // Setup: db initialization.
    let (client, datakeys) = init_client().await?;
    let schema = load_testdata("corpus/corpus-schema.json")?;
    let coll_opts = if local_schema {
        None
    } else {
        Some(
            CreateCollectionOptions::builder()
                .validator(doc! { "$jsonSchema": schema.clone() })
                .build(),
        )
    };
    client
        .database("db")
        .create_collection("coll", coll_opts)
        .await?;
    for f in [
        "corpus/corpus-key-local.json",
        "corpus/corpus-key-aws.json",
        "corpus/corpus-key-azure.json",
        "corpus/corpus-key-gcp.json",
        "corpus/corpus-key-kmip.json",
    ] {
        datakeys.insert_one(load_testdata(f)?, None).await?;
    }

    // Setup: encrypted client and manual encryption.
    let schema_map = if local_schema {
        Some([("db.coll".to_string(), schema)].into_iter().collect())
    } else {
        None
    };
    let auto_enc_opts = AutoEncryptionOptions::builder()
        .key_vault_namespace(KV_NAMESPACE.clone())
        .kms_providers(KMS_PROVIDERS.clone())
        .tls_options(KMIP_TLS_OPTIONS.clone())
        .extra_options(EXTRA_OPTIONS.clone())
        .disable_crypt_shared(DISABLE_CRYPT_SHARED.clone())
        .schema_map(schema_map)
        .build();
    let client_encrypted =
        Client::with_encryption_options(CLIENT_OPTIONS.get().await.clone(), auto_enc_opts).await?;
    let enc_opts = ClientEncryptionOptions::builder()
        .key_vault_namespace(KV_NAMESPACE.clone())
        .key_vault_client(client.clone().into_client())
        .kms_providers(KMS_PROVIDERS.clone())
        .tls_options(KMIP_TLS_OPTIONS.clone())
        .build();
    let client_encryption = ClientEncryption::new(enc_opts)?;

    // Test: build corpus.
    let corpus = load_corpus_nodecimal128("corpus/corpus.json")?;
    let mut corpus_copied = doc! {};
    for (name, field) in &corpus {
        // Copy simple fields
        if [
            "_id",
            "altname_aws",
            "altname_local",
            "altname_azure",
            "altname_gcp",
            "altname_kmip",
        ]
        .contains(&name.as_str())
        {
            corpus_copied.insert(name, field);
            continue;
        }
        // Encrypt `value` field in subdocuments.
        let subdoc = match field.as_document() {
            Some(d) => d,
            None => {
                return Err(failure!(
                    "unexpected field type for {:?}: {:?}",
                    name,
                    field.element_type()
                ))
            }
        };
        let method = subdoc.get_str("method")?;
        if method == "auto" {
            corpus_copied.insert(name, subdoc);
            continue;
        }
        if method != "explicit" {
            return Err(failure!("Invalid method {:?}", method));
        }
        let algo = match subdoc.get_str("algo")? {
            "rand" => Algorithm::AeadAes256CbcHmacSha512Random,
            "det" => Algorithm::AeadAes256CbcHmacSha512Deterministic,
            s => return Err(failure!("Invalid algorithm {:?}", s)),
        };
        let kms = KmsProvider::from_name(subdoc.get_str("kms")?);
        let key = match subdoc.get_str("identifier")? {
            "id" => EncryptKey::Id(base64_uuid(match kms {
                KmsProvider::Local => "LOCALAAAAAAAAAAAAAAAAA==",
                KmsProvider::Aws => "AWSAAAAAAAAAAAAAAAAAAA==",
                KmsProvider::Azure => "AZUREAAAAAAAAAAAAAAAAA==",
                KmsProvider::Gcp => "GCPAAAAAAAAAAAAAAAAAAA==",
                KmsProvider::Kmip => "KMIPAAAAAAAAAAAAAAAAAA==",
                _ => return Err(failure!("Invalid kms provider {:?}", kms)),
            })?),
            "altname" => EncryptKey::AltName(kms.name().to_string()),
            s => return Err(failure!("Invalid identifier {:?}", s)),
        };
        let value = subdoc.get("value").expect("no value to encrypt").clone();
        let result = client_encryption
            .encrypt(
                value.try_into()?,
                EncryptOptions::builder().key(key).algorithm(algo).build(),
            )
            .await;
        let mut subdoc_copied = subdoc.clone();
        if subdoc.get_bool("allowed")? {
            subdoc_copied.insert("value", result?);
        } else {
            result.expect_err("expected encryption to be disallowed");
        }
        corpus_copied.insert(name, subdoc_copied);
    }

    // Test: insert into and find from collection, with automatic encryption.
    let coll = client_encrypted
        .database("db")
        .collection::<Document>("coll");
    let id = coll.insert_one(corpus_copied, None).await?.inserted_id;
    let corpus_decrypted = coll
        .find_one(doc! { "_id": id.clone() }, None)
        .await?
        .expect("document lookup failed");
    assert_eq!(corpus, corpus_decrypted);

    // Test: validate encrypted form.
    let corpus_encrypted_expected = load_corpus_nodecimal128("corpus/corpus-encrypted.json")?;
    let corpus_encrypted_actual = client
        .database("db")
        .collection::<Document>("coll")
        .find_one(doc! { "_id": id }, None)
        .await?
        .expect("encrypted document lookup failed");
    for (name, field) in &corpus_encrypted_expected {
        let subdoc = match field.as_document() {
            Some(d) => d,
            None => continue,
        };
        let value = subdoc.get("value").expect("no expected value");
        let actual_value = corpus_encrypted_actual
            .get_document(name)?
            .get("value")
            .expect("no actual value");
        let algo = subdoc.get_str("algo")?;
        if algo == "det" {
            assert_eq!(value, actual_value);
        }
        let allowed = subdoc.get_bool("allowed")?;
        if algo == "rand" && allowed {
            assert_ne!(value, actual_value);
        }
        if allowed {
            let bin = match value {
                bson::Bson::Binary(b) => b,
                _ => {
                    return Err(failure!(
                        "expected value {:?} should be Binary, got {:?}",
                        name,
                        value
                    ))
                }
            };
            let actual_bin = match actual_value {
                bson::Bson::Binary(b) => b,
                _ => {
                    return Err(failure!(
                        "actual value {:?} should be Binary, got {:?}",
                        name,
                        actual_value
                    ))
                }
            };
            let dec = client_encryption.decrypt(bin.as_raw_binary()).await?;
            let actual_dec = client_encryption
                .decrypt(actual_bin.as_raw_binary())
                .await?;
            assert_eq!(dec, actual_dec);
        } else {
            assert_eq!(Some(value), corpus.get_document(name)?.get("value"));
        }
    }

    Ok(())
}

async fn custom_endpoint_setup(valid: bool) -> Result<ClientEncryption> {
    let mut kms_providers = KMS_PROVIDERS.clone();
    kms_providers.get_mut(&KmsProvider::Azure).unwrap().insert(
        "identityPlatformEndpoint",
        if valid {
            "login.microsoftonline.com:443"
        } else {
            "doesnotexist.invalid:443"
        },
    );
    kms_providers.get_mut(&KmsProvider::Gcp).unwrap().insert(
        "endpoint",
        if valid {
            "oauth2.googleapis.com:443"
        } else {
            "doesnotexist.invalid:443"
        },
    );
    kms_providers.get_mut(&KmsProvider::Kmip).unwrap().insert(
        "endpoint",
        if valid {
            "localhost:5698"
        } else {
            "doesnotexist.local:5698"
        },
    );
    let enc_opts = ClientEncryptionOptions::builder()
        .key_vault_namespace(KV_NAMESPACE.clone())
        .key_vault_client(TestClient::new().await.into_client())
        .kms_providers(kms_providers)
        .tls_options(KMIP_TLS_OPTIONS.clone())
        .build();
    Ok(ClientEncryption::new(enc_opts)?)
}

async fn validate_roundtrip(
    client_encryption: &ClientEncryption,
    key_id: bson::Binary,
) -> Result<()> {
    let value = RawBson::String("test".to_string());
    let encrypted = client_encryption
        .encrypt(
            value.clone(),
            EncryptOptions::builder()
                .key(EncryptKey::Id(key_id))
                .algorithm(Algorithm::AeadAes256CbcHmacSha512Deterministic)
                .build(),
        )
        .await?;
    let decrypted = client_encryption.decrypt(encrypted.as_raw_binary()).await?;
    assert_eq!(value, decrypted);
    Ok(())
}

async fn custom_endpoint_aws_ok(endpoint: Option<String>) -> Result<()> {
    let client_encryption = custom_endpoint_setup(true).await?;

    let key_id = client_encryption
        .create_data_key(
            &KmsProvider::Aws,
            DataKeyOptions::builder()
                .master_key(MasterKey::Aws {
                    region: "us-east-1".to_string(),
                    key: "arn:aws:kms:us-east-1:579766882180:key/\
                          89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                        .to_string(),
                    endpoint,
                })
                .build(),
        )
        .await?;
    validate_roundtrip(&client_encryption, key_id).await?;

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 1. aws, no endpoint)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_endpoint_aws_no_endpoint() -> Result<()> {
    if !check_env("custom_endpoint_aws_no_endpoint", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    custom_endpoint_aws_ok(None).await
}

// Prose test 7. Custom Endpoint Test (case 2. aws, endpoint without port)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_endpoint_aws_no_port() -> Result<()> {
    if !check_env("custom_endpoint_aws_no_port", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    custom_endpoint_aws_ok(Some("kms.us-east-1.amazonaws.com".to_string())).await
}

// Prose test 7. Custom Endpoint Test (case 3. aws, endpoint with port)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_endpoint_aws_with_port() -> Result<()> {
    if !check_env("custom_endpoint_aws_with_port", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    custom_endpoint_aws_ok(Some("kms.us-east-1.amazonaws.com:443".to_string())).await
}

// Prose test 7. Custom Endpoint Test (case 4. aws, endpoint with invalid port)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_endpoint_aws_invalid_port() -> Result<()> {
    if !check_env("custom_endpoint_aws_invalid_port", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let client_encryption = custom_endpoint_setup(true).await?;

    let result = client_encryption
        .create_data_key(
            &KmsProvider::Aws,
            DataKeyOptions::builder()
                .master_key(MasterKey::Aws {
                    region: "us-east-1".to_string(),
                    key: "arn:aws:kms:us-east-1:579766882180:key/\
                          89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                        .to_string(),
                    endpoint: Some("kms.us-east-1.amazonaws.com:12345".to_string()),
                })
                .build(),
        )
        .await;
    assert!(result.unwrap_err().is_network_error());

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 5. aws, invalid region)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_endpoint_aws_invalid_region() -> Result<()> {
    if !check_env("custom_endpoint_aws_invalid_region", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let client_encryption = custom_endpoint_setup(true).await?;

    let result = client_encryption
        .create_data_key(
            &KmsProvider::Aws,
            DataKeyOptions::builder()
                .master_key(MasterKey::Aws {
                    region: "us-east-1".to_string(),
                    key: "arn:aws:kms:us-east-1:579766882180:key/\
                          89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                        .to_string(),
                    endpoint: Some("kms.us-east-2.amazonaws.com".to_string()),
                })
                .build(),
        )
        .await;
    assert!(result.unwrap_err().is_csfle_error());

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 6. aws, invalid domain)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_endpoint_aws_invalid_domain() -> Result<()> {
    if !check_env("custom_endpoint_aws_invalid_domain", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let client_encryption = custom_endpoint_setup(true).await?;

    let result = client_encryption
        .create_data_key(
            &KmsProvider::Aws,
            DataKeyOptions::builder()
                .master_key(MasterKey::Aws {
                    region: "us-east-1".to_string(),
                    key: "arn:aws:kms:us-east-1:579766882180:key/\
                          89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                        .to_string(),
                    endpoint: Some("doesnotexist.invalid".to_string()),
                })
                .build(),
        )
        .await;
    assert!(result.unwrap_err().is_network_error());

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 7. azure)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_endpoint_azure() -> Result<()> {
    if !check_env("custom_endpoint_azure", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let key_options = DataKeyOptions::builder()
        .master_key(MasterKey::Azure {
            key_vault_endpoint: "key-vault-csfle.vault.azure.net".to_string(),
            key_name: "key-name-csfle".to_string(),
            key_version: None,
        })
        .build();

    let client_encryption = custom_endpoint_setup(true).await?;
    let key_id = client_encryption
        .create_data_key(&KmsProvider::Azure, key_options.clone())
        .await?;
    validate_roundtrip(&client_encryption, key_id).await?;

    let client_encryption_invalid = custom_endpoint_setup(false).await?;
    let result = client_encryption_invalid
        .create_data_key(&KmsProvider::Azure, key_options)
        .await;
    assert!(result.unwrap_err().is_network_error());

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 8. gcp)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_endpoint_gcp_valid() -> Result<()> {
    if !check_env("custom_endpoint_gcp_valid", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let key_options = DataKeyOptions::builder()
        .master_key(MasterKey::Gcp {
            project_id: "devprod-drivers".to_string(),
            location: "global".to_string(),
            key_ring: "key-ring-csfle".to_string(),
            key_name: "key-name-csfle".to_string(),
            key_version: None,
            endpoint: Some("cloudkms.googleapis.com:443".to_string()),
        })
        .build();

    let client_encryption = custom_endpoint_setup(true).await?;
    let key_id = client_encryption
        .create_data_key(&KmsProvider::Gcp, key_options.clone())
        .await?;
    validate_roundtrip(&client_encryption, key_id).await?;

    let client_encryption_invalid = custom_endpoint_setup(false).await?;
    let result = client_encryption_invalid
        .create_data_key(&KmsProvider::Gcp, key_options)
        .await;
    assert!(result.unwrap_err().is_network_error());

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 9. gcp, invalid endpoint)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_endpoint_gcp_invalid() -> Result<()> {
    if !check_env("custom_endpoint_gcp_invalid", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let key_options = DataKeyOptions::builder()
        .master_key(MasterKey::Gcp {
            project_id: "devprod-drivers".to_string(),
            location: "global".to_string(),
            key_ring: "key-ring-csfle".to_string(),
            key_name: "key-name-csfle".to_string(),
            key_version: None,
            endpoint: Some("doesnotexist.invalid:443".to_string()),
        })
        .build();

    let client_encryption = custom_endpoint_setup(true).await?;
    let result = client_encryption
        .create_data_key(&KmsProvider::Gcp, key_options.clone())
        .await;
    let err = result.unwrap_err();
    assert!(err.is_csfle_error());
    assert!(
        err.to_string().contains("Invalid KMS response"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 10. kmip, no endpoint)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_endpoint_kmip_no_endpoint() -> Result<()> {
    if !check_env("custom_endpoint_kmip_no_endpoint", true) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let key_options = DataKeyOptions::builder()
        .master_key(MasterKey::Kmip {
            key_id: Some("1".to_string()),
            endpoint: None,
        })
        .build();

    let client_encryption = custom_endpoint_setup(true).await?;
    let key_id = client_encryption
        .create_data_key(&KmsProvider::Kmip, key_options.clone())
        .await?;
    validate_roundtrip(&client_encryption, key_id).await?;

    let client_encryption_invalid = custom_endpoint_setup(false).await?;
    let result = client_encryption_invalid
        .create_data_key(&KmsProvider::Kmip, key_options)
        .await;
    assert!(result.unwrap_err().is_network_error());

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 11. kmip, valid endpoint)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_endpoint_kmip_valid_endpoint() -> Result<()> {
    if !check_env("custom_endpoint_kmip_valid_endpoint", true) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let key_options = DataKeyOptions::builder()
        .master_key(MasterKey::Kmip {
            key_id: Some("1".to_string()),
            endpoint: Some("localhost:5698".to_string()),
        })
        .build();

    let client_encryption = custom_endpoint_setup(true).await?;
    let key_id = client_encryption
        .create_data_key(&KmsProvider::Kmip, key_options)
        .await?;
    validate_roundtrip(&client_encryption, key_id).await
}

// Prose test 7. Custom Endpoint Test (case 12. kmip, invalid endpoint)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn custom_endpoint_kmip_invalid_endpoint() -> Result<()> {
    if !check_env("custom_endpoint_kmip_invalid_endpoint", true) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let key_options = DataKeyOptions::builder()
        .master_key(MasterKey::Kmip {
            key_id: Some("1".to_string()),
            endpoint: Some("doesnotexist.local:5698".to_string()),
        })
        .build();

    let client_encryption = custom_endpoint_setup(true).await?;
    let result = client_encryption
        .create_data_key(&KmsProvider::Kmip, key_options)
        .await;
    assert!(result.unwrap_err().is_network_error());

    Ok(())
}

// Prose test 8. Bypass Spawning mongocryptd (Via mongocryptdBypassSpawn)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn bypass_mongocryptd_via_bypass_spawn() -> Result<()> {
    if !check_env("bypass_mongocryptd_via_bypass_spawn", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    // Setup: encrypted client.
    let schema_map: HashMap<String, Document> = [(
        "db.coll".to_string(),
        load_testdata("external-schema.json")?,
    )]
    .into_iter()
    .collect();
    let extra_options = doc! {
        "mongocryptdBypassSpawn": true,
        "mongocryptdURI": "mongodb://localhost:27021/db?serverSelectionTimeoutMS=1000",
        "mongocryptdSpawnArgs": [ "--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"],
    };
    let auto_enc_opts = AutoEncryptionOptions::builder()
        .key_vault_namespace(KV_NAMESPACE.clone())
        .kms_providers(LOCAL_KMS.clone())
        .schema_map(schema_map)
        .extra_options(extra_options)
        .disable_crypt_shared(true)
        .build();
    let client_encrypted =
        Client::with_encryption_options(CLIENT_OPTIONS.get().await.clone(), auto_enc_opts).await?;

    // Test: insert fails.
    let err = client_encrypted
        .database("db")
        .collection::<Document>("coll")
        .insert_one(doc! { "encrypted": "test" }, None)
        .await
        .unwrap_err();
    assert!(err.is_server_selection_error(), "unexpected error: {}", err);

    Ok(())
}

async fn bypass_mongocryptd_unencrypted_insert(
    conf: impl FnOnce(&mut AutoEncryptionOptions),
) -> Result<()> {
    let _guard = LOCK.run_exclusively().await;

    // Setup: encrypted client.
    let extra_options = doc! {
        "mongocryptdSpawnArgs": [ "--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"],
    };
    let mut auto_enc_opts = AutoEncryptionOptions::builder()
        .key_vault_namespace(KV_NAMESPACE.clone())
        .kms_providers(LOCAL_KMS.clone())
        .extra_options(extra_options)
        .disable_crypt_shared(true)
        .build();
    conf(&mut auto_enc_opts);
    let client_encrypted =
        Client::with_encryption_options(CLIENT_OPTIONS.get().await.clone(), auto_enc_opts).await?;

    // Test: insert succeeds.
    client_encrypted
        .database("db")
        .collection::<Document>("coll")
        .insert_one(doc! { "unencrypted": "test" }, None)
        .await?;
    // Test: mongocryptd not spawned.
    assert!(!client_encrypted.mongocryptd_spawned().await);
    // Test: attempting to connect fails.
    let result =
        Client::with_uri_str("mongodb://localhost:27021/?serverSelectionTimeoutMS=1000").await;
    if let Err(err) = result {
        assert!(err.is_server_selection_error());
    } else {
        let client = result.unwrap();
        let result = client.list_database_names(None, None).await;
        assert!(result.unwrap_err().is_server_selection_error());
    }

    Ok(())
}

// Prose test 8. Bypass Spawning mongocryptd (Via bypassAutoEncryption)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn bypass_mongocryptd_via_bypass_auto_encryption() -> Result<()> {
    if !check_env("bypass_mongocryptd_via_bypass_auto_encryption", false) {
        return Ok(());
    }
    bypass_mongocryptd_unencrypted_insert(|opts| {
        opts.bypass_auto_encryption = Some(true);
    })
    .await
}

// Prose test 8. Bypass Spawning mongocryptd (Via bypassQueryAnalysis)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn bypass_mongocryptd_via_bypass_query_analysis() -> Result<()> {
    if !check_env("bypass_mongocryptd_via_bypass_query_analysis", false) {
        return Ok(());
    }
    bypass_mongocryptd_unencrypted_insert(|opts| {
        opts.bypass_query_analysis = Some(true);
    })
    .await
}

// Prose test 9. Deadlock Tests
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn deadlock() -> Result<()> {
    if !check_env("deadlock", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    // Case 1
    DeadlockTestCase {
        max_pool_size: 1,
        bypass_auto_encryption: false,
        set_key_vault_client: false,
        expected_encrypted_commands: vec![
            DeadlockExpectation {
                command: "listCollections",
                db: "db",
            },
            DeadlockExpectation {
                command: "find",
                db: "keyvault",
            },
            DeadlockExpectation {
                command: "insert",
                db: "db",
            },
            DeadlockExpectation {
                command: "find",
                db: "db",
            },
        ],
        expected_keyvault_commands: vec![],
        expected_number_of_clients: 2,
    }
    .run()
    .await?;
    // Case 2
    DeadlockTestCase {
        max_pool_size: 1,
        bypass_auto_encryption: false,
        set_key_vault_client: true,
        expected_encrypted_commands: vec![
            DeadlockExpectation {
                command: "listCollections",
                db: "db",
            },
            DeadlockExpectation {
                command: "insert",
                db: "db",
            },
            DeadlockExpectation {
                command: "find",
                db: "db",
            },
        ],
        expected_keyvault_commands: vec![DeadlockExpectation {
            command: "find",
            db: "keyvault",
        }],
        expected_number_of_clients: 2,
    }
    .run()
    .await?;
    // Case 3
    DeadlockTestCase {
        max_pool_size: 1,
        bypass_auto_encryption: true,
        set_key_vault_client: false,
        expected_encrypted_commands: vec![
            DeadlockExpectation {
                command: "find",
                db: "db",
            },
            DeadlockExpectation {
                command: "find",
                db: "keyvault",
            },
        ],
        expected_keyvault_commands: vec![],
        expected_number_of_clients: 2,
    }
    .run()
    .await?;
    // Case 4
    DeadlockTestCase {
        max_pool_size: 1,
        bypass_auto_encryption: true,
        set_key_vault_client: true,
        expected_encrypted_commands: vec![DeadlockExpectation {
            command: "find",
            db: "db",
        }],
        expected_keyvault_commands: vec![DeadlockExpectation {
            command: "find",
            db: "keyvault",
        }],
        expected_number_of_clients: 1,
    }
    .run()
    .await?;
    // Case 5: skipped (unlimited max_pool_size not supported)
    // Case 6: skipped (unlimited max_pool_size not supported)
    // Case 7: skipped (unlimited max_pool_size not supported)
    // Case 8: skipped (unlimited max_pool_size not supported)

    Ok(())
}

struct DeadlockTestCase {
    max_pool_size: u32,
    bypass_auto_encryption: bool,
    set_key_vault_client: bool,
    expected_encrypted_commands: Vec<DeadlockExpectation>,
    expected_keyvault_commands: Vec<DeadlockExpectation>,
    expected_number_of_clients: usize,
}

impl DeadlockTestCase {
    async fn run(&self) -> Result<()> {
        // Setup
        let client_test = TestClient::new().await;
        let client_keyvault = EventClient::with_options({
            let mut opts = CLIENT_OPTIONS.get().await.clone();
            opts.max_pool_size = Some(1);
            opts
        })
        .await;
        let mut keyvault_events = client_keyvault.subscribe_to_events();
        client_test
            .database("keyvault")
            .collection::<Document>("datakeys")
            .drop(None)
            .await?;
        client_test
            .database("db")
            .collection::<Document>("coll")
            .drop(None)
            .await?;
        client_keyvault
            .database("keyvault")
            .collection::<Document>("datakeys")
            .insert_one(
                load_testdata("external-key.json")?,
                InsertOneOptions::builder()
                    .write_concern(WriteConcern::MAJORITY)
                    .build(),
            )
            .await?;
        client_test
            .database("db")
            .create_collection(
                "coll",
                CreateCollectionOptions::builder()
                    .validator(doc! { "$jsonSchema": load_testdata("external-schema.json")? })
                    .build(),
            )
            .await?;
        let client_encryption = ClientEncryption::new(
            ClientEncryptionOptions::builder()
                .key_vault_client(client_test.clone().into_client())
                .key_vault_namespace(KV_NAMESPACE.clone())
                .kms_providers(LOCAL_KMS.clone())
                .build(),
        )?;
        let ciphertext = client_encryption
            .encrypt(
                RawBson::String("string0".to_string()),
                EncryptOptions::builder()
                    .algorithm(Algorithm::AeadAes256CbcHmacSha512Deterministic)
                    .key(EncryptKey::AltName("local".to_string()))
                    .build(),
            )
            .await?;

        // Run test case
        let auto_enc_opts = AutoEncryptionOptions::builder()
            .key_vault_namespace(KV_NAMESPACE.clone())
            .kms_providers(LOCAL_KMS.clone())
            .bypass_auto_encryption(self.bypass_auto_encryption)
            .key_vault_client(
                if self.set_key_vault_client {
                    Some(client_keyvault.clone().into_client())
                } else {
                    None
                },
            )
            .extra_options(EXTRA_OPTIONS.clone())
            .disable_crypt_shared(DISABLE_CRYPT_SHARED.clone())
            .build();
        let event_handler = Arc::new(EventHandler::new());
        let mut encrypted_events = event_handler.subscribe();
        let mut opts = CLIENT_OPTIONS.get().await.clone();
        opts.max_pool_size = Some(self.max_pool_size);
        opts.command_event_handler = Some(event_handler.clone());
        opts.sdam_event_handler = Some(event_handler.clone());
        let client_encrypted = Client::with_encryption_options(opts, auto_enc_opts).await?;

        if self.bypass_auto_encryption {
            client_test
                .database("db")
                .collection::<Document>("coll")
                .insert_one(doc! { "_id": 0, "encrypted": ciphertext }, None)
                .await?;
        } else {
            client_encrypted
                .database("db")
                .collection::<Document>("coll")
                .insert_one(doc! { "_id": 0, "encrypted": "string0" }, None)
                .await?;
        }

        let found = client_encrypted
            .database("db")
            .collection::<Document>("coll")
            .find_one(doc! { "_id": 0 }, None)
            .await?;
        assert_eq!(found, Some(doc! { "_id": 0, "encrypted": "string0" }));

        let encrypted_events = encrypted_events
            .collect_events(Duration::from_millis(500), |_| true)
            .await;
        let client_count = encrypted_events
            .iter()
            .filter(|ev| matches!(ev, Event::Sdam(SdamEvent::TopologyOpening(_))))
            .count();
        assert_eq!(self.expected_number_of_clients, client_count);

        let encrypted_commands: Vec<_> = encrypted_events
            .into_iter()
            .filter_map(|ev| ev.into_command_started_event())
            .collect();
        for expected in &self.expected_encrypted_commands {
            expected.assert_matches_any("encrypted", &encrypted_commands);
        }

        let keyvault_commands = keyvault_events
            .collect_events_map(Duration::from_millis(500), |ev| {
                ev.into_command_started_event()
            })
            .await;
        for expected in &self.expected_keyvault_commands {
            expected.assert_matches_any("keyvault", &keyvault_commands);
        }

        Ok(())
    }
}

#[derive(Debug)]
struct DeadlockExpectation {
    command: &'static str,
    db: &'static str,
}

impl DeadlockExpectation {
    fn matches(&self, ev: &CommandStartedEvent) -> bool {
        ev.command_name == self.command && ev.db == self.db
    }

    fn assert_matches_any(&self, name: &str, commands: &[CommandStartedEvent]) {
        for actual in commands {
            if self.matches(actual) {
                return;
            }
        }
        panic!(
            "No {} command matching {:?} found, events=\n{:?}",
            name, self, commands
        );
    }
}

// Prose test 10. KMS TLS Tests
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn kms_tls() -> Result<()> {
    if !check_env("kms_tls", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    // Invalid KMS Certificate
    let err = run_kms_tls_test("127.0.0.1:9000").await.unwrap_err();
    assert!(
        err.to_string().contains("certificate verify failed"),
        "unexpected error: {}",
        err
    );

    // Invalid Hostname in KMS Certificate
    let err = run_kms_tls_test("127.0.0.1:9001").await.unwrap_err();
    assert!(
        err.to_string().contains("certificate verify failed"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

async fn run_kms_tls_test(endpoint: impl Into<String>) -> crate::error::Result<()> {
    // Setup
    let kv_client = TestClient::new().await;
    let enc_opts = ClientEncryptionOptions::builder()
        .key_vault_namespace(KV_NAMESPACE.clone())
        .key_vault_client(kv_client.clone().into_client())
        .kms_providers(KMS_PROVIDERS.clone())
        .build();
    let client_encryption = ClientEncryption::new(enc_opts)?;

    // Test
    client_encryption
        .create_data_key(
            &KmsProvider::Aws,
            DataKeyOptions::builder()
                .master_key(MasterKey::Aws {
                    region: "us-east-1".to_string(),
                    key: "arn:aws:kms:us-east-1:579766882180:key/\
                          89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                        .to_string(),
                    endpoint: Some(endpoint.into()),
                })
                .build(),
        )
        .await
        .map(|_| ())
}

// Prose test 11. KMS TLS Options Tests
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn kms_tls_options() -> Result<()> {
    if !check_env("kms_tls_options", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    // Setup
    let mut providers = KMS_PROVIDERS.clone();
    providers
        .get_mut(&KmsProvider::Azure)
        .unwrap()
        .insert("identityPlatformEndpoint", "127.0.0.1:9002");
    providers
        .get_mut(&KmsProvider::Gcp)
        .unwrap()
        .insert("endpoint", "127.0.0.1:9002");
    let cert_dir = PathBuf::from(std::env::var("CSFLE_TLS_CERT_DIR").unwrap());
    let ca_path = cert_dir.join("ca.pem");
    let key_path = cert_dir.join("client.pem");

    fn build_kms_tls_opts(tls_opts: &TlsOptions) -> KmsProvidersTlsOptions {
        [
            (KmsProvider::Aws, tls_opts.clone()),
            (KmsProvider::Azure, tls_opts.clone()),
            (KmsProvider::Gcp, tls_opts.clone()),
            (KmsProvider::Kmip, tls_opts.clone()),
        ]
        .into_iter()
        .collect()
    }

    let client_encryption_no_client_cert = ClientEncryption::new(
        ClientEncryptionOptions::builder()
            .key_vault_namespace(KV_NAMESPACE.clone())
            .key_vault_client(TestClient::new().await.into_client())
            .kms_providers(providers.clone())
            .tls_options(build_kms_tls_opts(
                &TlsOptions::builder().ca_file_path(ca_path.clone()).build(),
            ))
            .build(),
    )?;

    let client_encryption_with_tls = ClientEncryption::new(
        ClientEncryptionOptions::builder()
            .key_vault_namespace(KV_NAMESPACE.clone())
            .key_vault_client(TestClient::new().await.into_client())
            .kms_providers(providers.clone())
            .tls_options(build_kms_tls_opts(
                &TlsOptions::builder()
                    .ca_file_path(ca_path.clone())
                    .cert_key_file_path(key_path.clone())
                    .build(),
            ))
            .build(),
    )?;

    let client_encryption_expired = {
        let mut providers = providers.clone();
        providers
            .get_mut(&KmsProvider::Azure)
            .unwrap()
            .insert("identityPlatformEndpoint", "127.0.0.1:9000");
        providers
            .get_mut(&KmsProvider::Gcp)
            .unwrap()
            .insert("endpoint", "127.0.0.1:9000");
        providers
            .get_mut(&KmsProvider::Kmip)
            .unwrap()
            .insert("endpoint", "127.0.0.1:9000");

        ClientEncryption::new(
            ClientEncryptionOptions::builder()
                .key_vault_namespace(KV_NAMESPACE.clone())
                .key_vault_client(TestClient::new().await.into_client())
                .kms_providers(providers)
                .tls_options(build_kms_tls_opts(
                    &TlsOptions::builder().ca_file_path(ca_path.clone()).build(),
                ))
                .build(),
        )?
    };

    let client_encryption_invalid_hostname = {
        let mut providers = providers.clone();
        providers
            .get_mut(&KmsProvider::Azure)
            .unwrap()
            .insert("identityPlatformEndpoint", "127.0.0.1:9001");
        providers
            .get_mut(&KmsProvider::Gcp)
            .unwrap()
            .insert("endpoint", "127.0.0.1:9001");
        providers
            .get_mut(&KmsProvider::Kmip)
            .unwrap()
            .insert("endpoint", "127.0.0.1:9001");

        ClientEncryption::new(
            ClientEncryptionOptions::builder()
                .key_vault_namespace(KV_NAMESPACE.clone())
                .key_vault_client(TestClient::new().await.into_client())
                .kms_providers(providers)
                .tls_options(build_kms_tls_opts(
                    &TlsOptions::builder().ca_file_path(ca_path.clone()).build(),
                ))
                .build(),
        )?
    };

    // Case 1: AWS
    async fn aws_test(
        client_encryption: &ClientEncryption,
        endpoint: impl Into<String>,
        err_str: &str,
    ) {
        let aws_key_opts = DataKeyOptions::builder()
            .master_key(MasterKey::Aws {
                region: "us-east-1".to_string(),
                key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                    .to_string(),
                endpoint: Some(endpoint.into()),
            })
            .build();
        let err = client_encryption
            .create_data_key(&KmsProvider::Aws, aws_key_opts)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains(err_str),
            "unexpected error: {}",
            err
        );
    }

    aws_test(
        &client_encryption_no_client_cert,
        "127.0.0.1:9002",
        "SSL routines",
    )
    .await;
    aws_test(&client_encryption_with_tls, "127.0.0.1:9002", "parse error").await;
    aws_test(
        &client_encryption_expired,
        "127.0.0.1:9000",
        "certificate verify failed",
    )
    .await;
    aws_test(
        &client_encryption_invalid_hostname,
        "127.0.0.1:9001",
        "certificate verify failed",
    )
    .await;

    // Case 2: Azure
    async fn azure_test(client_encryption: &ClientEncryption, err_str: &str) {
        let key_opts = DataKeyOptions::builder()
            .master_key(MasterKey::Azure {
                key_vault_endpoint: "doesnotexist.local".to_string(),
                key_name: "foo".to_string(),
                key_version: None,
            })
            .build();
        let err = client_encryption
            .create_data_key(&KmsProvider::Azure, key_opts)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains(err_str),
            "unexpected error: {}",
            err
        );
    }

    azure_test(&client_encryption_no_client_cert, "SSL routines").await;
    azure_test(&client_encryption_with_tls, "HTTP status=404").await;
    azure_test(&client_encryption_expired, "certificate verify failed").await;
    azure_test(
        &client_encryption_invalid_hostname,
        "certificate verify failed",
    )
    .await;

    // Case 3: GCP
    async fn gcp_test(client_encryption: &ClientEncryption, err_str: &str) {
        let key_opts = DataKeyOptions::builder()
            .master_key(MasterKey::Gcp {
                project_id: "foo".to_string(),
                location: "bar".to_string(),
                key_ring: "baz".to_string(),
                key_name: "foo".to_string(),
                endpoint: None,
                key_version: None,
            })
            .build();
        let err = client_encryption
            .create_data_key(&KmsProvider::Gcp, key_opts)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains(err_str),
            "unexpected error: {}",
            err
        );
    }

    gcp_test(&client_encryption_no_client_cert, "SSL routines").await;
    gcp_test(&client_encryption_with_tls, "HTTP status=404").await;
    gcp_test(&client_encryption_expired, "certificate verify failed").await;
    gcp_test(
        &client_encryption_invalid_hostname,
        "certificate verify failed",
    )
    .await;

    // Case 4: KMIP
    async fn kmip_test(client_encryption: &ClientEncryption, err_str: &str) {
        let key_opts = DataKeyOptions::builder()
            .master_key(MasterKey::Kmip {
                key_id: None,
                endpoint: None,
            })
            .build();
        let err = client_encryption
            .create_data_key(&KmsProvider::Kmip, key_opts)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains(err_str),
            "unexpected error: {}",
            err
        );
    }

    kmip_test(&client_encryption_no_client_cert, "SSL routines").await;
    // This one succeeds!
    client_encryption_with_tls
        .create_data_key(
            &KmsProvider::Kmip,
            DataKeyOptions::builder()
                .master_key(MasterKey::Kmip {
                    key_id: None,
                    endpoint: None,
                })
                .build(),
        )
        .await?;
    kmip_test(&client_encryption_expired, "certificate verify failed").await;
    kmip_test(
        &client_encryption_invalid_hostname,
        "certificate verify failed",
    )
    .await;

    Ok(())
}

// Prose test 12. Explicit Encryption (Case 1: can insert encrypted indexed and find)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn explicit_encryption_case_1() -> Result<()> {
    if !check_env("explicit_encryption_case_1", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let testdata = match explicit_encryption_setup().await? {
        Some(t) => t,
        None => return Ok(()),
    };
    let enc_coll = testdata
        .encrypted_client
        .database("db")
        .collection::<Document>("explicit_encryption");

    let insert_payload = testdata
        .client_encryption
        .encrypt(
            RawBson::String("encrypted indexed value".to_string()),
            EncryptOptions::builder()
                .key(EncryptKey::Id(testdata.key1_id.clone()))
                .algorithm(Algorithm::Indexed)
                .contention_factor(0)
                .build(),
        )
        .await?;
    enc_coll
        .insert_one(doc! { "encryptedIndexed": insert_payload }, None)
        .await?;

    let find_payload = testdata
        .client_encryption
        .encrypt(
            RawBson::String("encrypted indexed value".to_string()),
            EncryptOptions::builder()
                .key(EncryptKey::Id(testdata.key1_id))
                .algorithm(Algorithm::Indexed)
                .query_type("equality".to_string())
                .contention_factor(0)
                .build(),
        )
        .await?;
    let found: Vec<_> = enc_coll
        .find(doc! { "encryptedIndexed": find_payload }, None)
        .await?
        .try_collect()
        .await?;
    assert_eq!(1, found.len());
    assert_eq!(
        "encrypted indexed value",
        found[0].get_str("encryptedIndexed")?
    );

    Ok(())
}

// Prose test 12. Explicit Encryption (Case 2: can insert encrypted indexed and find with non-zero
// contention)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn explicit_encryption_case_2() -> Result<()> {
    if !check_env("explicit_encryption_case_2", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let testdata = match explicit_encryption_setup().await? {
        Some(t) => t,
        None => return Ok(()),
    };
    let enc_coll = testdata
        .encrypted_client
        .database("db")
        .collection::<Document>("explicit_encryption");

    for _ in 0..10 {
        let insert_payload = testdata
            .client_encryption
            .encrypt(
                RawBson::String("encrypted indexed value".to_string()),
                EncryptOptions::builder()
                    .key(EncryptKey::Id(testdata.key1_id.clone()))
                    .algorithm(Algorithm::Indexed)
                    .contention_factor(10)
                    .build(),
            )
            .await?;
        enc_coll
            .insert_one(doc! { "encryptedIndexed": insert_payload }, None)
            .await?;
    }

    let find_payload = testdata
        .client_encryption
        .encrypt(
            RawBson::String("encrypted indexed value".to_string()),
            EncryptOptions::builder()
                .key(EncryptKey::Id(testdata.key1_id.clone()))
                .algorithm(Algorithm::Indexed)
                .query_type("equality".to_string())
                .contention_factor(0)
                .build(),
        )
        .await?;
    let found: Vec<_> = enc_coll
        .find(doc! { "encryptedIndexed": find_payload }, None)
        .await?
        .try_collect()
        .await?;
    assert!(found.len() < 10);
    for doc in found {
        assert_eq!("encrypted indexed value", doc.get_str("encryptedIndexed")?);
    }

    let find_payload2 = testdata
        .client_encryption
        .encrypt(
            RawBson::String("encrypted indexed value".to_string()),
            EncryptOptions::builder()
                .key(EncryptKey::Id(testdata.key1_id.clone()))
                .algorithm(Algorithm::Indexed)
                .query_type("equality".to_string())
                .contention_factor(10)
                .build(),
        )
        .await?;
    let found: Vec<_> = enc_coll
        .find(doc! { "encryptedIndexed": find_payload2 }, None)
        .await?
        .try_collect()
        .await?;
    assert_eq!(10, found.len());
    for doc in found {
        assert_eq!("encrypted indexed value", doc.get_str("encryptedIndexed")?);
    }

    Ok(())
}

// Prose test 12. Explicit Encryption (Case 3: can insert encrypted unindexed)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn explicit_encryption_case_3() -> Result<()> {
    if !check_env("explicit_encryption_case_3", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let testdata = match explicit_encryption_setup().await? {
        Some(t) => t,
        None => return Ok(()),
    };
    let enc_coll = testdata
        .encrypted_client
        .database("db")
        .collection::<Document>("explicit_encryption");

    let insert_payload = testdata
        .client_encryption
        .encrypt(
            RawBson::String("encrypted indexed value".to_string()),
            EncryptOptions::builder()
                .key(EncryptKey::Id(testdata.key1_id.clone()))
                .algorithm(Algorithm::Unindexed)
                .build(),
        )
        .await?;
    enc_coll
        .insert_one(doc! { "_id": 1, "encryptedIndexed": insert_payload }, None)
        .await?;

    let found: Vec<_> = enc_coll
        .find(doc! { "_id": 1 }, None)
        .await?
        .try_collect()
        .await?;
    assert_eq!(1, found.len());
    assert_eq!(
        "encrypted indexed value",
        found[0].get_str("encryptedIndexed")?
    );

    Ok(())
}

// Prose test 12. Explicit Encryption (Case 4: can roundtrip encrypted indexed)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn explicit_encryption_case_4() -> Result<()> {
    if !check_env("explicit_encryption_case_4", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let testdata = match explicit_encryption_setup().await? {
        Some(t) => t,
        None => return Ok(()),
    };

    let raw_value = RawBson::String("encrypted indexed value".to_string());
    let payload = testdata
        .client_encryption
        .encrypt(
            raw_value.clone(),
            EncryptOptions::builder()
                .key(EncryptKey::Id(testdata.key1_id.clone()))
                .algorithm(Algorithm::Indexed)
                .contention_factor(0)
                .build(),
        )
        .await?;
    let roundtrip = testdata
        .client_encryption
        .decrypt(payload.as_raw_binary())
        .await?;
    assert_eq!(raw_value, roundtrip);

    Ok(())
}

// Prose test 12. Explicit Encryption (Case 5: can roundtrip encrypted unindexed)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn explicit_encryption_case_5() -> Result<()> {
    if !check_env("explicit_encryption_case_5", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let testdata = match explicit_encryption_setup().await? {
        Some(t) => t,
        None => return Ok(()),
    };

    let raw_value = RawBson::String("encrypted indexed value".to_string());
    let payload = testdata
        .client_encryption
        .encrypt(
            raw_value.clone(),
            EncryptOptions::builder()
                .key(EncryptKey::Id(testdata.key1_id.clone()))
                .algorithm(Algorithm::Unindexed)
                .build(),
        )
        .await?;
    let roundtrip = testdata
        .client_encryption
        .decrypt(payload.as_raw_binary())
        .await?;
    assert_eq!(raw_value, roundtrip);

    Ok(())
}

struct ExplicitEncryptionTestData {
    key1_id: Binary,
    client_encryption: ClientEncryption,
    encrypted_client: Client,
}

async fn explicit_encryption_setup() -> Result<Option<ExplicitEncryptionTestData>> {
    let key_vault_client = TestClient::new().await;
    if key_vault_client.server_version_lt(6, 0) {
        log_uncaptured("skipping explicit encryption test: server below 6.0");
        return Ok(None);
    }
    if key_vault_client.is_standalone() {
        log_uncaptured("skipping explicit encryption test: cannot run on standalone");
        return Ok(None);
    }

    let encrypted_fields = load_testdata("encryptedFields.json")?;
    let key1_document = load_testdata("key1-document.json")?;
    let key1_id = match key1_document.get("_id").unwrap() {
        Bson::Binary(b) => b.clone(),
        v => return Err(failure!("expected binary _id, got {:?}", v)),
    };

    let db = key_vault_client.database("db");
    db.collection::<Document>("explicit_encryption")
        .drop(
            DropCollectionOptions::builder()
                .encrypted_fields(encrypted_fields.clone())
                .build(),
        )
        .await?;
    db.create_collection(
        "explicit_encryption",
        CreateCollectionOptions::builder()
            .encrypted_fields(encrypted_fields)
            .build(),
    )
    .await?;
    let keyvault = key_vault_client.database("keyvault");
    keyvault
        .collection::<Document>("datakeys")
        .drop(None)
        .await?;
    keyvault.create_collection("datakeys", None).await?;
    keyvault
        .collection::<Document>("datakeys")
        .insert_one(
            key1_document,
            InsertOneOptions::builder()
                .write_concern(WriteConcern::MAJORITY)
                .build(),
        )
        .await?;

    let client_encryption = ClientEncryption::new(
        ClientEncryptionOptions::builder()
            .key_vault_client(key_vault_client.into_client())
            .key_vault_namespace(KV_NAMESPACE.clone())
            .kms_providers(LOCAL_KMS.clone())
            .build(),
    )?;
    let encrypted_client = Client::with_encryption_options(
        CLIENT_OPTIONS.get().await.clone(),
        AutoEncryptionOptions::builder()
            .key_vault_namespace(KV_NAMESPACE.clone())
            .kms_providers(LOCAL_KMS.clone())
            .bypass_query_analysis(true)
            .extra_options(EXTRA_OPTIONS.clone())
            .disable_crypt_shared(DISABLE_CRYPT_SHARED.clone())
            .build(),
    )
    .await?;

    Ok(Some(ExplicitEncryptionTestData {
        key1_id,
        client_encryption,
        encrypted_client,
    }))
}

// Prose test 13. Unique Index on keyAltNames (Case 1: createDataKey())
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn unique_index_keyaltnames_create_data_key() -> Result<()> {
    if !check_env("unique_index_keyaltnames_create_data_key", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let (client_encryption, _) = unique_index_keyaltnames_setup().await?;

    // Succeeds
    client_encryption
        .create_data_key(
            &KmsProvider::Local,
            DataKeyOptions::builder()
                .master_key(MasterKey::Local)
                .key_alt_names(vec!["abc".to_string()])
                .build(),
        )
        .await?;
    // Fails: duplicate key
    let err = client_encryption
        .create_data_key(
            &KmsProvider::Local,
            DataKeyOptions::builder()
                .master_key(MasterKey::Local)
                .key_alt_names(vec!["abc".to_string()])
                .build(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        Some(11000),
        write_err_code(&err),
        "unexpected error: {}",
        err
    );
    // Fails: duplicate key
    let err = client_encryption
        .create_data_key(
            &KmsProvider::Local,
            DataKeyOptions::builder()
                .master_key(MasterKey::Local)
                .key_alt_names(vec!["def".to_string()])
                .build(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        Some(11000),
        write_err_code(&err),
        "unexpected error: {}",
        err
    );

    Ok(())
}

// Prose test 13. Unique Index on keyAltNames (Case 2: addKeyAltName())
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn unique_index_keyaltnames_add_key_alt_name() -> Result<()> {
    if !check_env("unique_index_keyaltnames_add_key_alt_name", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let (client_encryption, key) = unique_index_keyaltnames_setup().await?;

    // Succeeds
    let new_key = client_encryption
        .create_data_key(&KmsProvider::Local, None)
        .await?;
    client_encryption.add_key_alt_name(&new_key, "abc").await?;
    // Still succeeds, has alt name
    let prev_key = client_encryption
        .add_key_alt_name(&new_key, "abc")
        .await?
        .unwrap();
    assert_eq!("abc", prev_key.get_array("keyAltNames")?.get_str(0)?);
    // Fails: adding alt name used for `key` to `new_key`
    let err = client_encryption
        .add_key_alt_name(&new_key, "def")
        .await
        .unwrap_err();
    assert_eq!(
        Some(11000),
        write_err_code(&err),
        "unexpected error: {}",
        err
    );
    // Succeds: re-adding alt name to `new_key`
    let prev_key = client_encryption
        .add_key_alt_name(&key, "def")
        .await?
        .unwrap();
    assert_eq!("def", prev_key.get_array("keyAltNames")?.get_str(0)?);

    Ok(())
}

// `Error::code` skips write errors per the SDAM spec, but we need those.
fn write_err_code(err: &crate::error::Error) -> Option<i32> {
    if let Some(code) = err.code() {
        return Some(code);
    }
    match *err.kind {
        ErrorKind::Write(WriteFailure::WriteError(WriteError { code, .. })) => Some(code),
        _ => None,
    }
}

async fn unique_index_keyaltnames_setup() -> Result<(ClientEncryption, Binary)> {
    let client = TestClient::new().await;
    let datakeys = client
        .database("keyvault")
        .collection::<Document>("datakeys");
    datakeys.drop(None).await?;
    datakeys
        .create_index(
            IndexModel {
                keys: doc! { "keyAltNames": 1 },
                options: Some(
                    IndexOptions::builder()
                        .name("keyAltNames_1".to_string())
                        .unique(true)
                        .partial_filter_expression(doc! { "keyAltNames": { "$exists": true } })
                        .build(),
                ),
            },
            CreateIndexOptions::builder()
                .write_concern(WriteConcern::MAJORITY)
                .build(),
        )
        .await?;
    let client_encryption = ClientEncryption::new(
        ClientEncryptionOptions::builder()
            .key_vault_client(client.into_client())
            .key_vault_namespace(KV_NAMESPACE.clone())
            .kms_providers(LOCAL_KMS.clone())
            .build(),
    )?;
    let key = client_encryption
        .create_data_key(
            &KmsProvider::Local,
            DataKeyOptions::builder()
                .master_key(MasterKey::Local)
                .key_alt_names(vec!["def".to_string()])
                .build(),
        )
        .await?;
    Ok((client_encryption, key))
}

// Prose test 14. Decryption Events (Case 1: Command Error)
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn decryption_events_command_error() -> Result<()> {
    if !check_env("decryption_events_command_error", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let td = match DecryptionEventsTestdata::setup().await? {
        Some(v) => v,
        None => return Ok(()),
    };

    let fp = FailPoint::fail_command(
        &["aggregate"],
        FailPointMode::Times(1),
        FailCommandOptions::builder().error_code(123).build(),
    );
    let _guard = fp.enable(&td.setup_client, None).await?;
    let err = td
        .decryption_events
        .aggregate(vec![doc! { "$count": "total" }], None)
        .await
        .unwrap_err();
    assert_eq!(Some(123), err.code());
    assert!(td.ev_handler.failed.lock().unwrap().is_some());

    Ok(())
}

// Prose test 14. Decryption Events (Case 2: Network Error)
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn decryption_events_network_error() -> Result<()> {
    if !check_env("decryption_events_network_error", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let td = match DecryptionEventsTestdata::setup().await? {
        Some(v) => v,
        None => return Ok(()),
    };

    let fp = FailPoint::fail_command(
        &["aggregate"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .error_code(123)
            .close_connection(true)
            .build(),
    );
    let _guard = fp.enable(&td.setup_client, None).await?;
    let err = td
        .decryption_events
        .aggregate(vec![doc! { "$count": "total" }], None)
        .await
        .unwrap_err();
    assert!(err.is_network_error(), "unexpected error: {}", err);
    assert!(td.ev_handler.failed.lock().unwrap().is_some());

    Ok(())
}

// Prose test 14. Decryption Events (Case 3: Decrypt Error)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn decryption_events_decrypt_error() -> Result<()> {
    if !check_env("decryption_events_decrypt_error", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let td = match DecryptionEventsTestdata::setup().await? {
        Some(v) => v,
        None => return Ok(()),
    };
    td.decryption_events
        .insert_one(doc! { "encrypted": td.malformed_ciphertext }, None)
        .await?;
    let err = td
        .decryption_events
        .aggregate(vec![], None)
        .await
        .unwrap_err();
    assert!(err.is_csfle_error());
    let guard = td.ev_handler.succeeded.lock().unwrap();
    let ev = guard.as_ref().unwrap();
    assert_eq!(
        ElementType::Binary,
        ev.reply.get_document("cursor")?.get_array("firstBatch")?[0]
            .as_document()
            .unwrap()
            .get("encrypted")
            .unwrap()
            .element_type()
    );

    Ok(())
}

// Prose test 14. Decryption Events (Case 4: Decrypt Success)
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn decryption_events_decrypt_success() -> Result<()> {
    if !check_env("decryption_events_decrypt_success", false) {
        return Ok(());
    }
    let _guard = LOCK.run_exclusively().await;

    let td = match DecryptionEventsTestdata::setup().await? {
        Some(v) => v,
        None => return Ok(()),
    };
    td.decryption_events
        .insert_one(doc! { "encrypted": td.ciphertext }, None)
        .await?;
    td.decryption_events.aggregate(vec![], None).await?;
    let guard = td.ev_handler.succeeded.lock().unwrap();
    let ev = guard.as_ref().unwrap();
    assert_eq!(
        ElementType::Binary,
        ev.reply.get_document("cursor")?.get_array("firstBatch")?[0]
            .as_document()
            .unwrap()
            .get("encrypted")
            .unwrap()
            .element_type()
    );

    Ok(())
}

struct DecryptionEventsTestdata {
    setup_client: TestClient,
    decryption_events: Collection<Document>,
    ev_handler: Arc<DecryptionEventsHandler>,
    ciphertext: Binary,
    malformed_ciphertext: Binary,
}

impl DecryptionEventsTestdata {
    async fn setup() -> Result<Option<Self>> {
        let setup_client = TestClient::new().await;
        if !setup_client.is_standalone() {
            log_uncaptured("skipping decryption events test: requires standalone topology");
            return Ok(None);
        }
        let db = setup_client.database("db");
        db.collection::<Document>("decryption_events")
            .drop(None)
            .await?;
        db.create_collection("decryption_events", None).await?;

        let client_encryption = ClientEncryption::new(
            ClientEncryptionOptions::builder()
                .key_vault_client(setup_client.clone().into_client())
                .key_vault_namespace(KV_NAMESPACE.clone())
                .kms_providers(LOCAL_KMS.clone())
                .build(),
        )?;
        let key_id = client_encryption
            .create_data_key(&KmsProvider::Local, None)
            .await?;
        let ciphertext = client_encryption
            .encrypt(
                RawBson::String("hello".to_string()),
                EncryptOptions::builder()
                    .key(EncryptKey::Id(key_id))
                    .algorithm(Algorithm::AeadAes256CbcHmacSha512Deterministic)
                    .build(),
            )
            .await?;
        let mut malformed_ciphertext = ciphertext.clone();
        let last = malformed_ciphertext.bytes.last_mut().unwrap();
        *last = last.wrapping_add(1);

        let ev_handler = DecryptionEventsHandler::new();
        let mut opts = CLIENT_OPTIONS.get().await.clone();
        opts.retry_reads = Some(false);
        opts.command_event_handler = Some(ev_handler.clone());
        let encrypted_client = Client::with_encryption_options(
            opts,
            AutoEncryptionOptions::builder()
                .key_vault_namespace(KV_NAMESPACE.clone())
                .kms_providers(LOCAL_KMS.clone())
                .extra_options(EXTRA_OPTIONS.clone())
                .disable_crypt_shared(DISABLE_CRYPT_SHARED.clone())
                .build(),
        )
        .await?;
        let decryption_events = encrypted_client
            .database("db")
            .collection("decryption_events");

        Ok(Some(Self {
            setup_client,
            decryption_events,
            ev_handler,
            ciphertext,
            malformed_ciphertext,
        }))
    }
}

#[derive(Debug)]
struct DecryptionEventsHandler {
    succeeded: Mutex<Option<CommandSucceededEvent>>,
    failed: Mutex<Option<CommandFailedEvent>>,
}

impl DecryptionEventsHandler {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            succeeded: Mutex::new(None),
            failed: Mutex::new(None),
        })
    }
}

impl CommandEventHandler for DecryptionEventsHandler {
    fn handle_command_succeeded_event(&self, event: CommandSucceededEvent) {
        if event.command_name == "aggregate" {
            *self.succeeded.lock().unwrap() = Some(event);
        }
    }

    fn handle_command_failed_event(&self, event: CommandFailedEvent) {
        if event.command_name == "aggregate" {
            *self.failed.lock().unwrap() = Some(event);
        }
    }
}

// TODO RUST-1314: implement prose test 15. On-demand AWS Credentials

// TODO RUST-1441: implement prose test 16. Rewrap

// TODO RUST-1417: implement prose test 17. On-demand GCP Credentials

// TODO RUST-1442: implement prose test 18. Azure IMDS Credentials
