#[path = "csfle/mock_server.rs"]
#[cfg(feature = "openssl-tls")]
mod mock_server_skip_local; // requires mock servers (see https://github.com/mongodb-labs/drivers-evergreen-tools/tree/master/.evergreen/csfle for details)

use std::{
    collections::BTreeMap,
    env,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
        Mutex,
    },
    time::Duration,
};

use anyhow::Context;
use bson::{
    doc,
    rawdoc,
    spec::{BinarySubtype, ElementType},
    Binary,
    Bson,
    DateTime,
    Document,
    RawBson,
    RawDocumentBuf,
};
use futures_util::TryStreamExt;
use mongocrypt::ctx::{Algorithm, KmsProvider, KmsProviderType};
use once_cell::sync::Lazy;
use tokio::net::TcpListener;

use crate::{
    client_encryption::{
        AwsMasterKey,
        AzureMasterKey,
        ClientEncryption,
        EncryptKey,
        GcpMasterKey,
        LocalMasterKey,
        MasterKey,
        RangeOptions,
    },
    error::{ErrorKind, WriteError, WriteFailure},
    event::{
        command::{CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent},
        sdam::SdamEvent,
    },
    options::{
        CollectionOptions,
        EncryptOptions,
        FindOptions,
        IndexOptions,
        ReadConcern,
        TlsOptions,
        WriteConcern,
    },
    runtime,
    test::{
        util::{
            event_buffer::EventBuffer,
            fail_point::{FailPoint, FailPointMode},
        },
        Event,
    },
    Client,
    Collection,
    IndexModel,
    Namespace,
};

use super::{get_client_options, log_uncaptured, EventClient, TestClient};

type Result<T> = anyhow::Result<T>;
pub(crate) type KmsInfo = (KmsProvider, Document, Option<TlsOptions>);
pub(crate) type KmsProviderList = Vec<KmsInfo>;

// The environment variables needed to run the CSFLE tests. These values can be retrieved from the
// AWS secrets manager by running the setup-secrets.sh script in drivers-evergreen-tools.
static CSFLE_LOCAL_KEY: Lazy<String> = Lazy::new(|| get_env_var("CSFLE_LOCAL_KEY"));
static FLE_AWS_KEY: Lazy<String> = Lazy::new(|| get_env_var("FLE_AWS_KEY"));
static FLE_AWS_SECRET: Lazy<String> = Lazy::new(|| get_env_var("FLE_AWS_SECRET"));
static FLE_AZURE_TENANTID: Lazy<String> = Lazy::new(|| get_env_var("FLE_AZURE_TENANTID"));
static FLE_AZURE_CLIENTID: Lazy<String> = Lazy::new(|| get_env_var("FLE_AZURE_CLIENTID"));
static FLE_AZURE_CLIENTSECRET: Lazy<String> = Lazy::new(|| get_env_var("FLE_AZURE_CLIENTSECRET"));
static FLE_GCP_EMAIL: Lazy<String> = Lazy::new(|| get_env_var("FLE_GCP_EMAIL"));
static FLE_GCP_PRIVATEKEY: Lazy<String> = Lazy::new(|| get_env_var("FLE_GCP_PRIVATEKEY"));

// Additional environment variables. These values should be set to the relevant local paths.
static CSFLE_TLS_CERT_DIR: Lazy<String> = Lazy::new(|| get_env_var("CSFLE_TLS_CERT_DIR"));
static CRYPT_SHARED_LIB_PATH: Lazy<String> = Lazy::new(|| get_env_var("CRYPT_SHARED_LIB_PATH"));

fn get_env_var(name: &str) -> String {
    std::env::var(name).expect(&format!(
        "Missing environment variable for {}. See src/test/csfle.rs for the list of required \
         variables and instructions for retrieving them.",
        name
    ))
}

fn add_name_to_info(kms_info: KmsInfo, name: &str) -> KmsInfo {
    (kms_info.0.with_name(name), kms_info.1, kms_info.2)
}

pub(crate) static AWS_KMS: Lazy<KmsInfo> = Lazy::new(|| {
    (
        KmsProvider::aws(),
        doc! {
            "accessKeyId": FLE_AWS_KEY.clone(),
            "secretAccessKey": FLE_AWS_SECRET.clone(),
        },
        None,
    )
});
pub(crate) static AWS_KMS_NAME1: Lazy<KmsInfo> = Lazy::new(|| {
    let aws_info = AWS_KMS.clone();
    (aws_info.0.with_name("name1"), aws_info.1, aws_info.2)
});
pub(crate) static AWS_KMS_NAME2: Lazy<KmsInfo> = Lazy::new(|| {
    (
        KmsProvider::aws().with_name("name2"),
        doc! {
            "accessKeyId": FLE_AWS_KEY.clone(),
            "secretAccessKey": FLE_AWS_SECRET.clone(),
        },
        None,
    )
});
pub(crate) static AZURE_KMS: Lazy<KmsInfo> = Lazy::new(|| {
    (
        KmsProvider::azure(),
        doc! {
            "tenantId": FLE_AZURE_TENANTID.clone(),
            "clientId": FLE_AZURE_CLIENTID.clone(),
            "clientSecret": FLE_AZURE_CLIENTSECRET.clone(),
        },
        None,
    )
});
pub(crate) static AZURE_KMS_NAME1: Lazy<KmsInfo> = Lazy::new(|| {
    let azure_info = AZURE_KMS.clone();
    (azure_info.0.with_name("name1"), azure_info.1, azure_info.2)
});
pub(crate) static GCP_KMS: Lazy<KmsInfo> = Lazy::new(|| {
    (
        KmsProvider::gcp(),
        doc! {
            "email": FLE_GCP_EMAIL.clone(),
            "privateKey": FLE_GCP_PRIVATEKEY.clone(),
        },
        None,
    )
});
pub(crate) static GCP_KMS_NAME1: Lazy<KmsInfo> = Lazy::new(|| {
    let gcp_info = GCP_KMS.clone();
    (gcp_info.0.with_name("name1"), gcp_info.1, gcp_info.2)
});
pub(crate) static LOCAL_KMS: Lazy<KmsInfo> = Lazy::new(|| {
    (
        KmsProvider::local(),
        doc! {
            "key": bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: base64::decode(&*CSFLE_LOCAL_KEY).unwrap(),
            },
        },
        None,
    )
});
pub(crate) static LOCAL_KMS_NAME1: Lazy<KmsInfo> = Lazy::new(|| {
    let local_info = LOCAL_KMS.clone();
    (local_info.0.with_name("name1"), local_info.1, local_info.2)
});
pub(crate) static KMIP_KMS: Lazy<KmsInfo> = Lazy::new(|| {
    let cert_dir = PathBuf::from(&*CSFLE_TLS_CERT_DIR);
    let tls_options = TlsOptions::builder()
        .ca_file_path(cert_dir.join("ca.pem"))
        .cert_key_file_path(cert_dir.join("client.pem"))
        .build();
    (
        KmsProvider::kmip(),
        doc! {
            "endpoint": "localhost:5698",
        },
        Some(tls_options),
    )
});
pub(crate) static KMIP_KMS_NAME1: Lazy<KmsInfo> = Lazy::new(|| {
    let kmip_info = KMIP_KMS.clone();
    (kmip_info.0.with_name("name1"), kmip_info.1, kmip_info.2)
});

pub(crate) static UNNAMED_KMS_PROVIDERS: Lazy<KmsProviderList> = Lazy::new(|| {
    vec![
        AWS_KMS.clone(),
        AZURE_KMS.clone(),
        GCP_KMS.clone(),
        LOCAL_KMS.clone(),
        KMIP_KMS.clone(),
    ]
});
pub(crate) static NAME1_KMS_PROVIDERS: Lazy<KmsProviderList> = Lazy::new(|| {
    vec![
        AWS_KMS_NAME1.clone(),
        AZURE_KMS_NAME1.clone(),
        GCP_KMS_NAME1.clone(),
        LOCAL_KMS_NAME1.clone(),
        KMIP_KMS_NAME1.clone(),
    ]
});
pub(crate) static ALL_KMS_PROVIDERS: Lazy<KmsProviderList> = Lazy::new(|| {
    let mut providers = UNNAMED_KMS_PROVIDERS.clone();
    providers.extend(NAME1_KMS_PROVIDERS.clone());
    providers.push(AWS_KMS_NAME2.clone());
    providers
});

static EXTRA_OPTIONS: Lazy<Document> =
    Lazy::new(|| doc! { "cryptSharedLibPath": CRYPT_SHARED_LIB_PATH.clone() });
static KV_NAMESPACE: Lazy<Namespace> =
    Lazy::new(|| Namespace::from_str("keyvault.datakeys").unwrap());
static DISABLE_CRYPT_SHARED: Lazy<bool> =
    Lazy::new(|| env::var("DISABLE_CRYPT_SHARED").map_or(false, |s| s == "true"));

async fn init_client() -> Result<(EventClient, Collection<Document>)> {
    let client = Client::for_test().monitor_events().await;
    let datakeys = client
        .database("keyvault")
        .collection_with_options::<Document>(
            "datakeys",
            CollectionOptions::builder()
                .read_concern(ReadConcern::majority())
                .write_concern(WriteConcern::majority())
                .build(),
        );
    datakeys.drop().await?;
    client
        .database("db")
        .collection::<Document>("coll")
        .drop()
        .await?;
    Ok((client, datakeys))
}

// Prose test 1. Custom Key Material Test
#[tokio::test]
async fn custom_key_material() -> Result<()> {
    let (client, datakeys) = init_client().await?;
    let enc = ClientEncryption::new(
        client.into_client(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?;

    let key = base64::decode(
        "xPTAjBRG5JiPm+d3fj6XLi2q5DMXUS/f1f+SMAlhhwkhDRL0kr8r9GDLIGTAGlvC+HVjSIgdL+RKw\
         ZCvpXSyxTICWSXTUYsWYPyu3IoHbuBZdmw2faM3WhcRIgbMReU5",
    )
    .unwrap();
    let id = enc
        .create_data_key(LocalMasterKey::builder().build())
        .key_material(key)
        .await?;
    let mut key_doc = datakeys
        .find_one(doc! { "_id": id.clone() })
        .await?
        .unwrap();
    datakeys.delete_one(doc! { "_id": id}).await?;
    let new_key_id = bson::Binary::from_uuid(bson::Uuid::from_bytes([0; 16]));
    key_doc.insert("_id", new_key_id.clone());
    datakeys.insert_one(key_doc).await?;

    let encrypted = enc
        .encrypt("test", EncryptKey::Id(new_key_id), Algorithm::Deterministic)
        .await?;
    let expected = base64::decode(
        "AQAAAAAAAAAAAAAAAAAAAAACz0ZOLuuhEYi807ZXTdhbqhLaS2/t9wLifJnnNYwiw79d75QYIZ6M/\
         aYC1h9nCzCjZ7pGUpAuNnkUhnIXM3PjrA==",
    )
    .unwrap();
    assert_eq!(encrypted.bytes, expected);

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

fn load_testdata_raw(name: &str) -> Result<String> {
    let path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "src/test/spec/json/testdata/client-side-encryption",
        name,
    ]
    .iter()
    .collect();
    std::fs::read_to_string(path.clone()).context(path.to_string_lossy().into_owned())
}

fn load_testdata(name: &str) -> Result<Document> {
    Ok(serde_json::from_str(&load_testdata_raw(name)?)?)
}

// Prose test 4. BSON Size Limits and Batch Splitting
#[tokio::test]
async fn bson_size_limits() -> Result<()> {
    // Setup: db initialization.
    let (client, datakeys) = init_client().await?;
    client
        .database("db")
        .create_collection("coll")
        .validator(doc! { "$jsonSchema": load_testdata("limits/limits-schema.json")? })
        .await?;
    datakeys
        .insert_one(load_testdata("limits/limits-key.json")?)
        .await?;

    // Setup: encrypted client.
    let mut opts = get_client_options().await.clone();
    let buffer = EventBuffer::<Event>::new();

    opts.command_event_handler = Some(buffer.handler());
    let client_encrypted =
        Client::encrypted_builder(opts, KV_NAMESPACE.clone(), vec![LOCAL_KMS.clone()])?
            .extra_options(EXTRA_OPTIONS.clone())
            .disable_crypt_shared(*DISABLE_CRYPT_SHARED)
            .build()
            .await?;
    let coll = client_encrypted
        .database("db")
        .collection::<Document>("coll");

    // Tests
    // Test operation 1
    coll.insert_one(doc! {
        "_id": "over_2mib_under_16mib",
        "unencrypted": "a".repeat(2097152),
    })
    .await?;

    // Test operation 2
    let mut doc: Document = load_testdata("limits/limits-doc.json")?;
    doc.insert("_id", "encryption_exceeds_2mib");
    doc.insert("unencrypted", "a".repeat(2_097_152 - 2_000));
    coll.insert_one(doc).await?;

    // Test operation 3
    let value = "a".repeat(2_097_152);
    let mut events = buffer.stream();
    coll.insert_many(vec![
        doc! {
            "_id": "over_2mib_1",
            "unencrypted": value.clone(),
        },
        doc! {
            "_id": "over_2mib_2",
            "unencrypted": value,
        },
    ])
    .await?;
    let inserts = events
        .collect(Duration::from_millis(500), |ev| {
            let ev = match ev.as_command_started_event() {
                Some(e) => e,
                None => return false,
            };
            ev.command_name == "insert"
        })
        .await;
    assert_eq!(2, inserts.len());

    // Test operation 4
    let mut doc = load_testdata("limits/limits-doc.json")?;
    doc.insert("_id", "encryption_exceeds_2mib_1");
    doc.insert("unencrypted", "a".repeat(2_097_152 - 2_000));
    let mut doc2 = doc.clone();
    doc2.insert("_id", "encryption_exceeds_2mib_2");
    let mut events = buffer.stream();
    coll.insert_many(vec![doc, doc2]).await?;
    let inserts = events
        .collect(Duration::from_millis(500), |ev| {
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
    coll.insert_one(doc).await?;

    // Test operation 6
    let mut doc: Document = load_testdata("limits/limits-doc.json")?;
    doc.insert("_id", "encryption_exceeds_16mib");
    doc.insert("unencrypted", "a".repeat(16_777_216 - 2_000));
    let result = coll.insert_one(doc).await;
    let err = result.unwrap_err();
    assert!(
        matches!(*err.kind, ErrorKind::Write(_)),
        "unexpected error: {}",
        err
    );

    Ok(())
}

// Prose test 5. Views Are Prohibited
#[tokio::test]
async fn views_prohibited() -> Result<()> {
    // Setup: db initialization.
    let (client, _) = init_client().await?;
    client
        .database("db")
        .collection::<Document>("view")
        .drop()
        .await?;
    client
        .database("db")
        .create_collection("view")
        .view_on("coll".to_string())
        .await?;

    // Setup: encrypted client.
    let client_encrypted = Client::encrypted_builder(
        get_client_options().await.clone(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?
    .extra_options(EXTRA_OPTIONS.clone())
    .disable_crypt_shared(*DISABLE_CRYPT_SHARED)
    .build()
    .await?;

    // Test: auto encryption fails on a view
    let result = client_encrypted
        .database("db")
        .collection::<Document>("view")
        .insert_one(doc! {})
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
use failure;

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

async fn custom_endpoint_setup(valid: bool) -> Result<ClientEncryption> {
    let update_provider =
        |(provider, mut conf, tls): (KmsProvider, Document, Option<TlsOptions>)| {
            match provider.provider_type() {
                KmsProviderType::Azure => {
                    conf.insert(
                        "identityPlatformEndpoint",
                        if valid {
                            "login.microsoftonline.com:443"
                        } else {
                            "doesnotexist.invalid:443"
                        },
                    );
                }
                KmsProviderType::Gcp => {
                    conf.insert(
                        "endpoint",
                        if valid {
                            "oauth2.googleapis.com:443"
                        } else {
                            "doesnotexist.invalid:443"
                        },
                    );
                }
                KmsProviderType::Kmip => {
                    conf.insert(
                        "endpoint",
                        if valid {
                            "localhost:5698"
                        } else {
                            "doesnotexist.local:5698"
                        },
                    );
                }
                _ => (),
            }
            (provider, conf, tls)
        };
    let kms_providers: KmsProviderList = UNNAMED_KMS_PROVIDERS
        .clone()
        .into_iter()
        .map(update_provider)
        .collect();
    Ok(ClientEncryption::new(
        Client::for_test().await.into_client(),
        KV_NAMESPACE.clone(),
        kms_providers,
    )?)
}

async fn validate_roundtrip(
    client_encryption: &ClientEncryption,
    key_id: bson::Binary,
) -> Result<()> {
    let value = RawBson::String("test".to_string());
    let encrypted = client_encryption
        .encrypt(
            value.clone(),
            EncryptKey::Id(key_id),
            Algorithm::Deterministic,
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
            AwsMasterKey::builder()
                .region("us-east-1")
                .key("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0")
                .endpoint(endpoint)
                .build(),
        )
        .await?;
    validate_roundtrip(&client_encryption, key_id).await?;

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 1. aws, no endpoint)
#[tokio::test]
async fn custom_endpoint_aws_no_endpoint() -> Result<()> {
    custom_endpoint_aws_ok(None).await
}

// Prose test 7. Custom Endpoint Test (case 2. aws, endpoint without port)
#[tokio::test]
async fn custom_endpoint_aws_no_port() -> Result<()> {
    custom_endpoint_aws_ok(Some("kms.us-east-1.amazonaws.com".to_string())).await
}

// Prose test 7. Custom Endpoint Test (case 3. aws, endpoint with port)
#[tokio::test]
async fn custom_endpoint_aws_with_port() -> Result<()> {
    custom_endpoint_aws_ok(Some("kms.us-east-1.amazonaws.com:443".to_string())).await
}

// Prose test 7. Custom Endpoint Test (case 4. aws, endpoint with invalid port)
#[tokio::test]
async fn custom_endpoint_aws_invalid_port() -> Result<()> {
    let client_encryption = custom_endpoint_setup(true).await?;

    let result = client_encryption
        .create_data_key(
            AwsMasterKey::builder()
                .region("us-east-1")
                .key("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0")
                .endpoint(Some("kms.us-east-1.amazonaws.com:12345".to_string()))
                .build(),
        )
        .await;
    assert!(result.unwrap_err().is_network_error());

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 5. aws, invalid region)
#[tokio::test]
async fn custom_endpoint_aws_invalid_region() -> Result<()> {
    let client_encryption = custom_endpoint_setup(true).await?;

    let result = client_encryption
        .create_data_key(
            AwsMasterKey::builder()
                .region("us-east-1")
                .key("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0")
                .endpoint(Some("kms.us-east-2.amazonaws.com".to_string()))
                .build(),
        )
        .await;
    assert!(result.unwrap_err().is_csfle_error());

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 6. aws, invalid domain)
#[tokio::test]
async fn custom_endpoint_aws_invalid_domain() -> Result<()> {
    let client_encryption = custom_endpoint_setup(true).await?;

    let result = client_encryption
        .create_data_key(
            AwsMasterKey::builder()
                .region("us-east-1")
                .key("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0")
                .endpoint(Some("doesnotexist.invalid".to_string()))
                .build(),
        )
        .await;
    assert!(result.unwrap_err().is_network_error());

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 7. azure)
#[tokio::test]
async fn custom_endpoint_azure() -> Result<()> {
    let master_key = AzureMasterKey::builder()
        .key_vault_endpoint("key-vault-csfle.vault.azure.net")
        .key_name("key-name-csfle")
        .build();

    let client_encryption = custom_endpoint_setup(true).await?;
    let key_id = client_encryption
        .create_data_key(master_key.clone())
        .await?;
    validate_roundtrip(&client_encryption, key_id).await?;

    let client_encryption_invalid = custom_endpoint_setup(false).await?;
    let result = client_encryption_invalid.create_data_key(master_key).await;
    assert!(result.unwrap_err().is_network_error());

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 8. gcp)
#[tokio::test]
async fn custom_endpoint_gcp_valid() -> Result<()> {
    let master_key = GcpMasterKey::builder()
        .project_id("devprod-drivers")
        .location("global")
        .key_ring("key-ring-csfle")
        .key_name("key-name-csfle")
        .endpoint(Some("cloudkms.googleapis.com:443".to_string()))
        .build();

    let client_encryption = custom_endpoint_setup(true).await?;
    let key_id = client_encryption
        .create_data_key(master_key.clone())
        .await?;
    validate_roundtrip(&client_encryption, key_id).await?;

    let client_encryption_invalid = custom_endpoint_setup(false).await?;
    let result = client_encryption_invalid.create_data_key(master_key).await;
    assert!(result.unwrap_err().is_network_error());

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 9. gcp, invalid endpoint)
#[tokio::test]
async fn custom_endpoint_gcp_invalid() -> Result<()> {
    let master_key = GcpMasterKey::builder()
        .project_id("devprod-drivers")
        .location("global")
        .key_ring("key-ring-csfle")
        .key_name("key-name-csfle")
        .endpoint(Some("doesnotexist.invalid:443".to_string()))
        .build();

    let client_encryption = custom_endpoint_setup(true).await?;
    let result = client_encryption.create_data_key(master_key).await;
    let err = result.unwrap_err();
    assert!(err.is_csfle_error());
    assert!(
        err.to_string().contains("Invalid KMS response"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

// Prose test 8. Bypass Spawning mongocryptd (Via loading shared library)
#[tokio::test]
async fn bypass_mongocryptd_via_shared_library() -> Result<()> {
    if *DISABLE_CRYPT_SHARED {
        log_uncaptured(
            "Skipping bypass mongocryptd via shared library test: crypt_shared is disabled.",
        );
        return Ok(());
    }

    // Setup: encrypted client.
    let client_encrypted = Client::encrypted_builder(
        get_client_options().await.clone(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?
    .schema_map([("db.coll", load_testdata("external/external-schema.json")?)])
    .extra_options(doc! {
        "mongocryptdURI": "mongodb://localhost:27021/db?serverSelectionTimeoutMS=1000",
        "mongocryptdSpawnArgs": ["--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"],
        "cryptSharedLibPath": EXTRA_OPTIONS.get("cryptSharedLibPath").unwrap(),
        "cryptSharedRequired": true,
    })
    .build()
    .await?;

    // Test: insert succeeds.
    client_encrypted
        .database("db")
        .collection::<Document>("coll")
        .insert_one(doc! { "unencrypted": "test" })
        .await?;
    // Test: mongocryptd not spawned.
    assert!(!client_encrypted.mongocryptd_spawned().await);
    // Test: attempting to connect fails.
    let client =
        Client::with_uri_str("mongodb://localhost:27021/?serverSelectionTimeoutMS=1000").await?;
    let result = client.list_database_names().await;
    assert!(result.unwrap_err().is_server_selection_error());

    Ok(())
}

// Prose test 8. Bypass Spawning mongocryptd (Via mongocryptdBypassSpawn)
#[tokio::test]
async fn bypass_mongocryptd_via_bypass_spawn() -> Result<()> {
    // Setup: encrypted client.
    let extra_options = doc! {
        "mongocryptdBypassSpawn": true,
        "mongocryptdURI": "mongodb://localhost:27021/db?serverSelectionTimeoutMS=1000",
        "mongocryptdSpawnArgs": [ "--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"],
    };
    let client_encrypted = Client::encrypted_builder(
        get_client_options().await.clone(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?
    .schema_map([("db.coll", load_testdata("external/external-schema.json")?)])
    .extra_options(extra_options)
    .disable_crypt_shared(true)
    .build()
    .await?;

    // Test: insert fails.
    let err = client_encrypted
        .database("db")
        .collection::<Document>("coll")
        .insert_one(doc! { "encrypted": "test" })
        .await
        .unwrap_err();
    assert!(err.is_server_selection_error(), "unexpected error: {}", err);

    Ok(())
}

enum Bypass {
    AutoEncryption,
    QueryAnalysis,
}

async fn bypass_mongocryptd_unencrypted_insert(bypass: Bypass) -> Result<()> {
    // Setup: encrypted client.
    let extra_options = doc! {
        "mongocryptdSpawnArgs": [ "--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"],
    };
    let builder = Client::encrypted_builder(
        get_client_options().await.clone(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?
    .extra_options(extra_options)
    .disable_crypt_shared(true);
    let builder = match bypass {
        Bypass::AutoEncryption => builder.bypass_auto_encryption(true),
        Bypass::QueryAnalysis => builder.bypass_query_analysis(true),
    };
    let client_encrypted = builder.build().await?;

    // Test: insert succeeds.
    client_encrypted
        .database("db")
        .collection::<Document>("coll")
        .insert_one(doc! { "unencrypted": "test" })
        .await?;
    // Test: mongocryptd not spawned.
    assert!(!client_encrypted.mongocryptd_spawned().await);
    // Test: attempting to connect fails.
    let client =
        Client::with_uri_str("mongodb://localhost:27021/?serverSelectionTimeoutMS=1000").await?;
    let result = client.list_database_names().await;
    assert!(result.unwrap_err().is_server_selection_error());

    Ok(())
}

// Prose test 8. Bypass Spawning mongocryptd (Via bypassAutoEncryption)
#[tokio::test]
async fn bypass_mongocryptd_via_bypass_auto_encryption() -> Result<()> {
    bypass_mongocryptd_unencrypted_insert(Bypass::AutoEncryption).await
}

// Prose test 8. Bypass Spawning mongocryptd (Via bypassQueryAnalysis)
#[tokio::test]
async fn bypass_mongocryptd_via_bypass_query_analysis() -> Result<()> {
    bypass_mongocryptd_unencrypted_insert(Bypass::QueryAnalysis).await
}

// Prose test 9. Deadlock Tests
#[tokio::test]
async fn deadlock() -> Result<()> {
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
        let client_test = Client::for_test().await;
        let client_keyvault = Client::for_test()
            .options({
                let mut opts = get_client_options().await.clone();
                opts.max_pool_size = Some(1);
                opts
            })
            .monitor_events()
            .await;

        let mut keyvault_events = client_keyvault.events.stream();
        client_test
            .database("keyvault")
            .collection::<Document>("datakeys")
            .drop()
            .await?;
        client_test
            .database("db")
            .collection::<Document>("coll")
            .drop()
            .await?;
        client_keyvault
            .database("keyvault")
            .collection::<Document>("datakeys")
            .insert_one(load_testdata("external/external-key.json")?)
            .write_concern(WriteConcern::majority())
            .await?;
        client_test
            .database("db")
            .create_collection("coll")
            .validator(doc! { "$jsonSchema": load_testdata("external/external-schema.json")? })
            .await?;
        let client_encryption = ClientEncryption::new(
            client_test.clone().into_client(),
            KV_NAMESPACE.clone(),
            vec![LOCAL_KMS.clone()],
        )?;
        let ciphertext = client_encryption
            .encrypt(
                RawBson::String("string0".to_string()),
                EncryptKey::AltName("local".to_string()),
                Algorithm::Deterministic,
            )
            .await?;

        // Run test case
        let event_buffer = EventBuffer::new();

        let mut encrypted_events = event_buffer.stream();
        let mut opts = get_client_options().await.clone();
        opts.max_pool_size = Some(self.max_pool_size);
        opts.command_event_handler = Some(event_buffer.handler());
        opts.sdam_event_handler = Some(event_buffer.handler());
        let client_encrypted =
            Client::encrypted_builder(opts, KV_NAMESPACE.clone(), vec![LOCAL_KMS.clone()])?
                .bypass_auto_encryption(self.bypass_auto_encryption)
                .key_vault_client(
                    if self.set_key_vault_client {
                        Some(client_keyvault.clone().into_client())
                    } else {
                        None
                    },
                )
                .extra_options(EXTRA_OPTIONS.clone())
                .disable_crypt_shared(*DISABLE_CRYPT_SHARED)
                .build()
                .await?;

        if self.bypass_auto_encryption {
            client_test
                .database("db")
                .collection::<Document>("coll")
                .insert_one(doc! { "_id": 0, "encrypted": ciphertext })
                .await?;
        } else {
            client_encrypted
                .database("db")
                .collection::<Document>("coll")
                .insert_one(doc! { "_id": 0, "encrypted": "string0" })
                .await?;
        }

        let found = client_encrypted
            .database("db")
            .collection::<Document>("coll")
            .find_one(doc! { "_id": 0 })
            .await?;
        assert_eq!(found, Some(doc! { "_id": 0, "encrypted": "string0" }));

        let encrypted_events = encrypted_events
            .collect(Duration::from_millis(500), |_| true)
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
            .collect_map(Duration::from_millis(500), |ev| {
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

async fn fle2v2_ok(name: &str) -> bool {
    let setup_client = Client::for_test().await;
    if setup_client.server_version_lt(7, 0) {
        log_uncaptured(format!("Skipping {}: not supported on server < 7.0", name));
        return false;
    }
    if setup_client.is_standalone() {
        log_uncaptured(format!("Skipping {}: not supported on standalone", name));
        return false;
    }
    true
}

// Prose test 12. Explicit Encryption (Case 1: can insert encrypted indexed and find)
#[tokio::test]
async fn explicit_encryption_case_1() -> Result<()> {
    if !fle2v2_ok("explicit_encryption_case_1").await {
        return Ok(());
    }

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
            "encrypted indexed value",
            EncryptKey::Id(testdata.key1_id.clone()),
            Algorithm::Indexed,
        )
        .contention_factor(0)
        .await?;
    enc_coll
        .insert_one(doc! { "encryptedIndexed": insert_payload })
        .await?;

    let find_payload = testdata
        .client_encryption
        .encrypt(
            "encrypted indexed value",
            EncryptKey::Id(testdata.key1_id),
            Algorithm::Indexed,
        )
        .query_type("equality".to_string())
        .contention_factor(0)
        .await?;
    let found: Vec<_> = enc_coll
        .find(doc! { "encryptedIndexed": find_payload })
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
#[tokio::test]
async fn explicit_encryption_case_2() -> Result<()> {
    if !fle2v2_ok("explicit_encryption_case_2").await {
        return Ok(());
    }

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
                "encrypted indexed value",
                EncryptKey::Id(testdata.key1_id.clone()),
                Algorithm::Indexed,
            )
            .contention_factor(10)
            .await?;
        enc_coll
            .insert_one(doc! { "encryptedIndexed": insert_payload })
            .await?;
    }

    let find_payload = testdata
        .client_encryption
        .encrypt(
            "encrypted indexed value",
            EncryptKey::Id(testdata.key1_id.clone()),
            Algorithm::Indexed,
        )
        .query_type("equality".to_string())
        .contention_factor(0)
        .await?;
    let found: Vec<_> = enc_coll
        .find(doc! { "encryptedIndexed": find_payload })
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
            "encrypted indexed value",
            EncryptKey::Id(testdata.key1_id.clone()),
            Algorithm::Indexed,
        )
        .query_type("equality")
        .contention_factor(10)
        .await?;
    let found: Vec<_> = enc_coll
        .find(doc! { "encryptedIndexed": find_payload2 })
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
#[tokio::test]
async fn explicit_encryption_case_3() -> Result<()> {
    if !fle2v2_ok("explicit_encryption_case_3").await {
        return Ok(());
    }

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
            "encrypted unindexed value",
            EncryptKey::Id(testdata.key1_id.clone()),
            Algorithm::Unindexed,
        )
        .await?;
    enc_coll
        .insert_one(doc! { "_id": 1, "encryptedUnindexed": insert_payload })
        .await?;

    let found: Vec<_> = enc_coll
        .find(doc! { "_id": 1 })
        .await?
        .try_collect()
        .await?;
    assert_eq!(1, found.len());
    assert_eq!(
        "encrypted unindexed value",
        found[0].get_str("encryptedUnindexed")?
    );

    Ok(())
}

// Prose test 12. Explicit Encryption (Case 4: can roundtrip encrypted indexed)
#[tokio::test]
async fn explicit_encryption_case_4() -> Result<()> {
    if !fle2v2_ok("explicit_encryption_case_4").await {
        return Ok(());
    }

    let testdata = match explicit_encryption_setup().await? {
        Some(t) => t,
        None => return Ok(()),
    };

    let raw_value = RawBson::String("encrypted indexed value".to_string());
    let payload = testdata
        .client_encryption
        .encrypt(
            raw_value.clone(),
            EncryptKey::Id(testdata.key1_id.clone()),
            Algorithm::Indexed,
        )
        .contention_factor(0)
        .await?;
    let roundtrip = testdata
        .client_encryption
        .decrypt(payload.as_raw_binary())
        .await?;
    assert_eq!(raw_value, roundtrip);

    Ok(())
}

// Prose test 12. Explicit Encryption (Case 5: can roundtrip encrypted unindexed)
#[tokio::test]
async fn explicit_encryption_case_5() -> Result<()> {
    if !fle2v2_ok("explicit_encryption_case_5").await {
        return Ok(());
    }

    let testdata = match explicit_encryption_setup().await? {
        Some(t) => t,
        None => return Ok(()),
    };

    let raw_value = RawBson::String("encrypted unindexed value".to_string());
    let payload = testdata
        .client_encryption
        .encrypt(
            raw_value.clone(),
            EncryptKey::Id(testdata.key1_id.clone()),
            Algorithm::Unindexed,
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
    let key_vault_client = Client::for_test().await;
    if key_vault_client.server_version_lt(6, 0) {
        log_uncaptured("skipping explicit encryption test: server below 6.0");
        return Ok(None);
    }
    if key_vault_client.is_standalone() {
        log_uncaptured("skipping explicit encryption test: cannot run on standalone");
        return Ok(None);
    }

    let encrypted_fields = load_testdata("data/encryptedFields.json")?;
    let key1_document = load_testdata("data/keys/key1-document.json")?;
    let key1_id = match key1_document.get("_id").unwrap() {
        Bson::Binary(b) => b.clone(),
        v => return Err(failure!("expected binary _id, got {:?}", v)),
    };

    let db = key_vault_client.database("db");
    db.collection::<Document>("explicit_encryption")
        .drop()
        .encrypted_fields(encrypted_fields.clone())
        .await?;
    db.create_collection("explicit_encryption")
        .encrypted_fields(encrypted_fields)
        .await?;
    let keyvault = key_vault_client.database("keyvault");
    keyvault.collection::<Document>("datakeys").drop().await?;
    keyvault.create_collection("datakeys").await?;
    keyvault
        .collection::<Document>("datakeys")
        .insert_one(key1_document)
        .write_concern(WriteConcern::majority())
        .await?;

    let client_encryption = ClientEncryption::new(
        key_vault_client.into_client(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?;
    let encrypted_client = Client::encrypted_builder(
        get_client_options().await.clone(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?
    .bypass_query_analysis(true)
    .extra_options(EXTRA_OPTIONS.clone())
    .disable_crypt_shared(*DISABLE_CRYPT_SHARED)
    .build()
    .await?;

    Ok(Some(ExplicitEncryptionTestData {
        key1_id,
        client_encryption,
        encrypted_client,
    }))
}

// Prose test 13. Unique Index on keyAltNames (Case 1: createDataKey())
#[tokio::test]
async fn unique_index_keyaltnames_create_data_key() -> Result<()> {
    let (client_encryption, _) = unique_index_keyaltnames_setup().await?;

    // Succeeds
    client_encryption
        .create_data_key(LocalMasterKey::builder().build())
        .key_alt_names(vec!["abc".to_string()])
        .await?;
    // Fails: duplicate key
    let err = client_encryption
        .create_data_key(LocalMasterKey::builder().build())
        .key_alt_names(vec!["abc".to_string()])
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
        .create_data_key(LocalMasterKey::builder().build())
        .key_alt_names(vec!["def".to_string()])
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
#[tokio::test]
async fn unique_index_keyaltnames_add_key_alt_name() -> Result<()> {
    let (client_encryption, key) = unique_index_keyaltnames_setup().await?;

    // Succeeds
    let new_key = client_encryption
        .create_data_key(LocalMasterKey::builder().build())
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
    if let Some(code) = err.sdam_code() {
        return Some(code);
    }
    match *err.kind {
        ErrorKind::Write(WriteFailure::WriteError(WriteError { code, .. })) => Some(code),
        _ => None,
    }
}

async fn unique_index_keyaltnames_setup() -> Result<(ClientEncryption, Binary)> {
    let client = Client::for_test().await;
    let datakeys = client
        .database("keyvault")
        .collection::<Document>("datakeys");
    datakeys.drop().await?;
    datakeys
        .create_index(IndexModel {
            keys: doc! { "keyAltNames": 1 },
            options: Some(
                IndexOptions::builder()
                    .name("keyAltNames_1".to_string())
                    .unique(true)
                    .partial_filter_expression(doc! { "keyAltNames": { "$exists": true } })
                    .build(),
            ),
        })
        .write_concern(WriteConcern::majority())
        .await?;
    let client_encryption = ClientEncryption::new(
        client.into_client(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?;
    let key = client_encryption
        .create_data_key(LocalMasterKey::builder().build())
        .key_alt_names(vec!["def".to_string()])
        .await?;
    Ok((client_encryption, key))
}

// Prose test 14. Decryption Events (Case 1: Command Error)
#[tokio::test(flavor = "multi_thread")]
async fn decryption_events_command_error() -> Result<()> {
    let td = match DecryptionEventsTestdata::setup().await? {
        Some(v) => v,
        None => return Ok(()),
    };

    let fail_point =
        FailPoint::fail_command(&["aggregate"], FailPointMode::Times(1)).error_code(123);
    let _guard = td.setup_client.enable_fail_point(fail_point).await.unwrap();
    let err = td
        .decryption_events
        .aggregate(vec![doc! { "$count": "total" }])
        .await
        .unwrap_err();
    assert_eq!(Some(123), err.sdam_code());
    assert!(td.ev_handler.failed.lock().unwrap().is_some());

    Ok(())
}

// Prose test 14. Decryption Events (Case 2: Network Error)
#[tokio::test(flavor = "multi_thread")]
async fn decryption_events_network_error() -> Result<()> {
    let td = match DecryptionEventsTestdata::setup().await? {
        Some(v) => v,
        None => return Ok(()),
    };

    let fail_point = FailPoint::fail_command(&["aggregate"], FailPointMode::Times(1))
        .error_code(123)
        .close_connection(true);
    let _guard = td.setup_client.enable_fail_point(fail_point).await.unwrap();
    let err = td
        .decryption_events
        .aggregate(vec![doc! { "$count": "total" }])
        .await
        .unwrap_err();
    assert!(err.is_network_error(), "unexpected error: {}", err);
    assert!(td.ev_handler.failed.lock().unwrap().is_some());

    Ok(())
}

// Prose test 14. Decryption Events (Case 3: Decrypt Error)
#[tokio::test]
async fn decryption_events_decrypt_error() -> Result<()> {
    let td = match DecryptionEventsTestdata::setup().await? {
        Some(v) => v,
        None => return Ok(()),
    };
    td.decryption_events
        .insert_one(doc! { "encrypted": td.malformed_ciphertext })
        .await?;
    let err = td.decryption_events.aggregate(vec![]).await.unwrap_err();
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
#[tokio::test]
async fn decryption_events_decrypt_success() -> Result<()> {
    let td = match DecryptionEventsTestdata::setup().await? {
        Some(v) => v,
        None => return Ok(()),
    };
    td.decryption_events
        .insert_one(doc! { "encrypted": td.ciphertext })
        .await?;
    td.decryption_events.aggregate(vec![]).await?;
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
        let setup_client = Client::for_test().await;
        if !setup_client.is_standalone() {
            log_uncaptured("skipping decryption events test: requires standalone topology");
            return Ok(None);
        }
        let db = setup_client.database("db");
        db.collection::<Document>("decryption_events")
            .drop()
            .await?;
        db.create_collection("decryption_events").await?;

        let client_encryption = ClientEncryption::new(
            setup_client.clone().into_client(),
            KV_NAMESPACE.clone(),
            vec![LOCAL_KMS.clone()],
        )?;
        let key_id = client_encryption
            .create_data_key(LocalMasterKey::builder().build())
            .await?;
        let ciphertext = client_encryption
            .encrypt("hello", EncryptKey::Id(key_id), Algorithm::Deterministic)
            .await?;
        let mut malformed_ciphertext = ciphertext.clone();
        let last = malformed_ciphertext.bytes.last_mut().unwrap();
        *last = last.wrapping_add(1);

        let ev_handler = DecryptionEventsHandler::new();
        let mut opts = get_client_options().await.clone();
        opts.retry_reads = Some(false);
        opts.command_event_handler = Some(ev_handler.clone().into());
        let encrypted_client =
            Client::encrypted_builder(opts, KV_NAMESPACE.clone(), vec![LOCAL_KMS.clone()])?
                .extra_options(EXTRA_OPTIONS.clone())
                .disable_crypt_shared(*DISABLE_CRYPT_SHARED)
                .build()
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

#[allow(deprecated)]
impl crate::event::command::CommandEventHandler for DecryptionEventsHandler {
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

// Prose test 15. On-demand AWS Credentials (failure)
#[cfg(feature = "aws-auth")] // isabeltodo make sure this is being run
#[tokio::test]
async fn on_demand_aws_failure() -> Result<()> {
    let ce = ClientEncryption::new(
        Client::for_test().await.into_client(),
        KV_NAMESPACE.clone(),
        [(KmsProvider::aws(), doc! {}, None)],
    )?;
    let result = ce
        .create_data_key(
            AwsMasterKey::builder()
                .region("us-east-1")
                .key("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0")
                .build(),
        )
        .await;
    assert!(result.is_err(), "Expected error, got {:?}", result);

    Ok(())
}

// Prose test 15. On-demand AWS Credentials (success)
#[cfg(feature = "aws-auth")] // isabeltodo make sure this is being run
#[tokio::test]
async fn on_demand_aws_success() -> Result<()> {
    let ce = ClientEncryption::new(
        Client::for_test().await.into_client(),
        KV_NAMESPACE.clone(),
        [(KmsProvider::aws(), doc! {}, None)],
    )?;
    ce.create_data_key(
        AwsMasterKey::builder()
            .region("us-east-1")
            .key("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0")
            .build(),
    )
    .await?;

    Ok(())
}

// TODO RUST-1441: implement prose test 16. Rewrap

// Prose test 17. On-demand GCP Credentials
#[cfg(feature = "gcp-kms")] // isabeltodo figure out when this is being run
#[tokio::test]
async fn on_demand_gcp_credentials() -> Result<()> {
    let util_client = Client::for_test().await.into_client();
    let client_encryption = ClientEncryption::new(
        util_client,
        KV_NAMESPACE.clone(),
        [(KmsProvider::gcp(), doc! {}, None)],
    )?;

    let result = client_encryption
        .create_data_key(
            GcpMasterKey::builder()
                .project_id("devprod-drivers")
                .location("global")
                .key_ring("key-ring-csfle")
                .key_name("key-name-csfle")
                .build(),
        )
        .await;

    if env::var("ON_DEMAND_GCP_CREDS_SHOULD_SUCCEED").is_ok() {
        result.unwrap();
    } else {
        let error = result.unwrap_err();
        match *error.kind {
            ErrorKind::Encryption(e) => {
                assert!(matches!(e.kind, mongocrypt::error::ErrorKind::Kms));
                assert!(e.message.unwrap().contains("GCP credentials"));
            }
            other => panic!("Expected encryption error, got {:?}", other),
        }
    }

    Ok(())
}

// Prose test 18. Azure IMDS Credentials
#[cfg(feature = "azure-kms")]
#[tokio::test]
async fn azure_imds() -> Result<()> {
    let mut azure_exec = crate::client::csfle::state_machine::azure::ExecutorState::new()?;
    azure_exec.test_host = Some((
        "localhost",
        env::var("AZURE_IMDS_MOCK_PORT").unwrap().parse().unwrap(),
    ));

    // Case 1: Success
    {
        let now = std::time::Instant::now();
        let token = azure_exec.get_token().await?;
        assert_eq!(token, rawdoc! { "accessToken": "magic-cookie" });
        let cached = azure_exec.take_cached().await.expect("cached token");
        assert_eq!(cached.server_response.expires_in, "70");
        assert_eq!(cached.server_response.resource, "https://vault.azure.net");
        assert!((65..75).contains(&cached.expire_time.duration_since(now).as_secs()));
    }

    // Case 2: Empty JSON
    {
        azure_exec.test_param = Some("case=empty-json");
        let result = azure_exec.get_token().await;
        assert!(result.is_err(), "expected err got {:?}", result);
        assert!(result.unwrap_err().is_auth_error());
    }

    // Case 3: Bad JSON
    {
        azure_exec.test_param = Some("case=bad-json");
        let result = azure_exec.get_token().await;
        assert!(result.is_err(), "expected err got {:?}", result);
        assert!(result.unwrap_err().is_auth_error());
    }

    // Case 4: HTTP 404
    {
        azure_exec.test_param = Some("case=404");
        let result = azure_exec.get_token().await;
        assert!(result.is_err(), "expected err got {:?}", result);
        assert!(result.unwrap_err().is_auth_error());
    }

    // Case 5: HTTP 500
    {
        azure_exec.test_param = Some("case=500");
        let result = azure_exec.get_token().await;
        assert!(result.is_err(), "expected err got {:?}", result);
        assert!(result.unwrap_err().is_auth_error());
    }

    // Case 6: Slow Response
    {
        azure_exec.test_param = Some("case=slow");
        let result = azure_exec.get_token().await;
        assert!(result.is_err(), "expected err got {:?}", result);
        assert!(result.unwrap_err().is_auth_error());
    }

    Ok(())
}

// Prose test 19. Azure IMDS Credentials Integration Test (case 1: failure)
#[cfg(feature = "azure-kms")]
#[tokio::test]
async fn azure_imds_integration_failure() -> Result<()> {
    let c = ClientEncryption::new(
        Client::for_test().await.into_client(),
        KV_NAMESPACE.clone(),
        [(KmsProvider::azure(), doc! {}, None)],
    )?;

    let result = c
        .create_data_key(
            AzureMasterKey::builder()
                .key_vault_endpoint("https://keyvault-drivers-2411.vault.azure.net/keys/")
                .key_name("KEY-NAME")
                .build(),
        )
        .await;

    assert!(result.is_err(), "expected error, got {:?}", result);
    assert!(result.unwrap_err().is_auth_error());

    Ok(())
}

// Prose test 20. Bypass creating mongocryptd client when shared library is loaded
#[tokio::test]
async fn bypass_mongocryptd_client() -> Result<()> {
    if *DISABLE_CRYPT_SHARED {
        log_uncaptured("Skipping bypass mongocryptd client test: crypt_shared is disabled.");
        return Ok(());
    }

    let connected = Arc::new(AtomicBool::new(false));
    {
        let connected = Arc::clone(&connected);
        let listener = bind("127.0.0.1:27021").await?;
        runtime::spawn(async move {
            let _ = listener.accept().await;
            log_uncaptured("test failure: connection accepted");
            connected.store(true, Ordering::SeqCst);
        })
    };

    let client_encrypted = Client::encrypted_builder(
        get_client_options().await.clone(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?
    .extra_options({
        let mut extra_options = EXTRA_OPTIONS.clone();
        extra_options.insert("mongocryptdURI", "mongodb://localhost:27021");
        extra_options
    })
    .build()
    .await?;
    client_encrypted
        .database("db")
        .collection::<Document>("coll")
        .insert_one(doc! { "unencrypted": "test" })
        .await?;

    assert!(!client_encrypted.has_mongocryptd_client().await);
    assert!(!connected.load(Ordering::SeqCst));

    Ok(())
}

// Prost test 21. Automatic Data Encryption Keys
#[tokio::test]
async fn auto_encryption_keys_local() -> Result<()> {
    auto_encryption_keys(LocalMasterKey::builder().build()).await
}

#[tokio::test]
async fn auto_encryption_keys_aws() -> Result<()> {
    auto_encryption_keys(
        AwsMasterKey::builder()
            .region("us-east-1")
            .key("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0")
            .build(),
    )
    .await
}

async fn auto_encryption_keys(master_key: impl Into<MasterKey>) -> Result<()> {
    let master_key = master_key.into();

    if !fle2v2_ok("auto_encryption_keys").await {
        return Ok(());
    }

    let client = Client::for_test().await;
    if client.server_version_lt(6, 0) {
        log_uncaptured("Skipping auto_encryption_key test: server < 6.0");
        return Ok(());
    }
    if client.is_standalone() {
        log_uncaptured("Skipping auto_encryption_key test: standalone server");
        return Ok(());
    }
    let db = client.database("test_auto_encryption_keys");
    db.drop().await?;
    let ce = ClientEncryption::new(
        client.into_client(),
        KV_NAMESPACE.clone(),
        vec![AWS_KMS.clone(), LOCAL_KMS.clone()],
    )?;

    // Case 1: Simple Creation and Validation
    ce.create_encrypted_collection(&db, "case_1", master_key.clone())
        .encrypted_fields(doc! {
            "fields": [{
                "path": "ssn",
                "bsonType": "string",
                "keyId": Bson::Null,
            }],
        })
        .await
        .1?;
    let coll = db.collection::<Document>("case_1");
    let result = coll.insert_one(doc! { "ssn": "123-45-6789" }).await;
    assert!(
        result.as_ref().unwrap_err().code() == Some(121),
        "Expected error 121 (failed validation), got {:?}",
        result
    );

    // Case 2: Missing encryptedFields
    let result = ce
        .create_encrypted_collection(&db, "case_2", master_key.clone())
        .await
        .1;
    assert!(
        result.as_ref().unwrap_err().is_invalid_argument(),
        "Expected invalid argument error, got {:?}",
        result
    );

    // Case 3: Invalid keyId
    let result = ce
        .create_encrypted_collection(&db, "case_1", master_key.clone())
        .encrypted_fields(doc! {
            "fields": [{
                "path": "ssn",
                "bsonType": "string",
                "keyId": false,
            }],
        })
        .await
        .1;
    assert!(
        result.as_ref().unwrap_err().code() == Some(14),
        "Expected error 14 (type mismatch), got {:?}",
        result
    );

    // Case 4: Insert encrypted value
    let (ef, result) = ce
        .create_encrypted_collection(&db, "case_4", master_key.clone())
        .encrypted_fields(doc! {
            "fields": [{
                "path": "ssn",
                "bsonType": "string",
                "keyId": Bson::Null,
            }],
        })
        .await;
    result?;
    let key = match ef.get_array("fields")?[0]
        .as_document()
        .unwrap()
        .get("keyId")
        .unwrap()
    {
        Bson::Binary(bin) => bin.clone(),
        v => panic!("invalid keyId {:?}", v),
    };
    let encrypted_payload = ce.encrypt("123-45-6789", key, Algorithm::Unindexed).await?;
    let coll = db.collection::<Document>("case_1");
    coll.insert_one(doc! { "ssn": encrypted_payload }).await?;

    Ok(())
}

// Prose test 22. Range explicit encryption
#[tokio::test]
async fn range_explicit_encryption() -> Result<()> {
    if !fle2v2_ok("range_explicit_encryption").await {
        return Ok(());
    }
    let client = Client::for_test().await;
    if client.server_version_lt(8, 0) || client.is_standalone() {
        log_uncaptured("Skipping range_explicit_encryption due to unsupported topology");
        return Ok(());
    }

    range_explicit_encryption_test(
        "DecimalNoPrecision",
        RangeOptions::builder().sparsity(1).trim_factor(1).build(),
    )
    .await?;
    range_explicit_encryption_test(
        "DecimalPrecision",
        RangeOptions::builder()
            .trim_factor(1)
            .sparsity(1)
            .min(Bson::Decimal128("0".parse()?))
            .max(Bson::Decimal128("200".parse()?))
            .precision(2)
            .build(),
    )
    .await?;
    range_explicit_encryption_test(
        "DoubleNoPrecision",
        RangeOptions::builder().trim_factor(1).sparsity(1).build(),
    )
    .await?;
    range_explicit_encryption_test(
        "DoublePrecision",
        RangeOptions::builder()
            .trim_factor(1)
            .sparsity(1)
            .min(Bson::Double(0.0))
            .max(Bson::Double(200.0))
            .precision(2)
            .build(),
    )
    .await?;
    range_explicit_encryption_test(
        "Date",
        RangeOptions::builder()
            .trim_factor(1)
            .sparsity(1)
            .min(Bson::DateTime(DateTime::from_millis(0)))
            .max(Bson::DateTime(DateTime::from_millis(200)))
            .build(),
    )
    .await?;
    range_explicit_encryption_test(
        "Int",
        RangeOptions::builder()
            .trim_factor(1)
            .sparsity(1)
            .min(Bson::Int32(0))
            .max(Bson::Int32(200))
            .build(),
    )
    .await?;
    range_explicit_encryption_test(
        "Long",
        RangeOptions::builder()
            .trim_factor(1)
            .sparsity(1)
            .min(Bson::Int64(0))
            .max(Bson::Int64(200))
            .build(),
    )
    .await?;

    Ok(())
}

async fn range_explicit_encryption_test(
    bson_type: &str,
    range_options: RangeOptions,
) -> Result<()> {
    let util_client = Client::for_test().await;

    let encrypted_fields =
        load_testdata(&format!("data/range-encryptedFields-{}.json", bson_type))?;

    let key1_document = load_testdata("data/keys/key1-document.json")?;
    let key1_id = match key1_document.get("_id").unwrap() {
        Bson::Binary(binary) => binary,
        _ => unreachable!(),
    }
    .clone();

    let explicit_encryption_collection = util_client
        .database("db")
        .collection::<Document>("explicit_encryption");
    explicit_encryption_collection
        .drop()
        .encrypted_fields(encrypted_fields.clone())
        .await?;
    util_client
        .database("db")
        .create_collection("explicit_encryption")
        .encrypted_fields(encrypted_fields.clone())
        .await?;

    let datakeys_collection = util_client
        .database("keyvault")
        .collection::<Document>("datakeys");
    datakeys_collection.drop().await?;
    util_client
        .database("keyvault")
        .create_collection("datakeys")
        .await?;

    datakeys_collection
        .insert_one(key1_document)
        .write_concern(WriteConcern::majority())
        .await?;

    let key_vault_client = Client::for_test().await;

    let client_encryption = ClientEncryption::new(
        key_vault_client.into_client(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?;

    let encrypted_client = Client::encrypted_builder(
        get_client_options().await.clone(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?
    .extra_options(EXTRA_OPTIONS.clone())
    .bypass_query_analysis(true)
    .build()
    .await?;

    let key = format!("encrypted{}", bson_type);
    let bson_numbers: BTreeMap<i32, RawBson> = [0, 6, 30, 200]
        .iter()
        .map(|num| (*num, get_raw_bson_from_num(bson_type, *num)))
        .collect();
    let explicit_encryption_collection = encrypted_client
        .database("db")
        .collection("explicit_encryption");

    for (id, num) in bson_numbers.keys().enumerate() {
        let encrypted_value = client_encryption
            .encrypt(bson_numbers[num].clone(), key1_id.clone(), Algorithm::Range)
            .contention_factor(0)
            .range_options(range_options.clone())
            .await?;

        explicit_encryption_collection
            .insert_one(doc! {
                &key: encrypted_value,
                "_id": id as i32,
            })
            .await?;
    }

    // Case 1: Decrypt a payload
    let insert_payload = client_encryption
        .encrypt(bson_numbers[&6].clone(), key1_id.clone(), Algorithm::Range)
        .contention_factor(0)
        .range_options(range_options.clone())
        .await?;

    let decrypted = client_encryption
        .decrypt(insert_payload.as_raw_binary())
        .await?;
    assert_eq!(decrypted, bson_numbers[&6]);

    // Utilities for cases 2-5
    let explicit_encryption_collection =
        explicit_encryption_collection.clone_with_type::<RawDocumentBuf>();
    let find_options = FindOptions::builder().sort(doc! { "_id": 1 }).build();
    let assert_success = |actual: Vec<RawDocumentBuf>, expected: &[i32]| {
        assert_eq!(actual.len(), expected.len());
        for (idx, num) in expected.iter().enumerate() {
            assert_eq!(
                actual[idx].get(&key),
                Ok(Some(bson_numbers[num].as_raw_bson_ref()))
            );
        }
    };

    // Case 2: Find encrypted range and return the maximum
    let query = rawdoc! {
        "$and": [
            { &key: { "$gte": bson_numbers[&6].clone() } },
            { &key: { "$lte": bson_numbers[&200].clone() } },
        ]
    };
    let find_payload = client_encryption
        .encrypt_expression(query, key1_id.clone())
        .contention_factor(0)
        .range_options(range_options.clone())
        .await?;

    let docs: Vec<RawDocumentBuf> = explicit_encryption_collection
        .find(find_payload)
        .with_options(find_options.clone())
        .await?
        .try_collect()
        .await?;
    assert_success(docs, &[6, 30, 200]);

    // Case 3: Find encrypted range and return the minimum
    let query = rawdoc! {
        "$and": [
            { &key: { "$gte": bson_numbers[&0].clone() } },
            { &key: { "$lte": bson_numbers[&6].clone() } },
        ]
    };
    let find_payload = client_encryption
        .encrypt_expression(query, key1_id.clone())
        .contention_factor(0)
        .range_options(range_options.clone())
        .await?;

    let docs: Vec<RawDocumentBuf> = encrypted_client
        .database("db")
        .collection("explicit_encryption")
        .find(find_payload)
        .with_options(find_options.clone())
        .await?
        .try_collect()
        .await?;
    assert_success(docs, &[0, 6]);

    // Case 4: Find encrypted range with an open range query
    let query = rawdoc! {
        "$and": [
            { &key: { "$gt": bson_numbers[&30].clone() } },
        ]
    };
    let find_payload = client_encryption
        .encrypt_expression(query, key1_id.clone())
        .contention_factor(0)
        .range_options(range_options.clone())
        .await?;

    let docs: Vec<RawDocumentBuf> = encrypted_client
        .database("db")
        .collection("explicit_encryption")
        .find(find_payload)
        .with_options(find_options.clone())
        .await?
        .try_collect()
        .await?;
    assert_success(docs, &[200]);

    // Case 5: Run an aggregation expression inside $expr
    let query = rawdoc! { "$and": [ { "$lt": [ format!("${key}"), get_raw_bson_from_num(bson_type, 30) ] } ] };
    let find_payload = client_encryption
        .encrypt_expression(query, key1_id.clone())
        .contention_factor(0)
        .range_options(range_options.clone())
        .await?;

    let docs: Vec<RawDocumentBuf> = encrypted_client
        .database("db")
        .collection("explicit_encryption")
        .find(doc! { "$expr": find_payload })
        .with_options(find_options.clone())
        .await?
        .try_collect()
        .await?;
    assert_success(docs, &[0, 6]);

    // Case 6: Encrypting a document greater than the maximum errors
    if bson_type != "DoubleNoPrecision" && bson_type != "DecimalNoPrecision" {
        let num = get_raw_bson_from_num(bson_type, 201);
        let error = client_encryption
            .encrypt(num, key1_id.clone(), Algorithm::Range)
            .contention_factor(0)
            .range_options(range_options.clone())
            .await
            .unwrap_err();
        assert!(matches!(*error.kind, ErrorKind::Encryption(_)));
    }

    // Case 7: Encrypting a document of a different type errors
    if bson_type != "DoubleNoPrecision" && bson_type != "DecimalNoPrecision" {
        let value = if bson_type == "Int" {
            rawdoc! { &key: { "$numberDouble": "6" } }
        } else {
            rawdoc! { &key: { "$numberInt": "6" } }
        };
        let error = client_encryption
            .encrypt(value, key1_id.clone(), Algorithm::Range)
            .contention_factor(0)
            .range_options(range_options.clone())
            .await
            .unwrap_err();
        assert!(matches!(*error.kind, ErrorKind::Encryption(_)));
    }

    // Case 8: Setting precision errors if the type is not a double
    if !bson_type.contains("Double") && !bson_type.contains("Decimal") {
        let range_options = RangeOptions::builder()
            .sparsity(1)
            .min(get_bson_from_num(bson_type, 0))
            .max(get_bson_from_num(bson_type, 200))
            .precision(2)
            .build();
        let error = client_encryption
            .encrypt(bson_numbers[&6].clone(), key1_id.clone(), Algorithm::Range)
            .contention_factor(0)
            .range_options(range_options)
            .await
            .unwrap_err();
        assert!(matches!(*error.kind, ErrorKind::Encryption(_)));
    }

    Ok(())
}

fn get_bson_from_num(bson_type: &str, num: i32) -> Bson {
    match bson_type {
        "DecimalNoPrecision" | "DecimalPrecision" => {
            Bson::Decimal128(num.to_string().parse().unwrap())
        }
        "DoubleNoPrecision" | "DoublePrecision" => Bson::Double(num as f64),
        "Date" => Bson::DateTime(DateTime::from_millis(num as i64)),
        "Int" => Bson::Int32(num),
        "Long" => Bson::Int64(num as i64),
        _ => unreachable!(),
    }
}

fn get_raw_bson_from_num(bson_type: &str, num: i32) -> RawBson {
    match bson_type {
        "DecimalNoPrecision" | "DecimalPrecision" => {
            RawBson::Decimal128(num.to_string().parse().unwrap())
        }
        "DoubleNoPrecision" | "DoublePrecision" => RawBson::Double(num as f64),
        "Date" => RawBson::DateTime(DateTime::from_millis(num as i64)),
        "Int" => RawBson::Int32(num),
        "Long" => RawBson::Int64(num as i64),
        _ => unreachable!(),
    }
}

async fn bind(addr: &str) -> Result<TcpListener> {
    Ok(TcpListener::bind(addr.parse::<std::net::SocketAddr>()?).await?)
}

// Prose test 23. Range explicit encryption applies defaults
#[tokio::test]
async fn range_explicit_encryption_defaults() -> Result<()> {
    // Setup
    let key_vault_client = Client::for_test().await;
    let client_encryption = ClientEncryption::new(
        key_vault_client.into_client(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?;
    let key_id = client_encryption
        .create_data_key(LocalMasterKey::builder().build())
        .await?;
    let payload_defaults = client_encryption
        .encrypt(123, key_id.clone(), Algorithm::Range)
        .contention_factor(0)
        .range_options(
            RangeOptions::builder()
                .min(Bson::from(0))
                .max(Bson::from(1000))
                .build(),
        )
        .await?;

    // Case 1: Uses libmongocrypt defaults
    let payload = client_encryption
        .encrypt(123, key_id.clone(), Algorithm::Range)
        .contention_factor(0)
        .range_options(
            RangeOptions::builder()
                .min(Bson::from(0))
                .max(Bson::from(1000))
                .sparsity(2)
                .trim_factor(6)
                .build(),
        )
        .await?;
    assert_eq!(payload_defaults.bytes.len(), payload.bytes.len());

    // Case 2: Accepts trimFactor 0
    let payload = client_encryption
        .encrypt(123, key_id.clone(), Algorithm::Range)
        .contention_factor(0)
        .range_options(
            RangeOptions::builder()
                .min(Bson::from(0))
                .max(Bson::from(1000))
                .trim_factor(0)
                .build(),
        )
        .await?;
    assert!(payload.bytes.len() > payload_defaults.bytes.len());

    Ok(())
}

// Prose Test 24. KMS Retry Tests
#[tokio::test]
// using openssl causes errors after configuring a network failpoint
#[cfg(not(feature = "openssl-tls"))]
async fn kms_retry() {
    if *super::SERVERLESS {
        log_uncaptured("skipping kms_retry on serverless");
        return;
    }

    use reqwest::{Certificate, Client as HttpClient};

    let endpoint = "127.0.0.1:9003";

    let mut certificate_file_path = PathBuf::from(&*CSFLE_TLS_CERT_DIR);
    certificate_file_path.push("ca.pem");
    let certificate_file = std::fs::read(&certificate_file_path).unwrap();

    let set_failpoint = |kind: &str, count: u8| {
        // create a fresh client for each request to avoid hangs
        let http_client = HttpClient::builder()
            .add_root_certificate(Certificate::from_pem(&certificate_file).unwrap())
            .build()
            .unwrap();
        let url = format!("https://localhost:9003/set_failpoint/{}", kind);
        let body = format!("{{\"count\":{}}}", count);
        http_client.post(url).body(body).send()
    };

    let aws_kms = AWS_KMS.clone();
    let mut azure_kms = AZURE_KMS.clone();
    azure_kms.1.insert("identityPlatformEndpoint", endpoint);
    let mut gcp_kms = GCP_KMS.clone();
    gcp_kms.1.insert("endpoint", endpoint);
    let mut kms_providers = vec![aws_kms, azure_kms, gcp_kms];

    let tls_options = get_client_options().await.tls_options();
    for kms_provider in kms_providers.iter_mut() {
        kms_provider.2 = tls_options.clone();
    }

    let key_vault_client = Client::for_test().await.into_client();
    let client_encryption = ClientEncryption::new(
        key_vault_client,
        Namespace::new("keyvault", "datakeys"),
        kms_providers,
    )
    .unwrap();

    let aws_master_key = AwsMasterKey::builder()
        .region("foo")
        .key("bar")
        .endpoint(endpoint.to_string())
        .build();
    let azure_master_key = AzureMasterKey::builder()
        .key_vault_endpoint(endpoint)
        .key_name("foo")
        .build();
    let gcp_master_key = GcpMasterKey::builder()
        .project_id("foo")
        .location("bar")
        .key_ring("baz")
        .key_name("qux")
        .endpoint(endpoint.to_string())
        .build();

    // Case 1: createDataKey and encrypt with TCP retry

    // AWS
    set_failpoint("network", 1).await.unwrap();
    let key_id = client_encryption
        .create_data_key(aws_master_key.clone())
        .await
        .unwrap();
    set_failpoint("network", 1).await.unwrap();
    client_encryption
        .encrypt(123, key_id, Algorithm::Deterministic)
        .await
        .unwrap();

    // Azure
    set_failpoint("network", 1).await.unwrap();
    let key_id = client_encryption
        .create_data_key(azure_master_key.clone())
        .await
        .unwrap();
    set_failpoint("network", 1).await.unwrap();
    client_encryption
        .encrypt(123, key_id, Algorithm::Deterministic)
        .await
        .unwrap();

    // GCP
    set_failpoint("network", 1).await.unwrap();
    let key_id = client_encryption
        .create_data_key(gcp_master_key.clone())
        .await
        .unwrap();
    set_failpoint("network", 1).await.unwrap();
    client_encryption
        .encrypt(123, key_id, Algorithm::Deterministic)
        .await
        .unwrap();

    // Case 2: createDataKey and encrypt with HTTP retry

    // AWS
    set_failpoint("http", 1).await.unwrap();
    let key_id = client_encryption
        .create_data_key(aws_master_key.clone())
        .await
        .unwrap();
    set_failpoint("http", 1).await.unwrap();
    client_encryption
        .encrypt(123, key_id, Algorithm::Deterministic)
        .await
        .unwrap();

    // Azure
    set_failpoint("http", 1).await.unwrap();
    let key_id = client_encryption
        .create_data_key(azure_master_key.clone())
        .await
        .unwrap();
    set_failpoint("http", 1).await.unwrap();
    client_encryption
        .encrypt(123, key_id, Algorithm::Deterministic)
        .await
        .unwrap();

    // GCP
    set_failpoint("http", 1).await.unwrap();
    let key_id = client_encryption
        .create_data_key(gcp_master_key.clone())
        .await
        .unwrap();
    set_failpoint("http", 1).await.unwrap();
    client_encryption
        .encrypt(123, key_id, Algorithm::Deterministic)
        .await
        .unwrap();

    // Case 3: createDataKey fails after too many retries

    // AWS
    set_failpoint("network", 4).await.unwrap();
    client_encryption
        .create_data_key(aws_master_key)
        .await
        .unwrap_err();

    // Azure
    set_failpoint("network", 4).await.unwrap();
    client_encryption
        .create_data_key(azure_master_key)
        .await
        .unwrap_err();

    // GCP
    set_failpoint("network", 4).await.unwrap();
    client_encryption
        .create_data_key(gcp_master_key)
        .await
        .unwrap_err();
}

// FLE 2.0 Documentation Example
#[tokio::test]
async fn fle2_example() -> Result<()> {
    // FLE 2 is not supported on Standalone topology.
    let test_client = Client::for_test().await;
    if test_client.server_version_lt(7, 0) {
        log_uncaptured("skipping fle2 example: server below 7.0");
        return Ok(());
    }
    if test_client.is_standalone() {
        log_uncaptured("skipping fle2 example: cannot run on standalone");
        return Ok(());
    }

    // Drop data from prior test runs.
    test_client
        .database("keyvault")
        .collection::<Document>("datakeys")
        .drop()
        .await?;
    test_client.database("docsExamples").drop().await?;

    // Create two data keys.
    let ce = ClientEncryption::new(
        test_client.clone().into_client(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?;
    let key1_id = ce
        .create_data_key(LocalMasterKey::builder().build())
        .await?;
    let key2_id = ce
        .create_data_key(LocalMasterKey::builder().build())
        .await?;

    // Create an encryptedFieldsMap.
    let encrypted_fields_map = [(
        "docsExamples.encrypted",
        doc! {
            "fields": [
                {
                    "path":     "encryptedIndexed",
                    "bsonType": "string",
                    "keyId":    key1_id,
                    "queries": { "queryType": "equality" },
                },
                {
                    "path":     "encryptedUnindexed",
                    "bsonType": "string",
                    "keyId":    key2_id,
                },
            ]
        },
    )];

    // Create an FLE 2 collection.
    let encrypted_client = Client::encrypted_builder(
        get_client_options().await.clone(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?
    .extra_options(EXTRA_OPTIONS.clone())
    .encrypted_fields_map(encrypted_fields_map)
    .build()
    .await?;
    let db = encrypted_client.database("docsExamples");
    db.create_collection("encrypted").await?;
    let encrypted_coll = db.collection::<Document>("encrypted");

    // Auto encrypt an insert and find.

    // Encrypt an insert.
    encrypted_coll
        .insert_one(doc! {
            "_id":                1,
            "encryptedIndexed":   "indexedValue",
            "encryptedUnindexed": "unindexedValue",
        })
        .await?;

    // Encrypt a find.
    let found = encrypted_coll
        .find_one(doc! {
            "encryptedIndexed": "indexedValue",
        })
        .await?
        .unwrap();
    assert_eq!("indexedValue", found.get_str("encryptedIndexed")?);
    assert_eq!("unindexedValue", found.get_str("encryptedUnindexed")?);

    // Find documents without decryption.
    let unencrypted_coll = test_client
        .database("docsExamples")
        .collection::<Document>("encrypted");
    let found = unencrypted_coll.find_one(doc! { "_id": 1 }).await?.unwrap();
    assert_eq!(
        Some(ElementType::Binary),
        found.get("encryptedIndexed").map(Bson::element_type)
    );
    assert_eq!(
        Some(ElementType::Binary),
        found.get("encryptedUnindexed").map(Bson::element_type)
    );

    Ok(())
}

#[tokio::test]
async fn encrypt_expression_with_options() {
    let key_vault_client = Client::for_test().await.into_client();
    let client_encryption = ClientEncryption::new(
        key_vault_client,
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )
    .unwrap();
    let data_key = client_encryption
        .create_data_key(LocalMasterKey::builder().build())
        .await
        .unwrap();

    let expression = rawdoc! {
        "$and": [
            { "a": { "$gt": 0 } },
            { "a": { "$lt": 10 } },
        ]
    };
    let range_options = RangeOptions::builder()
        .min(Bson::from(0))
        .max(Bson::from(10))
        .build();

    let invalid_encrypt_options = EncryptOptions::builder()
        .contention_factor(0)
        .range_options(range_options.clone())
        .query_type("bad".to_string())
        .build();
    let error = client_encryption
        .encrypt_expression(expression.clone(), data_key.clone())
        .with_options(invalid_encrypt_options)
        .await
        .unwrap_err();
    assert!(matches!(*error.kind, ErrorKind::InvalidArgument { .. }));

    let valid_encrypt_options = EncryptOptions::builder()
        .contention_factor(0)
        .range_options(range_options)
        .build();
    client_encryption
        .encrypt_expression(expression, data_key)
        .with_options(valid_encrypt_options)
        .await
        .unwrap();
}
