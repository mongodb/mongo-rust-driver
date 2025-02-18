#[cfg(feature = "openssl-tls")]
#[path = "csfle/kmip.rs"]
mod kmip_skip_local;
mod prose;

use std::{env, path::PathBuf};

use anyhow::Context;
use bson::{doc, Document, RawBson};
use mongocrypt::ctx::{Algorithm, KmsProvider, KmsProviderType};
use once_cell::sync::Lazy;

use crate::{
    client_encryption::{ClientEncryption, EncryptKey},
    options::{CollectionOptions, ReadConcern, TlsOptions, WriteConcern},
    Client,
    Collection,
    Namespace,
};

use super::{log_uncaptured, EventClient};

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

// Additional environment variables. These values should be set to the relevant local paths/ports.
static CSFLE_TLS_CERT_DIR: Lazy<String> = Lazy::new(|| get_env_var("CSFLE_TLS_CERT_DIR"));
static CRYPT_SHARED_LIB_PATH: Lazy<String> = Lazy::new(|| get_env_var("CRYPT_SHARED_LIB_PATH"));
static AZURE_IMDS_MOCK_PORT: Lazy<u16> = Lazy::new(|| {
    get_env_var("AZURE_IMDS_MOCK_PORT")
        .parse()
        .expect("AZURE_IMDS_MOCK_PORT")
});

fn get_env_var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!(
            "Missing environment variable for {}. See src/test/csfle.rs for the list of required \
             variables and instructions for retrieving them.",
            name
        )
    })
}

pub(crate) static AWS_KMS: Lazy<KmsInfo> = Lazy::new(|| {
    (
        KmsProvider::aws(),
        doc! {
            "accessKeyId": &*FLE_AWS_KEY,
            "secretAccessKey": &*FLE_AWS_SECRET
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
            "accessKeyId": &*FLE_AWS_KEY,
            "secretAccessKey": &*FLE_AWS_SECRET
        },
        None,
    )
});
pub(crate) static AZURE_KMS: Lazy<KmsInfo> = Lazy::new(|| {
    (
        KmsProvider::azure(),
        doc! {
            "tenantId": &*FLE_AZURE_TENANTID,
            "clientId": &*FLE_AZURE_CLIENTID,
            "clientSecret": &*FLE_AZURE_CLIENTSECRET,
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
            "email": &*FLE_GCP_EMAIL,
            "privateKey": &*FLE_GCP_PRIVATEKEY,
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
    Lazy::new(|| doc! { "cryptSharedLibPath": &*CRYPT_SHARED_LIB_PATH });
static KV_NAMESPACE: Lazy<Namespace> =
    Lazy::new(|| Namespace::from_str("keyvault.datakeys").unwrap());
static DISABLE_CRYPT_SHARED: Lazy<bool> =
    Lazy::new(|| env::var("DISABLE_CRYPT_SHARED").is_ok_and(|s| s == "true"));

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

macro_rules! failure {
    ($($arg:tt)*) => {{
        crate::error::Error::internal(format!($($arg)*)).into()
    }}
}
use failure;

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
