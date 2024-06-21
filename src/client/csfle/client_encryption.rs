//! Support for explicit encryption.

mod create_data_key;
mod encrypt;

use mongocrypt::{ctx::KmsProvider, Crypt};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{
    bson::{doc, spec::BinarySubtype, Binary, RawBinaryRef, RawDocumentBuf},
    client::options::TlsOptions,
    coll::options::CollectionOptions,
    error::{Error, Result},
    options::{ReadConcern, WriteConcern},
    results::DeleteResult,
    Client,
    Collection,
    Cursor,
    Namespace,
};

use super::{options::KmsProviders, state_machine::CryptExecutor};

pub use crate::action::csfle::encrypt::{EncryptKey, RangeOptions};

/// A handle to the key vault.  Used to create data encryption keys, and to explicitly encrypt and
/// decrypt values when auto-encryption is not an option.
pub struct ClientEncryption {
    crypt: Crypt,
    exec: CryptExecutor,
    key_vault: Collection<RawDocumentBuf>,
}

impl ClientEncryption {
    /// Initialize a new `ClientEncryption`.
    ///
    /// ```no_run
    /// # use bson::doc;
    /// # use mongocrypt::ctx::KmsProvider;
    /// # use mongodb::client_encryption::ClientEncryption;
    /// # use mongodb::error::Result;
    /// # fn func() -> Result<()> {
    /// # let kv_client = todo!();
    /// # let kv_namespace = todo!();
    /// # let local_key = doc! { };
    /// let enc = ClientEncryption::new(
    ///     kv_client,
    ///     kv_namespace,
    ///     [
    ///         (KmsProvider::Local, doc! { "key": local_key }, None),
    ///         (KmsProvider::Kmip, doc! { "endpoint": "localhost:5698" }, None),
    ///     ]
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(
        key_vault_client: Client,
        key_vault_namespace: Namespace,
        kms_providers: impl IntoIterator<Item = (KmsProvider, bson::Document, Option<TlsOptions>)>,
    ) -> Result<Self> {
        let kms_providers = KmsProviders::new(kms_providers)?;
        let crypt = Crypt::builder()
            .kms_providers(&kms_providers.credentials_doc()?)?
            .use_need_kms_credentials_state()
            .use_range_v2()?
            .build()?;
        let exec = CryptExecutor::new_explicit(
            key_vault_client.weak(),
            key_vault_namespace.clone(),
            kms_providers,
        )?;
        let key_vault = key_vault_client
            .database(&key_vault_namespace.db)
            .collection_with_options(
                &key_vault_namespace.coll,
                CollectionOptions::builder()
                    .write_concern(WriteConcern::majority())
                    .read_concern(ReadConcern::majority())
                    .build(),
            );
        Ok(ClientEncryption {
            crypt,
            exec,
            key_vault,
        })
    }

    // pub async fn rewrap_many_data_key(&self, _filter: Document, _opts: impl
    // Into<Option<RewrapManyDataKeyOptions>>) -> Result<RewrapManyDataKeyResult> {
    // todo!("RUST-1441") }

    /// Removes the key document with the given UUID (BSON binary subtype 0x04) from the key vault
    /// collection. Returns the result of the internal deleteOne() operation on the key vault
    /// collection.
    pub async fn delete_key(&self, id: &Binary) -> Result<DeleteResult> {
        self.key_vault.delete_one(doc! { "_id": id }).await
    }

    /// Finds a single key document with the given UUID (BSON binary subtype 0x04).
    /// Returns the result of the internal find() operation on the key vault collection.
    pub async fn get_key(&self, id: &Binary) -> Result<Option<RawDocumentBuf>> {
        self.key_vault.find_one(doc! { "_id": id }).await
    }

    /// Finds all documents in the key vault collection.
    /// Returns the result of the internal find() operation on the key vault collection.
    pub async fn get_keys(&self) -> Result<Cursor<RawDocumentBuf>> {
        self.key_vault.find(doc! {}).await
    }

    /// Adds a keyAltName to the keyAltNames array of the key document in the key vault collection
    /// with the given UUID (BSON binary subtype 0x04). Returns the previous version of the key
    /// document.
    pub async fn add_key_alt_name(
        &self,
        id: &Binary,
        key_alt_name: &str,
    ) -> Result<Option<RawDocumentBuf>> {
        self.key_vault
            .find_one_and_update(
                doc! { "_id": id },
                doc! { "$addToSet": { "keyAltNames": key_alt_name } },
            )
            .await
    }

    /// Removes a keyAltName from the keyAltNames array of the key document in the key vault
    /// collection with the given UUID (BSON binary subtype 0x04). Returns the previous version
    /// of the key document.
    pub async fn remove_key_alt_name(
        &self,
        id: &Binary,
        key_alt_name: &str,
    ) -> Result<Option<RawDocumentBuf>> {
        let update = doc! {
            "$set": {
                "keyAltNames": {
                    "$cond": [
                        { "$eq": ["$keyAltNames", [key_alt_name]] },
                        "$$REMOVE",
                        {
                            "$filter": {
                                "input": "$keyAltNames",
                                "cond": { "$ne": ["$$this", key_alt_name] },
                            }
                        }
                    ]
                }
            }
        };
        self.key_vault
            .find_one_and_update(doc! { "_id": id }, vec![update])
            .await
    }

    /// Returns a key document in the key vault collection with the given keyAltName.
    pub async fn get_key_by_alt_name(
        &self,
        key_alt_name: impl AsRef<str>,
    ) -> Result<Option<RawDocumentBuf>> {
        self.key_vault
            .find_one(doc! { "keyAltNames": key_alt_name.as_ref() })
            .await
    }

    /// Decrypts an encrypted value (BSON binary of subtype 6).
    /// Returns the original BSON value.
    pub async fn decrypt<'a>(&self, value: RawBinaryRef<'a>) -> Result<bson::RawBson> {
        if value.subtype != BinarySubtype::Encrypted {
            return Err(Error::invalid_argument(format!(
                "Invalid binary subtype for decrypt: expected {:?}, got {:?}",
                BinarySubtype::Encrypted,
                value.subtype
            )));
        }
        let ctx = self
            .crypt
            .ctx_builder()
            .build_explicit_decrypt(value.bytes)?;
        let result = self.exec.run_ctx(ctx, None).await?;
        Ok(result
            .get("v")?
            .ok_or_else(|| Error::internal("invalid decryption result"))?
            .to_raw_bson())
    }
}

/// A KMS-specific key used to encrypt data keys.
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum MasterKey {
    Aws(AwsMasterKey),
    Azure(AzureMasterKey),
    Gcp(GcpMasterKey),
    Kmip(KmipMasterKey),
    Local(LocalMasterKey),
}

/// An AWS master key.
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AwsMasterKey {
    /// The name for the key. The value for this field must be the same as the corresponding
    /// [`KmsProvider`](mongocrypt::ctx::KmsProvider)'s name.
    #[serde(skip)]
    pub name: Option<String>,

    /// The region.
    pub region: String,

    /// The Amazon Resource Name (ARN) to the AWS customer master key (CMK).
    pub key: String,

    /// An alternate host identifier to send KMS requests to. May include port number. Defaults to
    /// "kms.<region>.amazonaws.com".
    pub endpoint: Option<String>,
}

impl From<AwsMasterKey> for MasterKey {
    fn from(aws_master_key: AwsMasterKey) -> Self {
        Self::Aws(aws_master_key)
    }
}

/// An Azure master key.
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AzureMasterKey {
    /// The name for the key. The value for this field must be the same as the corresponding
    /// [`KmsProvider`](mongocrypt::ctx::KmsProvider)'s name.
    #[serde(skip)]
    pub name: Option<String>,

    /// Host with optional port. Example: "example.vault.azure.net".
    pub key_vault_endpoint: String,

    /// The key name.
    pub key_name: String,

    /// A specific version of the named key, defaults to using the key's primary version.
    pub key_version: Option<String>,
}

impl From<AzureMasterKey> for MasterKey {
    fn from(azure_master_key: AzureMasterKey) -> Self {
        Self::Azure(azure_master_key)
    }
}

/// A GCP master key.
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct GcpMasterKey {
    /// The name for the key. The value for this field must be the same as the corresponding
    /// [`KmsProvider`](mongocrypt::ctx::KmsProvider)'s name.
    #[serde(skip)]
    pub name: Option<String>,

    /// The project ID.
    pub project_id: String,

    /// The location.
    pub location: String,

    /// The key ring.
    pub key_ring: String,

    /// The key name.
    pub key_name: String,

    /// A specific version of the named key. Defaults to using the key's primary version.
    pub key_version: Option<String>,

    /// Host with optional port. Defaults to "cloudkms.googleapis.com".
    pub endpoint: Option<String>,
}

impl From<GcpMasterKey> for MasterKey {
    fn from(gcp_master_key: GcpMasterKey) -> Self {
        Self::Gcp(gcp_master_key)
    }
}

/// A local master key.
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct LocalMasterKey {
    /// The name for the key. The value for this field must be the same as the corresponding
    /// [`KmsProvider`](mongocrypt::ctx::KmsProvider)'s name.
    #[serde(skip)]
    pub name: Option<String>,
}

impl From<LocalMasterKey> for MasterKey {
    fn from(local_master_key: LocalMasterKey) -> Self {
        Self::Local(local_master_key)
    }
}

/// A KMIP master key.
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct KmipMasterKey {
    /// The name for the key. The value for this field must be the same as the corresponding
    /// [`KmsProvider`](mongocrypt::ctx::KmsProvider)'s name.
    #[serde(skip)]
    pub name: Option<String>,

    /// The KMIP Unique Identifier to a 96 byte KMIP Secret Data managed object. If this field is
    /// not specified, the driver creates a random 96 byte KMIP Secret Data managed object.
    pub key_id: Option<String>,

    /// If true (recommended), the KMIP server must decrypt this key. Defaults to false.
    pub delegated: Option<bool>,

    /// Host with optional port.
    pub endpoint: Option<String>,
}

impl From<KmipMasterKey> for MasterKey {
    fn from(kmip_master_key: KmipMasterKey) -> Self {
        Self::Kmip(kmip_master_key)
    }
}

impl MasterKey {
    /// Returns the `KmsProvider` associated with this key.
    pub fn provider(&self) -> KmsProvider {
        let (provider, name) = match self {
            MasterKey::Aws(AwsMasterKey { name, .. }) => (KmsProvider::aws(), name.clone()),
            MasterKey::Azure(AzureMasterKey { name, .. }) => (KmsProvider::azure(), name.clone()),
            MasterKey::Gcp(GcpMasterKey { name, .. }) => (KmsProvider::gcp(), name.clone()),
            MasterKey::Kmip(KmipMasterKey { name, .. }) => (KmsProvider::kmip(), name.clone()),
            MasterKey::Local(LocalMasterKey { name, .. }) => (KmsProvider::local(), name.clone()),
        };
        if let Some(name) = name {
            provider.with_name(name)
        } else {
            provider
        }
    }
}

// #[non_exhaustive]
// pub struct RewrapManyDataKeyOptions {
// pub provider: KmsProvider,
// pub master_key: Option<Document>,
// }
//
//
// #[non_exhaustive]
// pub struct RewrapManyDataKeyResult {
// pub bulk_write_result: Option<BulkWriteResult>,
// }
