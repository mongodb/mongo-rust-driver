//! Support for explicit encryption.

mod create_data_key;
mod encrypt;

use mongocrypt::{ctx::KmsProvider, Crypt};
use serde::{Deserialize, Serialize};

use crate::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document, RawBinaryRef, RawDocumentBuf},
    client::options::TlsOptions,
    coll::options::CollectionOptions,
    db::options::CreateCollectionOptions,
    error::{Error, Result},
    options::{ReadConcern, WriteConcern},
    results::DeleteResult,
    Client,
    Collection,
    Cursor,
    Database,
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
        self.key_vault.find_one(doc! { "_id": id }, None).await
    }

    /// Finds all documents in the key vault collection.
    /// Returns the result of the internal find() operation on the key vault collection.
    pub async fn get_keys(&self) -> Result<Cursor<RawDocumentBuf>> {
        self.key_vault.find(doc! {}, None).await
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
                None,
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
            .find_one_and_update(doc! { "_id": id }, vec![update], None)
            .await
    }

    /// Returns a key document in the key vault collection with the given keyAltName.
    pub async fn get_key_by_alt_name(
        &self,
        key_alt_name: impl AsRef<str>,
    ) -> Result<Option<RawDocumentBuf>> {
        self.key_vault
            .find_one(doc! { "keyAltNames": key_alt_name.as_ref() }, None)
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

    /// Creates a new collection with encrypted fields, automatically creating new data encryption
    /// keys when needed based on the configured [`CreateCollectionOptions::encrypted_fields`].
    ///
    /// Returns the potentially updated `encrypted_fields` along with status, as keys may have been
    /// created even when a failure occurs.
    ///
    /// Does not affect any auto encryption settings on existing MongoClients that are already
    /// configured with auto encryption.
    pub async fn create_encrypted_collection(
        &self,
        db: &Database,
        name: impl AsRef<str>,
        master_key: MasterKey,
        options: CreateCollectionOptions,
    ) -> (Document, Result<()>) {
        let ef = match options.encrypted_fields.as_ref() {
            Some(ef) => ef,
            None => {
                return (
                    doc! {},
                    Err(Error::invalid_argument(
                        "no encrypted_fields defined for collection",
                    )),
                );
            }
        };
        let mut ef_prime = ef.clone();
        if let Ok(fields) = ef_prime.get_array_mut("fields") {
            for f in fields {
                let f_doc = if let Some(d) = f.as_document_mut() {
                    d
                } else {
                    continue;
                };
                if f_doc.get("keyId") == Some(&Bson::Null) {
                    let d = match self.create_data_key(master_key.clone()).await {
                        Ok(v) => v,
                        Err(e) => return (ef_prime, Err(e)),
                    };
                    f_doc.insert("keyId", d);
                }
            }
        }
        let mut opts_prime = options.clone();
        opts_prime.encrypted_fields = Some(ef_prime.clone());
        (
            ef_prime,
            db.create_collection(name.as_ref())
                .with_options(opts_prime)
                .await,
        )
    }
}

/// A KMS-specific key used to encrypt data keys.
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum MasterKey {
    #[serde(rename_all = "camelCase")]
    Aws {
        region: String,
        /// The Amazon Resource Name (ARN) to the AWS customer master key (CMK).
        key: String,
        /// An alternate host identifier to send KMS requests to. May include port number. Defaults
        /// to "kms.REGION.amazonaws.com"
        endpoint: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Azure {
        /// Host with optional port. Example: "example.vault.azure.net".
        key_vault_endpoint: String,
        key_name: String,
        /// A specific version of the named key, defaults to using the key's primary version.
        key_version: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Gcp {
        project_id: String,
        location: String,
        key_ring: String,
        key_name: String,
        /// A specific version of the named key, defaults to using the key's primary version.
        key_version: Option<String>,
        /// Host with optional port. Defaults to "cloudkms.googleapis.com".
        endpoint: Option<String>,
    },
    /// Master keys are not applicable to `KmsProvider::Local`.
    Local,
    #[serde(rename_all = "camelCase")]
    Kmip {
        /// keyId is the KMIP Unique Identifier to a 96 byte KMIP Secret Data managed object.  If
        /// keyId is omitted, the driver creates a random 96 byte KMIP Secret Data managed object.
        key_id: Option<String>,
        /// Host with optional port.
        endpoint: Option<String>,
    },
}

impl MasterKey {
    /// Returns the `KmsProvider` associated with this key.
    pub fn provider(&self) -> KmsProvider {
        match self {
            MasterKey::Aws { .. } => KmsProvider::Aws,
            MasterKey::Azure { .. } => KmsProvider::Azure,
            MasterKey::Gcp { .. } => KmsProvider::Gcp,
            MasterKey::Kmip { .. } => KmsProvider::Kmip,
            MasterKey::Local => KmsProvider::Local,
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
