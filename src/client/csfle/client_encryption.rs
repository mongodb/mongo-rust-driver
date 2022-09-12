//! Support for explicit encryption.

use crate::{
    bson::Binary,
    coll::options::CollectionOptions,
    error::{Error, Result},
    options::{ReadConcern, WriteConcern},
    results::DeleteResult,
    Client,
    Collection,
    Cursor,
    Namespace,
};
use bson::{doc, spec::BinarySubtype, RawBinaryRef, RawDocumentBuf};
use mongocrypt::{
    ctx::{Algorithm, Ctx, KmsProvider},
    Crypt,
};
use serde::Serialize;
use typed_builder::TypedBuilder;

use super::{
    options::{KmsProviders, KmsProvidersTlsOptions},
    state_machine::CryptExecutor,
};

/// A handle to the key vault.  Used to create data encryption keys, and to explicitly encrypt and
/// decrypt values when auto-encryption is not an option.
pub struct ClientEncryption {
    crypt: Crypt,
    exec: CryptExecutor,
    key_vault: Collection<RawDocumentBuf>,
}

impl ClientEncryption {
    /// Create a new key vault handle with the given options.
    pub fn new(options: ClientEncryptionOptions) -> Result<Self> {
        let crypt = Crypt::builder()
            .kms_providers(&bson::to_document(&options.kms_providers)?)?
            .build()?;
        let exec = CryptExecutor::new_explicit(
            options.key_vault_client.weak(),
            options.key_vault_namespace.clone(),
            options.tls_options,
        )?;
        let key_vault = options
            .key_vault_client
            .database(&options.key_vault_namespace.db)
            .collection_with_options(
                &options.key_vault_namespace.coll,
                CollectionOptions::builder()
                    .write_concern(WriteConcern::MAJORITY)
                    .read_concern(ReadConcern::MAJORITY)
                    .build(),
            );
        Ok(Self {
            crypt,
            exec,
            key_vault,
        })
    }

    /// Creates a new key document and inserts into the key vault collection.
    /// Returns the _id of the created document as a UUID (BSON binary subtype 0x04).
    pub async fn create_data_key(
        &self,
        kms_provider: KmsProvider,
        opts: impl Into<Option<DataKeyOptions>>,
    ) -> Result<Binary> {
        let ctx = self.create_data_key_ctx(kms_provider, opts.into())?;
        let data_key = self.exec.run_ctx(ctx, None).await?;
        self.key_vault.insert_one(&data_key, None).await?;
        let bin_ref = data_key
            .get_binary("_id")
            .map_err(|e| Error::internal(format!("invalid data key id: {}", e)))?;
        Ok(bin_ref.to_binary())
    }

    fn create_data_key_ctx(
        &self,
        kms_provider: KmsProvider,
        opts: Option<DataKeyOptions>,
    ) -> Result<Ctx> {
        let mut builder = self.crypt.ctx_builder();
        let mut key_doc = doc! { "provider": kms_provider.name() };
        if let Some(opts) = opts {
            if !matches!(opts.master_key, MasterKey::Local) {
                let master_doc = bson::to_document(&opts.master_key)?;
                key_doc.extend(master_doc);
            }
            if let Some(alt_names) = opts.key_alt_names {
                for name in alt_names {
                    builder = builder.key_alt_name(&name)?;
                }
            }
            if let Some(material) = opts.key_material {
                builder = builder.key_material(&material)?;
            }
        }
        builder = builder.key_encryption_key(&key_doc)?;
        Ok(builder.build_datakey()?)
    }

    // pub async fn rewrap_many_data_key(&self, _filter: Document, _opts: impl
    // Into<Option<RewrapManyDataKeyOptions>>) -> Result<RewrapManyDataKeyResult> {
    // todo!("RUST-1441") }

    /// Removes the key document with the given UUID (BSON binary subtype 0x04) from the key vault
    /// collection. Returns the result of the internal deleteOne() operation on the key vault
    /// collection.
    pub async fn delete_key(&self, id: &Binary) -> Result<DeleteResult> {
        self.key_vault.delete_one(doc! { "_id": id }, None).await
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

    /// Encrypts a BsonValue with a given key and algorithm.
    /// Returns an encrypted value (BSON binary of subtype 6).
    pub async fn encrypt(&self, value: bson::RawBson, opts: EncryptOptions) -> Result<Binary> {
        let ctx = self.encrypt_ctx(value, opts)?;
        let result = self.exec.run_ctx(ctx, None).await?;
        let bin_ref = result
            .get_binary("v")
            .map_err(|e| Error::internal(format!("invalid encryption result: {}", e)))?;
        Ok(bin_ref.to_binary())
    }

    fn encrypt_ctx(&self, value: bson::RawBson, opts: EncryptOptions) -> Result<Ctx> {
        let mut builder = self.crypt.ctx_builder();
        match opts.key {
            EncryptKey::Id(id) => {
                builder = builder.key_id(&id.bytes)?;
            }
            EncryptKey::AltName(name) => {
                builder = builder.key_alt_name(&name)?;
            }
        }
        builder = builder.algorithm(opts.algorithm)?;
        if let Some(factor) = opts.contention_factor {
            builder = builder.contention_factor(factor)?;
        }
        if let Some(qtype) = opts.query_type {
            builder = builder.query_type(&qtype)?;
        }
        Ok(builder.build_explicit_encrypt(value)?)
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

/// Options for initializing a new `ClientEncryption`.
#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct ClientEncryptionOptions {
    /// The key vault `Client`.
    ///
    /// Typically this is the same as the main client; it can be different in order to route data
    /// key queries to a separate MongoDB cluster, or the same cluster but with a different
    /// credential.
    pub key_vault_client: Client,
    /// The key vault `Namespace`, referring to a collection that contains all data keys used for
    /// encryption and decryption.
    pub key_vault_namespace: Namespace,
    /// Map of KMS provider properties; multiple KMS providers may be specified.
    pub kms_providers: KmsProviders,
    /// TLS options for connecting to KMS providers.  If not given, default options will be used.
    #[builder(default)]
    pub tls_options: Option<KmsProvidersTlsOptions>,
}

/// Options for creating a data key.
#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct DataKeyOptions {
    /// The master key document, a KMS-specific key used to encrypt the new data key.
    pub master_key: MasterKey,
    /// An optional list of alternate names used to reference a key.
    #[builder(default)]
    pub key_alt_names: Option<Vec<String>>,
    /// An optional buffer of 96 bytes to use as custom key material for the data key being
    /// created.  If unset, key material for the new data key is generated from a cryptographically
    /// secure random device.
    #[builder(default)]
    pub key_material: Option<Vec<u8>>,
}

/// A KMS-specific key used to encrypt data keys.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase", untagged)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum MasterKey {
    Aws {
        region: String,
        /// The Amazon Resource Name (ARN) to the AWS customer master key (CMK).
        key: String,
        /// An alternate host identifier to send KMS requests to. May include port number. Defaults
        /// to "kms.<region>.amazonaws.com"
        endpoint: Option<String>,
    },
    Azure {
        /// Host with optional port. Example: "example.vault.azure.net".
        key_vault_endpoint: String,
        key_name: String,
        /// A specific version of the named key, defaults to using the key's primary version.
        key_version: Option<String>,
    },
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
    Kmip {
        /// keyId is the KMIP Unique Identifier to a 96 byte KMIP Secret Data managed object.  If
        /// keyId is omitted, the driver creates a random 96 byte KMIP Secret Data managed object.
        key_id: Option<String>,
        /// Host with optional port.
        endpoint: Option<String>,
    },
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

/// The options for explicit encryption.
#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct EncryptOptions {
    /// The key to use.
    pub key: EncryptKey,
    /// The encryption algorithm.
    ///
    /// To insert or query with an "Indexed" encrypted payload, use a `Client` configured with
    /// `AutoEncryptionOptions`. `AutoEncryptionOptions.bypass_query_analysis may be true.
    /// `AutoEncryptionOptions.bypass_auto_encryption` must be false.
    pub algorithm: Algorithm,
    /// The contention factor.
    #[builder(default)]
    pub contention_factor: Option<i64>,
    /// The query type.
    #[builder(default)]
    pub query_type: Option<String>,
}

/// An encryption key reference.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EncryptKey {
    /// Find the key by _id value.
    Id(Binary),
    /// Find the key by alternate name.
    AltName(String),
}
