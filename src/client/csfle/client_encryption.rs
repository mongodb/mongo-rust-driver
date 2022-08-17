//! Support for explicit encryption.

use crate::{
    bson::{Binary, Document},
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
    pub fn new(opts: ClientEncryptionOptions) -> Result<Self> {
        let crypt = Crypt::builder().build()?;
        let exec = CryptExecutor::new(
            opts.key_vault_client.weak(),
            opts.key_vault_namespace.clone(),
            None,
            None,
            opts.tls_options,
        )?;
        let key_vault = opts
            .key_vault_client
            .database(&opts.key_vault_namespace.db)
            .collection_with_options(
                &opts.key_vault_namespace.coll,
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
        opts: DataKeyOptions,
    ) -> Result<Binary> {
        let ctx = self.create_data_key_ctx(kms_provider, opts)?;
        let data_key = self.exec.run_ctx(ctx, None).await?;
        self.key_vault.insert_one(&data_key, None).await?;
        let bin_ref = data_key
            .get_binary("_id")
            .map_err(|e| Error::internal(format!("invalid data key id: {}", e)))?;
        Ok(bin_owned(bin_ref))
    }

    fn create_data_key_ctx(&self, kms_provider: KmsProvider, opts: DataKeyOptions) -> Result<Ctx> {
        let mut builder = self.crypt.ctx_builder();
        let mut key_doc = doc! { "provider": kms_provider.name() };
        if let Some(master_key) = opts.master_key {
            key_doc.extend(master_key);
        }
        builder = builder.key_encryption_key(&key_doc)?;
        if let Some(alt_names) = opts.key_alt_names {
            for name in alt_names {
                builder = builder.key_alt_name(&name)?;
            }
        }
        if let Some(material) = opts.key_material {
            builder = builder.key_material(&material)?;
        }
        Ok(builder.build_datakey()?)
    }

    // pub async fn rewrap_many_data_key(&self, _filter: Document, _opts: RewrapManyDataKeyOptions)
    // -> Result<RewrapManyDataKeyResult> { todo!("RUST-1441")
    // }

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
    pub async fn get_key_by_alt_name(&self, key_alt_name: &str) -> Result<Option<RawDocumentBuf>> {
        self.key_vault
            .find_one(doc! { "keyAltNames": key_alt_name }, None)
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
        Ok(bin_owned(bin_ref))
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
    pub tls_options: Option<KmsProvidersTlsOptions>,
}

/// Options for creating a data key.
#[non_exhaustive]
pub struct DataKeyOptions {
    /// The master key document, a KMS-specific key used to encrypt the new data key.
    pub master_key: Option<Document>,
    /// An optional list of alternate names used to reference a key.
    pub key_alt_names: Option<Vec<String>>,
    /// An optional buffer of 96 bytes to use as custom key material for the data key being
    /// created.  If unset, key material for the new data key is generated from a cryptographically
    /// secure random device.
    pub key_material: Option<Vec<u8>>,
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
#[non_exhaustive]
pub struct EncryptOptions {
    /// The key to use.
    pub key: EncryptKey,
    /// The encryption algorithm.
    pub algorithm: Algorithm,
    /// The contention factor.
    pub contention_factor: Option<i64>,
    /// The query type.
    pub query_type: Option<String>,
}

/// An encryption key reference.
pub enum EncryptKey {
    /// Find the key by _id value.
    Id(Binary),
    /// Find the key by alternate name.
    AltName(String),
}

// TODO: this can be `bin_ref.to_binary()` once PR#370 is merged.
fn bin_owned(bin_ref: RawBinaryRef) -> Binary {
    Binary {
        subtype: bin_ref.subtype,
        bytes: bin_ref.bytes.to_owned(),
    }
}
