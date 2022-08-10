#![allow(dead_code)]

use crate::coll::options::CollectionOptions;
use crate::options::{WriteConcern, ReadConcern};
use crate::results::DeleteResult;
use crate::{Cursor, Client, Namespace, Collection};
use crate::bson::{Binary, Document};
use crate::error::{Result, Error};
use bson::{doc, RawDocumentBuf};
use mongocrypt::Crypt;
use mongocrypt::ctx::{KmsProvider, Ctx};

use super::options::{KmsProviders, KmsProvidersTlsOptions};
use super::state_machine::CryptExecutor;

pub struct ClientEncryption {
    crypt: Crypt,
    exec: CryptExecutor,
    opts: ClientEncryptionOptions,
    key_vault: Collection<RawDocumentBuf>,
}

impl ClientEncryption {
    pub fn new(opts: ClientEncryptionOptions) -> Result<Self> {
        let crypt = Crypt::builder().build()?;
        let exec = CryptExecutor::new(
            opts.key_vault_client.weak(),
            opts.key_vault_namespace.clone(),
            None,
            None,
        )?;
        let key_vault = opts.key_vault_client
            .database(&opts.key_vault_namespace.db)
            .collection_with_options(
                &opts.key_vault_namespace.coll, 
                CollectionOptions::builder()
                    .write_concern(WriteConcern::MAJORITY)
                    .read_concern(ReadConcern::MAJORITY)
                    .build(),
            );
        Ok(Self { crypt, exec, opts, key_vault })
    }

    pub async fn create_data_key(&self, kms_provider: KmsProvider, opts: DataKeyOptions) -> Result<Binary> {
        let ctx = self.create_data_key_ctx(kms_provider, opts)?;
        let data_key = self.exec.run_ctx(ctx, None).await?;
        self.key_vault.insert_one(&data_key, None).await?;
        let bin_ref = data_key.get_binary("_id")
            .map_err(|e| Error::internal(format!("invalid data key id: {}", e)))?;
        // TODO(aegnor): impl ToOwned
        Ok(Binary {
            subtype: bin_ref.subtype,
            bytes: bin_ref.bytes.to_owned(),
        })
    }

    fn create_data_key_ctx(&self, kms_provider: KmsProvider, opts: DataKeyOptions) -> Result<Ctx> {
        let mut builder = self.crypt.ctx_builder();
        // TODO(aegnor): move this to KmsProvider
        let kms_provider = match kms_provider {
            KmsProvider::Aws => "aws",
            KmsProvider::Azure => "azure",
            KmsProvider::Gcp => "gcp",
            KmsProvider::Kmip => "kmip",
            KmsProvider::Other(s) => s,
        };
        let mut key_doc = doc! { "provider": kms_provider };
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

    pub fn rewrap_many_data_key(&self, _filter: Document, _opts: RewrapManyDataKeyOptions) -> Result<RewrapManyDataKeyResult> {
        todo!()
    }

    pub async fn delete_key(&self, id: &Binary) -> Result<DeleteResult> {
        self.key_vault.delete_one(doc! { "_id": id }, None).await
    }

    pub async fn get_key(&self, id: &Binary) -> Result<Option<RawDocumentBuf>> {
        self.key_vault.find_one(doc! { "_id": id }, None).await
    }

    pub async fn get_keys(&self) -> Result<Cursor<RawDocumentBuf>> {
        self.key_vault.find(doc! { }, None).await
    }

    pub async fn add_key_alt_name(&self, id: &Binary, key_alt_name: &str) -> Result<Option<RawDocumentBuf>> {
        self.key_vault.find_one_and_update(doc! { "_id": id }, doc! { "$addToSet": { "keyAltNames": key_alt_name } }, None).await
    }

    pub async fn remove_key_alt_name(&self, id: &Binary, key_alt_name: &str) -> Result<Option<RawDocumentBuf>> {
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
        self.key_vault.find_one_and_update(doc! { "_id": id }, vec![update], None).await
    }

    pub async fn get_key_by_alt_name(&self, key_alt_name: &str) -> Result<Option<RawDocumentBuf>> {
        self.key_vault.find_one(doc! { "keyAltNames": key_alt_name }, None).await
    }

    pub fn encrypt(&self, value: bson::Bson, opts: EncryptOptions) -> Result<Binary> {
        todo!()
    }

    pub fn decrypt(&self, value: Binary) -> Result<bson::Bson> {
        todo!()
    }
}

#[non_exhaustive]
pub struct ClientEncryptionOptions {
    pub key_vault_client: Client,
    pub key_vault_namespace: Namespace,
    pub kms_providers: KmsProviders,
    pub tls_options: Option<KmsProvidersTlsOptions>,
}

#[non_exhaustive]
pub struct DataKeyOptions {
    pub master_key: Option<Document>,
    pub key_alt_names: Option<Vec<String>>,
    pub key_material: Option<Vec<u8>>,  // TODO(aegnor): BinData?
}

#[non_exhaustive]
pub struct RewrapManyDataKeyOptions {
    pub provider: KmsProvider,  // TODO(aegnor): String?
    pub master_key: Option<Document>,
}

#[non_exhaustive]
pub struct RewrapManyDataKeyResult {
    //pub bulk_write_result: Option<BulkWriteResult>  // No bulk write support!
}

#[non_exhaustive]
pub struct EncryptOptions {
    pub key: EncryptKey,
    pub algorithm: String,
    pub contention_factor: Option<i64>,
    pub query_type: Option<String>,
}

pub enum EncryptKey {
    Id(Binary),
    AltName(String),
}