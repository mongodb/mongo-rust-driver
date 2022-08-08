#![allow(dead_code)]

use crate::results::DeleteResult;
use crate::{Cursor, Client, Namespace};
use crate::bson::{Binary, Document};
use crate::error::Result;
use mongocrypt::Crypt;
use mongocrypt::ctx::KmsProvider;

use super::options::{KmsProviders, KmsProvidersTlsOptions};
use super::state_machine::CryptExecutor;

pub struct ClientEncryption {
    crypt: Crypt,
    exec: CryptExecutor,
    opts: ClientEncryptionOptions,
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
        Ok(Self { crypt, exec, opts })
    }

    pub fn create_data_key(&self, _kms_provider: KmsProvider, _opts: DataKeyOptions) -> Result<Binary> {
        todo!()
    }

    pub fn rewrap_many_data_key(&self, _filter: Document, _opts: RewrapManyDataKeyOptions) -> Result<RewrapManyDataKeyResult> {
        todo!()
    }

    pub fn delete_key(&self, _id: &Binary) -> Result<DeleteResult> {
        todo!()
    }

    pub fn get_key(&self, _id: &Binary) -> Result<Option<Document>> {
        todo!()
    }

    pub fn get_keys(&self) -> Result<Cursor<Document>> {
        todo!()
    }

    pub fn add_key_alt_name(&self, _id: &Binary, key_alt_name: &str) -> Result<Option<Document>> {
        todo!()
    }

    pub fn remove_key_alt_name(&self, _id: &Binary, key_alt_name: &str) -> Result<Option<Document>> {
        todo!()
    }

    pub fn get_key_by_alt_name(&self, key_alt_name: &str) -> Result<Option<Document>> {
        todo!()
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
    pub key_material: Option<Vec<u8>>,  // TODO: BinData?
}

#[non_exhaustive]
pub struct RewrapManyDataKeyOptions {
    pub provider: KmsProvider,  // TODO: String?
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