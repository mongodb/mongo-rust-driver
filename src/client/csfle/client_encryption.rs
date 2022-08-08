use crate::Cursor;
use crate::bson::{Binary, Document};
use crate::error::Result;
use mongocrypt::ctx::KmsProvider;

pub struct ClientEncryption {
    _todo: (),
}

impl ClientEncryption {
    pub fn new(_opts: ClientEncryptionOptions) -> Self {
        Self { _todo: () }
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

pub struct ClientEncryptionOptions {
    _todo: (),
}

pub struct DataKeyOptions {
    _todo: (),
}

pub struct RewrapManyDataKeyOptions {
    _todo: (),
}

pub struct RewrapManyDataKeyResult {
    _todo: (),
}

pub struct DeleteResult {
    _todo: (),
}

pub struct EncryptOptions {
    _todo: (),
}