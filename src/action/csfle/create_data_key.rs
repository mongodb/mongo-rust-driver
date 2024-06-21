use crate::client_encryption::{ClientEncryption, MasterKey};

use super::super::{deeplink, option_setters};

impl ClientEncryption {
    /// Creates a new key document and inserts into the key vault collection.
    ///
    /// `await` will return d[`Result<Binary>`] (subtype 0x04) with the _id of the created
    /// document as a UUID.
    #[deeplink]
    pub fn create_data_key(&self, master_key: impl Into<MasterKey>) -> CreateDataKey {
        CreateDataKey {
            client_enc: self,
            master_key: master_key.into(),
            options: None,
            #[cfg(test)]
            test_kms_provider: None,
        }
    }
}

/// Create a new key document and insert it into the key vault collection.  Construct via
/// [`ClientEncryption::create_data_key`].
#[must_use]
pub struct CreateDataKey<'a> {
    pub(crate) client_enc: &'a ClientEncryption,
    pub(crate) master_key: MasterKey,
    pub(crate) options: Option<DataKeyOptions>,
    #[cfg(test)]
    pub(crate) test_kms_provider: Option<mongocrypt::ctx::KmsProvider>,
}

/// Options for creating a data key.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct DataKeyOptions {
    /// An optional list of alternate names that can be used to reference the key.
    pub key_alt_names: Option<Vec<String>>,
    /// A buffer of 96 bytes to use as custom key material for the data key being
    /// created.  If unset, key material for the new data key is generated from a cryptographically
    /// secure random device.
    pub key_material: Option<Vec<u8>>,
}

impl<'a> CreateDataKey<'a> {
    option_setters! { options: DataKeyOptions;
        key_alt_names: Vec<String>,
        key_material: Vec<u8>,
    }

    #[cfg(test)]
    pub(crate) fn test_kms_provider(mut self, value: mongocrypt::ctx::KmsProvider) -> Self {
        self.test_kms_provider = Some(value);
        self
    }
}

// Action impl in src/client/csfle/client_encryption/create_data_key.rs
