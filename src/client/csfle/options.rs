use std::collections::HashMap;

use bson::Array;
use mongocrypt::ctx::KmsProvider;
use typed_builder::TypedBuilder;

use crate::{
    bson::{doc, Bson, Document},
    client::options::TlsOptions,
    error::{Error, Result},
};

/// Options related to automatic encryption.
///
/// Automatic encryption is an enterprise only feature that only applies to operations on a
/// collection. Automatic encryption is not supported for operations on a database or view, and
/// operations that are not bypassed will result in error (see [libmongocrypt: Auto Encryption
/// Allow-List](
/// https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/client-side-encryption.rst#libmongocrypt-auto-encryption-allow-list
/// )). To bypass automatic encryption for all operations, set bypassAutoEncryption=true in
/// AutoEncryptionOpts.
#[derive(Debug, Default, TypedBuilder, Clone)]
#[non_exhaustive]
#[builder(field_defaults(setter(into)))]
pub struct AutoEncryptionOpts {
    /// Used for data key queries.  Will default to an internal client if not set.
    #[builder(default)]
    pub key_vault_client: Option<crate::Client>,
    /// A collection that contains all data keys used for encryption and decryption (aka the key
    /// vault collection).
    pub key_vault_namespace: String,
    /// Options individual to each KMS provider.
    pub kms_providers: KmsProviders,
    /// Specify a JSONSchema locally.
    ///
    /// Supplying a `schema_map` provides more security than relying on JSON Schemas obtained from
    /// the server. It protects against a malicious server advertising a false JSON Schema, which
    /// could trick the client into sending unencrypted data that should be encrypted.
    ///
    /// Schemas supplied in the `schema_map` only apply to configuring automatic encryption for
    /// client side encryption. Other validation rules in the JSON schema will not be enforced by
    /// the driver and will result in an error.
    #[builder(default)]
    pub schema_map: Option<HashMap<String, Document>>,
    /// Disable automatic encryption and do not spawn mongocryptd.  Defaults to false.
    #[builder(default)]
    pub bypass_auto_encryption: Option<bool>,
    /// Options related to mongocryptd.
    #[builder(default)]
    pub extra_options: Option<Document>,
    /// Configure TLS for connections to KMS providers.
    #[builder(default)]
    pub tls_options: Option<KmsProvidersTlsOptions>,
    /// Maps namespace to encrypted fields.
    ///
    /// Supplying an `encrypted_fields_map` provides more security than relying on an
    /// encryptedFields obtained from the server. It protects against a malicious server
    /// advertising a false encryptedFields.
    #[builder(default)]
    pub encrypted_fields_map: Option<HashMap<String, Document>>,
    /// Disable serverside processing of encrypted indexed fields, allowing use of explicit
    /// encryption with queryable encryption.
    #[builder(default)]
    pub bypass_query_analysis: Option<bool>,
}

/// Options specific to each KMS provider.
pub type KmsProviders = HashMap<KmsProvider, Document>;
/// TLS options for connections to each KMS provider.
pub type KmsProvidersTlsOptions = HashMap<KmsProvider, TlsOptions>;

impl AutoEncryptionOpts {
    pub(crate) fn extra_option<'a, Opt: ExtraOption<'a>>(
        &'a self,
        opt: &Opt,
    ) -> Result<Option<Opt::Output>> {
        let key = opt.key();
        match self.extra_options.as_ref().and_then(|o| o.get(key)) {
            None => Ok(None),
            Some(b) => match Opt::as_type(b) {
                Some(v) => Ok(Some(v)),
                None => Err(Error::invalid_argument(format!(
                    "unexpected type for extra option {:?}: {:?}",
                    key, b
                ))),
            },
        }
    }
}

pub(crate) trait ExtraOption<'a> {
    type Output;
    fn key(&self) -> &'static str;
    fn as_type(input: &'a Bson) -> Option<Self::Output>;
}

pub(crate) struct ExtraOptionStr(&'static str);

impl<'a> ExtraOption<'a> for ExtraOptionStr {
    type Output = &'a str;
    fn key(&self) -> &'static str {
        self.0
    }
    fn as_type(input: &'a Bson) -> Option<&'a str> {
        input.as_str()
    }
}

pub(crate) struct ExtraOptionBool(&'static str);

impl<'a> ExtraOption<'a> for ExtraOptionBool {
    type Output = bool;
    fn key(&self) -> &'static str {
        self.0
    }
    fn as_type(input: &'a Bson) -> Option<bool> {
        input.as_bool()
    }
}

pub(crate) struct ExtraOptionArray(&'static str);

impl<'a> ExtraOption<'a> for ExtraOptionArray {
    type Output = &'a Array;
    fn key(&self) -> &'static str {
        self.0
    }
    fn as_type(input: &'a Bson) -> Option<&'a Array> {
        input.as_array()
    }
}

pub(crate) const EO_MONGOCRYPTD_URI: ExtraOptionStr = ExtraOptionStr("mongocryptdURI");
pub(crate) const EO_MONGOCRYPTD_BYPASS_SPAWN: ExtraOptionBool =
    ExtraOptionBool("mongocryptdBypassSpawn");
pub(crate) const EO_MONGOCRYPTD_SPAWN_PATH: ExtraOptionStr = ExtraOptionStr("mongocryptdSpawnPath");
pub(crate) const EO_MONGOCRYPTD_SPAWN_ARGS: ExtraOptionArray =
    ExtraOptionArray("mongocryptdSpawnArgs");
pub(crate) const EO_CRYPT_SHARED_LIB_PATH: ExtraOptionStr = ExtraOptionStr("cryptSharedLibPath");
pub(crate) const EO_CRYPT_SHARED_REQUIRED: ExtraOptionBool = ExtraOptionBool("cryptSharedRequired");
