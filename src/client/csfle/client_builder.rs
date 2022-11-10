use std::collections::HashMap;

use mongocrypt::ctx::KmsProvider;

use crate::{
    bson::Document,
    client::options::TlsOptions,
    client_encryption::{BUILDER_SET, BUILDER_UNSET},
    error::{Error, Result},
    options::ClientOptions,
    Client,
    Namespace,
};

use super::options::KmsProviders;

/// A builder for constructing a `Client` with auto-encryption enabled.  At least one KMS provider
/// will need to be set to complete the build.
///
/// ```no_run
/// # use bson::doc;
/// # use mongocrypt::ctx::KmsProvider;
/// # use mongodb::Client;
/// # use mongodb::error::Result;
/// # async fn func() -> Result<()> {
/// # let client_options = todo!();
/// # let key_vault_namespace = todo!();
/// # let key_vault_client: Client = todo!();
/// # let local_key: bson::Binary = todo!();
/// let encrypted_client = Client::encrypted_builder(client_options)
///     .key_vault_namespace(key_vault_namespace)
///     .kms_providers([(KmsProvider::Local, doc! { "key": local_key }, None)])?
///     .key_vault_client(key_vault_client)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct EncryptedClientBuilder<const KV_NAMESPACE: bool, const KMS_PROVIDERS: bool> {
    client_options: ClientOptions,
    key_vault_namespace: Option<Namespace>,
    kms_providers: Option<KmsProviders>,
    optional: Optional,
}

#[derive(Default)]
struct Optional {
    key_vault_client: Option<crate::Client>,
    schema_map: Option<HashMap<String, Document>>,
    bypass_auto_encryption: Option<bool>,
    extra_options: Option<Document>,
    encrypted_fields_map: Option<HashMap<String, Document>>,
    bypass_query_analysis: Option<bool>,
    #[cfg(test)]
    disable_crypt_shared: Option<bool>,    
}

impl EncryptedClientBuilder<BUILDER_UNSET, BUILDER_UNSET> {
    pub(crate) fn new(client_options: ClientOptions) -> Self {
        EncryptedClientBuilder {
            client_options,
            key_vault_namespace: None,
            kms_providers: None,
            optional: Optional::default(),
        }
    }
}

impl<const KMS_PROVIDERS: bool> EncryptedClientBuilder<BUILDER_UNSET, KMS_PROVIDERS> {
    /// Set the key vault `Namespace`, referring to a collection that contains all data keys used
    /// for encryption and decryption.
    pub fn key_vault_namespace(
        self,
        namespace: Namespace,
    ) -> EncryptedClientBuilder<BUILDER_SET, KMS_PROVIDERS> {
        EncryptedClientBuilder {
            client_options: self.client_options,
            key_vault_namespace: Some(namespace),
            kms_providers: self.kms_providers,
            optional: self.optional,
        }
    }
}

impl<const KV_NAMESPACE: bool> EncryptedClientBuilder<KV_NAMESPACE, BUILDER_UNSET> {
    /// Add configuration for multiple KMS providers.
    pub fn kms_providers(
        self,
        providers: impl IntoIterator<Item = (KmsProvider, bson::Document, Option<TlsOptions>)>,
    ) -> Result<EncryptedClientBuilder<KV_NAMESPACE, BUILDER_SET>> {
        Ok(EncryptedClientBuilder {
            client_options: self.client_options,
            key_vault_namespace: self.key_vault_namespace,
            kms_providers: Some(KmsProviders::new(providers)?),
            optional: self.optional,
        })
    }
}

impl<const KV_NAMESPACE: bool, const KMS_PROVIDERS: bool>
    EncryptedClientBuilder<KV_NAMESPACE, KMS_PROVIDERS>
{
    /// Set the client used for data key queries.  Will default to an internal client if not set.
    pub fn key_vault_client(mut self, client: impl Into<Option<Client>>) -> Self {
        self.optional.key_vault_client = client.into();
        self
    }

    /// Specify a JSONSchema locally.
    ///
    /// Supplying a `schema_map` provides more security than relying on JSON Schemas obtained from
    /// the server. It protects against a malicious server advertising a false JSON Schema, which
    /// could trick the client into sending unencrypted data that should be encrypted.
    ///
    /// Schemas supplied in the `schema_map` only apply to configuring automatic encryption for
    /// client side encryption. Other validation rules in the JSON schema will not be enforced by
    /// the driver and will result in an error.
    pub fn schema_map(mut self, map: impl IntoIterator<Item = (String, Document)>) -> Self {
        self.optional.schema_map = Some(map.into_iter().collect());
        self
    }

    /// Disable automatic encryption and do not spawn mongocryptd.  Defaults to false.
    pub fn bypass_auto_encryption(mut self, bypass: impl Into<Option<bool>>) -> Self {
        self.optional.bypass_auto_encryption = bypass.into();
        self
    }

    /// Set options related to mongocryptd.
    pub fn extra_options(mut self, extra_opts: impl Into<Option<Document>>) -> Self {
        self.optional.extra_options = extra_opts.into();
        self
    }

    /// Maps namespace to encrypted fields.
    ///
    /// Supplying an `encrypted_fields_map` provides more security than relying on an
    /// encryptedFields obtained from the server. It protects against a malicious server
    /// advertising a false encryptedFields.
    pub fn encrypted_fields_map(
        mut self,
        map: impl IntoIterator<Item = (String, Document)>,
    ) -> Self {
        self.optional.encrypted_fields_map = Some(map.into_iter().collect());
        self
    }

    /// Disable serverside processing of encrypted indexed fields, allowing use of explicit
    /// encryption with queryable encryption.
    pub fn bypass_query_analysis(mut self, bypass: impl Into<Option<bool>>) -> Self {
        self.optional.bypass_query_analysis = bypass.into();
        self
    }

    /// Disable loading crypt_shared.
    #[cfg(test)]
    pub(crate) fn disable_crypt_shared(mut self, disable: bool) -> Self {
        self.optional.disable_crypt_shared = Some(disable);
        self
    }
}

impl EncryptedClientBuilder<BUILDER_SET, BUILDER_SET> {
    /// Constructs a new `Client` using automatic encryption.  May perform DNS lookups as part of
    /// `Client` initialization.
    pub async fn build(self) -> Result<Client> {
        let client = Client::with_options(self.client_options)?;
        let enc_options = super::options::AutoEncryptionOptions {
            key_vault_client: self.optional.key_vault_client,
            key_vault_namespace: self
                .key_vault_namespace
                .ok_or_else(|| Error::internal("missing key_vault_namespace"))?,
            kms_providers: self
                .kms_providers
                .ok_or_else(|| Error::internal("missing kms_providers"))?,
            schema_map: self.optional.schema_map,
            bypass_auto_encryption: self.optional.bypass_auto_encryption,
            extra_options: self.optional.extra_options,
            encrypted_fields_map: self.optional.encrypted_fields_map,
            bypass_query_analysis: self.optional.bypass_query_analysis,
            #[cfg(test)]
            disable_crypt_shared: self.optional.disable_crypt_shared,
        };
        *client.inner.csfle.write().await =
            Some(super::ClientState::new(&client, enc_options).await?);
        Ok(client)
    }
}
