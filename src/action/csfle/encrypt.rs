use bson::{Binary, Bson, RawDocumentBuf};
use macro_magic::export_tokens;
use mongocrypt::ctx::Algorithm;
use mongodb_internal_macros::{export_doc, option_setters_2, options_doc};
use serde::Serialize;
use serde_with::skip_serializing_none;
use typed_builder::TypedBuilder;

use super::super::deeplink;
use crate::client_encryption::ClientEncryption;

impl ClientEncryption {
    /// Encrypts a BsonValue with a given key and algorithm.
    ///
    /// To insert or query with an "Indexed" encrypted payload, use a `Client` configured with
    /// `AutoEncryptionOptions`. `AutoEncryptionOptions.bypass_query_analysis` may be true.
    /// `AutoEncryptionOptions.bypass_auto_encryption` must be false.
    ///
    /// `await` will return a d[`Result<Binary>`] (subtype 6) containing the encrypted value.
    #[deeplink]
    #[options_doc(encrypt)]
    pub fn encrypt(
        &self,
        value: impl Into<bson::RawBson>,
        key: impl Into<EncryptKey>,
        algorithm: Algorithm,
    ) -> Encrypt {
        Encrypt {
            client_enc: self,
            mode: Value {
                value: value.into(),
            },
            key: key.into(),
            algorithm,
            options: None,
        }
    }

    /// Encrypts a Match Expression or Aggregate Expression to query a range index.
    /// `expression` is expected to be a BSON document of one of the following forms:
    /// 1. A Match Expression of this form:
    ///   {$and: [{<field>: {$gt: <value1>}}, {<field>: {$lt: <value2> }}]}
    /// 2. An Aggregate Expression of this form:
    ///   {$and: [{$gt: [<fieldpath>, <value1>]}, {$lt: [<fieldpath>, <value2>]}]
    /// $gt may also be $gte. $lt may also be $lte.
    ///
    /// The expression will be encrypted using the [`Algorithm::Range`] algorithm and the
    /// "range" query type.
    ///
    /// `await` will return a d[`Result<Document>`] containing the encrypted expression.
    #[deeplink]
    #[options_doc(encrypt_expr)]
    pub fn encrypt_expression(
        &self,
        expression: RawDocumentBuf,
        key: impl Into<EncryptKey>,
    ) -> Encrypt<'_, Expression> {
        Encrypt {
            client_enc: self,
            mode: Expression { value: expression },
            key: key.into(),
            algorithm: Algorithm::Range,
            options: Some(EncryptOptions {
                query_type: Some("range".into()),
                ..Default::default()
            }),
        }
    }
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

impl From<Binary> for EncryptKey {
    fn from(bin: Binary) -> Self {
        Self::Id(bin)
    }
}

impl From<String> for EncryptKey {
    fn from(s: String) -> Self {
        Self::AltName(s)
    }
}

/// Encrypt a value.  Construct with [`ClientEncryption::encrypt`].
#[must_use]
pub struct Encrypt<'a, Mode = Value> {
    pub(crate) client_enc: &'a ClientEncryption,
    pub(crate) mode: Mode,
    pub(crate) key: EncryptKey,
    pub(crate) algorithm: Algorithm,
    pub(crate) options: Option<EncryptOptions>,
}

pub struct Value {
    pub(crate) value: bson::RawBson,
}

pub struct Expression {
    pub(crate) value: RawDocumentBuf,
}

/// Options for encrypting a value.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
#[export_tokens]
pub struct EncryptOptions {
    /// The contention factor.
    pub contention_factor: Option<i64>,
    /// The query type.
    pub query_type: Option<String>,
    /// Set the range options. This should only be set when the algorithm is
    /// [`Algorithm::Range`].
    pub range_options: Option<RangeOptions>,
}

/// The index options for a Queryable Encryption field supporting "range" queries.
/// The options set must match the values set in the encryptedFields of the destination collection.
#[skip_serializing_none]
#[derive(Clone, Default, Debug, Serialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct RangeOptions {
    /// The minimum value. This option must be set if `precision` is set.
    pub min: Option<Bson>,

    /// The maximum value. This option must be set if `precision` is set.
    pub max: Option<Bson>,

    /// May be used to tune performance. When omitted, a default value is used.
    pub trim_factor: Option<i32>,

    /// May be used to tune performance. When omitted, a default value is used.
    pub sparsity: Option<i64>,

    /// Determines the number of significant digits after the decimal point. This value must only
    /// be set for Double and Decimal128 fields.
    pub precision: Option<i32>,
}

#[option_setters_2(EncryptOptions, skip = [query_type])]
#[export_doc(encrypt, extra = [query_type])]
#[export_doc(encrypt_expr)]
impl<Mode> Encrypt<'_, Mode> {}

impl Encrypt<'_, Value> {
    /// Set the [`EncryptOptions::query_type`] option.
    pub fn query_type(mut self, value: impl Into<String>) -> Self {
        self.options().query_type = Some(value.into());
        self
    }
}

// Action impl in src/client/csfle/client_encryption/encrypt.rs
