use bson::RawDocumentBuf;
use mongocrypt::ctx::Algorithm;

use super::super::option_setters;
use crate::client_encryption::{ClientEncryption, EncryptKey, RangeOptions};

impl ClientEncryption {
    /// Encrypts a BsonValue with a given key and algorithm.
    ///
    /// To insert or query with an "Indexed" encrypted payload, use a `Client` configured with
    /// `AutoEncryptionOptions`. `AutoEncryptionOptions.bypass_query_analysis` may be true.
    /// `AutoEncryptionOptions.bypass_auto_encryption` must be false.
    ///
    /// `await` will return a `Result<Binary>` (subtype 6) containing the encrypted value.
    pub fn encrypt_2(
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

    /// NOTE: This method is experimental only. It is not intended for public use.
    ///
    /// Encrypts a match or aggregate expression with the given key.
    ///
    /// The expression will be encrypted using the [`Algorithm::RangePreview`] algorithm and the
    /// "rangePreview" query type.
    ///
    /// `await` returns a `Result<Document>` containing the encrypted expression.
    pub fn encrypt_expression_2(
        &self,
        expression: RawDocumentBuf,
        key: impl Into<EncryptKey>,
    ) -> Encrypt<'_, Expression> {
        Encrypt {
            client_enc: self,
            mode: Expression { value: expression },
            key: key.into(),
            algorithm: Algorithm::RangePreview,
            options: None,
        }
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
pub struct EncryptOptions {
    /// The contention factor.
    pub contention_factor: Option<i64>,
    /// The query type.
    pub query_type: Option<String>,
    /// NOTE: This method is experimental and not intended for public use.
    ///
    /// Set the range options. This method should only be called when the algorithm is
    /// [`Algorithm::RangePreview`].
    pub range_options: Option<RangeOptions>,
}

impl<'a, Mode> Encrypt<'a, Mode> {
    option_setters!(options: EncryptOptions;
        contention_factor: i64,
    );
}

impl<'a> Encrypt<'a, Value> {
    /// Set the [`EncryptOptions::query_type`] option.
    pub fn query_type(mut self, value: impl Into<String>) -> Self {
        self.options().query_type = Some(value.into());
        self
    }
}

impl<'a> Encrypt<'a, Expression> {
    /// Set the [`EncryptOptions::range_options`] option.
    pub fn range_options(mut self, value: RangeOptions) -> Self {
        self.options().range_options = Some(value);
        self
    }
}

// Action impl in src/client/csfle/client_encryption/encrypt.rs
