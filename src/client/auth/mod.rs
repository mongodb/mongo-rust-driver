use std::str::FromStr;

use bson::{Bson, Document};
use typed_builder::TypedBuilder;

use crate::error::{Error, ErrorKind, Result};

const SCRAM_SHA_1_STR: &str = "SCRAM-SHA-1";
const SCRAM_SHA_256_STR: &str = "SCRAM-SHA-256";
const MONGODB_CR_STR: &str = "MONGODB-CR";
const GSSAPI_STR: &str = "GSSAPI";
const MONGODB_X509_STR: &str = "MONGODB-X509";
const PLAIN_STR: &str = "PLAIN";

/// The authentication mechanisms supported by MongoDB.
///
/// Note: not all of these mechanisms are currently supported by the driver.
#[derive(Clone, PartialEq, Debug)]
pub enum AuthMechanism {
    /// MongoDB Challenge Response nonce and MD5 based authentication system. It is currently
    /// deprecated and will never be supported by this driver.
    MongoDbCr,

    /// The SCRAM-SHA-1 mechanism as defined in [RFC 5802](http://tools.ietf.org/html/rfc5802).
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/security-scram/) for more information.
    ScramSha1,

    /// The SCRAM-SHA-256 mechanism which extends [RFC 5802](http://tools.ietf.org/html/rfc5802) and is formally defined in [RFC 7677](https://tools.ietf.org/html/rfc7677).
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/security-scram/) for more information.
    ///
    /// Note: This mechanism is not currently supported by this driver but will be in the future.
    ScramSha256,

    /// The MONGODB-X509 mechanism based on the usage of X.509 certificates to validate a client
    /// where the distinguished subject name of the client certificate acts as the username.
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/security-x.509/) for more information.
    ///
    /// Note: This mechanism is not currently supported by this driver but will be in the future.
    MongoDbX509,

    /// Kerberos authentication mechanism as defined in [RFC 4752](http://tools.ietf.org/html/rfc4752).
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/kerberos/) for more information.
    ///
    /// Note: This mechanism is not currently supported by this driver but will be in the future.
    Gssapi,

    /// The SASL PLAIN mechanism, as defined in [RFC 4616](), is used in MongoDB to perform LDAP
    /// authentication and cannot be used for any other type of authenticaiton.
    /// Since the credentials are stored outside of MongoDB, the "$external" database must be used
    /// for authentication.
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/security-ldap/#ldap-proxy-authentication) for more information on LDAP authentication.
    ///
    /// Note: This mechanism is not currently supported by this driver but will be in the future.
    Plain,
}

impl AuthMechanism {
    /// Determines if the provided credentials have the required information to perform
    /// authentication.
    pub fn validate_credential(&self, credential: &Credential) -> Result<()> {
        match self {
            AuthMechanism::ScramSha1 | AuthMechanism::ScramSha256 => {
                if credential.username.is_none() {
                    return Err(ErrorKind::ArgumentError {
                        message: "No username provided for SCRAM authentication".to_string(),
                    }
                    .into());
                };
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            AuthMechanism::ScramSha1 => SCRAM_SHA_1_STR,
            AuthMechanism::ScramSha256 => SCRAM_SHA_256_STR,
            AuthMechanism::MongoDbCr => MONGODB_CR_STR,
            AuthMechanism::MongoDbX509 => MONGODB_X509_STR,
            AuthMechanism::Gssapi => GSSAPI_STR,
            AuthMechanism::Plain => PLAIN_STR,
        }
    }

    /// Get the default authSource for a given mechanism depending on the database provided in the
    /// connection string.
    pub(crate) fn default_source(&self, uri_db: Option<&str>) -> String {
        // TODO: fill in others as they're implemented
        match self {
            AuthMechanism::ScramSha1 | AuthMechanism::ScramSha256 | AuthMechanism::MongoDbCr => {
                uri_db.unwrap_or("admin").to_string()
            }
            _ => String::new(),
        }
    }
}

impl FromStr for AuthMechanism {
    type Err = Error;

    fn from_str(str: &str) -> Result<Self> {
        match str {
            SCRAM_SHA_1_STR => Ok(AuthMechanism::ScramSha1),
            SCRAM_SHA_256_STR => Ok(AuthMechanism::ScramSha256),
            MONGODB_CR_STR => Ok(AuthMechanism::MongoDbCr),
            MONGODB_X509_STR => Ok(AuthMechanism::MongoDbX509),
            GSSAPI_STR => Ok(AuthMechanism::Gssapi),
            PLAIN_STR => Ok(AuthMechanism::Plain),
            _ => Err(ErrorKind::ArgumentError {
                message: format!("invalid mechanism string: {}", str).to_string(),
            }
            .into()),
        }
    }
}

/// A struct containing authentication information.
///
/// Some fields (mechanism and source) may be omitted and will either be negotiated or assigned a
/// default value, depending on the values of other fields in the credential.
#[derive(Clone, Debug, Default, TypedBuilder, PartialEq)]
pub struct Credential {
    /// The username to authenticate with. This applies to all mechanisms but may be omitted when
    /// authenticating via MONGODB-X509.
    #[builder(default)]
    pub username: Option<String>,

    /// The database used to authenticate. This applies to all mechanisms and defaults to "admin"
    /// in SCRAM authentication mechanisms and "$external" for GSSAPI, MONGODB-X509 and PLAIN.
    #[builder(default)]
    pub source: Option<String>,

    /// The password to authenticate with. This does not apply to all mechanisms.
    #[builder(default)]
    pub password: Option<String>,

    /// Which authentication mechanism to use. If not provided, one will be negotiated with the
    /// server.
    #[builder(default)]
    pub mechanism: Option<AuthMechanism>,

    /// Additional properties for the given mechanism.
    #[builder(default)]
    pub mechanism_properties: Option<Document>,
}

impl Credential {
    #[allow(dead_code)]
    pub(crate) fn into_document(mut self) -> Document {
        let mut doc = Document::new();

        if let Some(s) = self.username.take() {
            doc.insert("username", s);
        }

        if let Some(s) = self.password.take() {
            doc.insert("password", s);
        } else {
            doc.insert("password", Bson::Null);
        }

        if let Some(s) = self.source.take() {
            doc.insert("db", s);
        }

        doc
    }
}
