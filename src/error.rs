use std::{fmt, sync::Arc};

use err_derive::Error;
use lazy_static::lazy_static;
use serde::Deserialize;

use crate::options::StreamAddress;

lazy_static! {
    static ref RECOVERING_CODES: Vec<i32> = vec![11600, 11602, 13436, 189, 91];
    static ref NOTMASTER_CODES: Vec<i32> = vec![10107, 13435];
    static ref SHUTTING_DOWN_CODES: Vec<i32> = vec![11600, 91];
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Error)]
#[error(display = "{}", kind)]
pub struct Error {
    pub kind: Arc<ErrorKind>,
}

impl Error {
    /// Creates an `AuthenticationError` for the given mechanism with the provided reason.
    pub(crate) fn authentication_error(mechanism_name: &str, reason: &str) -> Self {
        ErrorKind::AuthenticationError {
            message: format!("{} failure: {}", mechanism_name, reason),
        }
        .into()
    }

    /// Creates an `AuthenticationError` for the given mechanism with a generic "unknown" message.
    pub(crate) fn unknown_authentication_error(mechanism_name: &str) -> Error {
        Error::authentication_error(mechanism_name, "internal error")
    }

    /// Creates an `AuthenticationError` for the given mechanism when the server response is
    /// invalid.
    pub(crate) fn invalid_authentication_response(mechanism_name: &str) -> Error {
        Error::authentication_error(mechanism_name, "invalid server response")
    }
}

impl<E> From<E> for Error
where
    ErrorKind: From<E>,
{
    fn from(err: E) -> Self {
        Self {
            kind: Arc::new(err.into()),
        }
    }
}

impl std::ops::Deref for Error {
    type Target = Arc<ErrorKind>;

    fn deref(&self) -> &Self::Target {
        &self.kind
    }
}

#[derive(Debug, Error)]
pub enum ErrorKind {
    #[error(
        display = "An invalid argument was provided to a database operation: {}",
        message
    )]
    ArgumentError { message: String },

    #[error(display = "{}", message)]
    AuthenticationError { message: String },

    #[error(display = "{}", _0)]
    BsonDecode(#[error(source)] bson::DecoderError),

    #[error(display = "{}", _0)]
    BsonEncode(#[error(source)] bson::EncoderError),

    #[error(
        display = "An error ocurred when trying to execute a write operation: {:?}",
        _0
    )]
    BulkWriteError(BulkWriteFailure),

    #[error(display = "Command failed {}", _0)]
    CommandError(CommandError),

    #[error(display = "{}", _0)]
    DnsName(#[error(source)] webpki::InvalidDNSNameError),

    // `trust_dns` does not implement the `Error` trait on their errors, so we have to manually
    // implement `From` rather than using the `source annotation.
    #[error(display = "{}", _0)]
    DnsResolve(trust_dns_resolver::error::ResolveError),

    #[error(display = "Unable to parse hostname: {}", hostname)]
    InvalidHostname { hostname: String },

    #[error(display = "{}", _0)]
    Io(#[error(source)] std::io::Error),

    #[error(
        display = "A database operation failed to send or receive a reply: {}",
        message
    )]
    OperationError { message: String },

    #[error(display = "Unable to parse {} data from {}", data_type, file_path)]
    ParseError {
        data_type: String,
        file_path: String,
    },

    #[error(
        display = "Attempted to check out a connection from closed connection pool with address {}",
        address
    )]
    PoolClosedError { address: String },

    #[error(
        display = "A database operation returned an invalid reply: {}",
        message
    )]
    ResponseError { message: String },

    #[error(
        display = "A database operation returned an invalid reply: {}",
        message
    )]
    ServerSelectionError { message: String },

    #[error(display = "An error occurred during SRV record lookup: {}", message)]
    SrvLookupError { message: String },

    #[error(display = "An error occurred during TXT record lookup: {}", message)]
    TxtLookupError { message: String },

    #[error(
        display = "Timed out while checking out a connection from connection pool with address {}",
        address
    )]
    WaitQueueTimeoutError { address: StreamAddress },

    #[error(
        display = "An error occurred when trying to execute a write operation: {}",
        _0
    )]
    WriteError(WriteFailure),
}

impl From<trust_dns_resolver::error::ResolveError> for ErrorKind {
    fn from(error: trust_dns_resolver::error::ResolveError) -> Self {
        Self::DnsResolve(error)
    }
}

impl ErrorKind {
    pub(crate) fn is_non_timeout_network_error(&self) -> bool {
        match self {
            ErrorKind::Io(ref io_err) if io_err.kind() != std::io::ErrorKind::TimedOut => true,
            _ => false,
        }
    }

    /// Gets the code/message tuple from this error, if applicable. In the case of write errors, the
    /// code and message are taken from the write concern error, if there is one.
    fn code_and_message(&self) -> Option<(i32, &str)> {
        match self {
            ErrorKind::CommandError(ref cmd_err) => Some((cmd_err.code, cmd_err.message.as_str())),
            ErrorKind::WriteError(WriteFailure::WriteConcernError(ref wc_err)) => {
                Some((wc_err.code, wc_err.message.as_str()))
            }
            ErrorKind::BulkWriteError(ref bwe) => bwe
                .write_concern_error
                .as_ref()
                .map(|wc_err| (wc_err.code, wc_err.message.as_str())),
            _ => None,
        }
    }

    /// If this error corresponds to a "not master" error as per the SDAM spec.
    pub(crate) fn is_not_master(&self) -> bool {
        self.code_and_message()
            .map(|(code, msg)| is_not_master(code, msg))
            .unwrap_or(false)
    }

    /// If this error corresponds to a "node is recovering" error as per the SDAM spec.
    pub(crate) fn is_recovering(&self) -> bool {
        self.code_and_message()
            .map(|(code, msg)| is_recovering(code, msg))
            .unwrap_or(false)
    }

    /// If this error corresponds to a "node is shutting down" error as per the SDAM spec.
    pub(crate) fn is_shutting_down(&self) -> bool {
        self.code_and_message()
            .map(|(code, _)| SHUTTING_DOWN_CODES.contains(&code))
            .unwrap_or(false)
    }
}

fn is_not_master(code: i32, message: &str) -> bool {
    if NOTMASTER_CODES.contains(&code) {
        return true;
    } else if is_recovering(code, message) {
        return false;
    }
    message.contains("not master")
}

fn is_recovering(code: i32, message: &str) -> bool {
    if RECOVERING_CODES.contains(&code) {
        return true;
    }
    message.contains("not master or secondary") || message.contains("node is recovering")
}

/// An error that occurred due to a database command failing.
#[derive(Clone, Debug, Deserialize)]
pub struct CommandError {
    pub code: i32,

    #[serde(rename = "codeName", default)]
    pub code_name: String,

    #[serde(rename = "errmsg")]
    pub message: String,

    #[serde(rename = "errorLabels", default)]
    pub labels: Vec<String>,
}

impl fmt::Display for CommandError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "({}): {})", self.code_name, self.message)
    }
}

/// An error that occurred due to not being able to satisfy a write concern.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct WriteConcernError {
    /// Identifies the type of write concern error.
    pub code: i32,

    /// The name associated with the error code.
    #[serde(rename = "codeName")]
    pub code_name: String,

    /// A description of the error that occurred.
    #[serde(rename = "errmsg")]
    pub message: String,
}

/// An error that occurred during a write operation that wasn't due to being unable to satisfy a
/// write concern.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteError {
    /// Identifies the type of write concern error.
    pub code: i32,

    /// The name associated with the error code.
    ///
    /// Note that the server will not return this in some cases, hence `code_name` being an
    /// `Option`.
    pub code_name: Option<String>,

    /// A description of the error that occurred.
    pub message: String,
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct BulkWriteError {
    /// Index into the list of operations that this error corresponds to.
    pub index: i32,

    /// Identifies the type of write concern error.
    pub code: i32,

    /// The name associated with the error code.
    ///
    /// Note that the server will not return this in some cases, hence `code_name` being an
    /// `Option`.
    #[serde(rename = "codeName", default)]
    pub code_name: Option<String>,

    /// A description of the error that occurred.
    #[serde(rename = "errmsg")]
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct BulkWriteFailure {
    /// The error(s) that occured on account of a non write concern failure.
    pub write_errors: Option<Vec<BulkWriteError>>,

    /// The error that ocurred on account of write concern failure.
    pub write_concern_error: Option<WriteConcernError>,
}

/// An error that occurred when trying to execute a write operation.
#[derive(Clone, Debug)]
pub enum WriteFailure {
    WriteConcernError(WriteConcernError),
    WriteError(WriteError),
}

impl WriteFailure {
    fn from_bulk_failure(bulk: BulkWriteFailure) -> Result<Self> {
        if let Some(bulk_write_error) = bulk.write_errors.and_then(|es| es.into_iter().next()) {
            let write_error = WriteError {
                code: bulk_write_error.code,
                code_name: bulk_write_error.code_name,
                message: bulk_write_error.message,
            };
            Ok(WriteFailure::WriteError(write_error))
        } else if let Some(wc_error) = bulk.write_concern_error {
            Ok(WriteFailure::WriteConcernError(wc_error))
        } else {
            Err(ErrorKind::ResponseError {
                message: "error missing write errors and write concern errors".to_string(),
            }
            .into())
        }
    }
}

impl fmt::Display for WriteFailure {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        unimplemented!()
    }
}

/// Translates ErrorKind::BulkWriteError cases to ErrorKind::WriteErrors, leaving all other errors
/// untouched.
pub(crate) fn convert_bulk_errors(error: Error) -> Error {
    match *error.kind {
        ErrorKind::BulkWriteError(ref bulk_failure) => {
            match WriteFailure::from_bulk_failure(bulk_failure.clone()) {
                Ok(failure) => ErrorKind::WriteError(failure).into(),
                Err(e) => e,
            }
        }
        _ => error,
    }
}
