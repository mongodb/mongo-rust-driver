//! Contains the `Error` and `Result` types that `mongodb` uses.

use std::{fmt, sync::Arc};

use err_derive::Error;
use lazy_static::lazy_static;
use serde::Deserialize;
use time::OutOfRangeError;

use crate::options::StreamAddress;

lazy_static! {
    static ref RECOVERING_CODES: Vec<i32> = vec![11600, 11602, 13436, 189, 91];
    static ref NOTMASTER_CODES: Vec<i32> = vec![10107, 13435];
    static ref SHUTTING_DOWN_CODES: Vec<i32> = vec![11600, 91];
    static ref RETRYABLE_READ_CODES: Vec<i32> =
        vec![11600, 11602, 10107, 13435, 13436, 189, 91, 7, 6, 89, 9001];
}

/// The result type for all methods that can return an error in the `mongodb` crate.
pub type Result<T> = std::result::Result<T, Error>;

/// An error that can occur in the `mongodb` crate. The inner
/// [`ErrorKind`](enum.ErrorKind.html) is wrapped in an `Arc` to allow the errors to be
/// cloned.
#[derive(Clone, Debug, Error)]
#[error(display = "{}", kind)]
pub struct Error {
    /// The type of error that occurred.
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

    /// Attempts to get the `std::io::Error` from this `Error`.
    /// If there are other references to the underlying `Arc`, or if the `ErrorKind` is not `Io`,
    /// then the original error is returned as a custom `std::io::Error`.
    pub(crate) fn into_io_error(self) -> std::io::Error {
        match Arc::try_unwrap(self.kind) {
            Ok(ErrorKind::Io(io_error)) => io_error,
            Ok(other_error_kind) => {
                let error: Error = other_error_kind.into();
                std::io::Error::new(std::io::ErrorKind::Other, Box::new(error))
            }
            Err(e) => std::io::Error::new(std::io::ErrorKind::Other, Box::new(Error { kind: e })),
        }
    }

    /// Whether this error is an "ns not found" error or not.
    pub(crate) fn is_ns_not_found(&self) -> bool {
        match self.kind.as_ref() {
            ErrorKind::CommandError(err) if err.code == 26 => true,
            _ => false,
        }
    }

    /// Whether a read operation should be retried if this error occurs
    pub(crate) fn is_read_retryable(&self) -> bool {
        if self.is_network_error() {
            return true;
        }
        let error_kind: &ErrorKind = &self.kind;
        match &self.kind.code_and_message() {
            Some((code, message)) => {
                if RETRYABLE_READ_CODES.contains(&code) {
                    return true;
                }
                if message.contains("not master") || message.contains("node is recovering") {
                    return true;
                }
                false
            }
            None => false,
        }
    }

    /// Whether an error originated in the driver
    pub(crate) fn is_driver_error(&self) -> bool {
        match self.kind.as_ref() {
            ErrorKind::AddrParse(_) => true,
            #[cfg(feature = "async-std-runtime")]
            ErrorKind::AsyncStdTimeout(_) => true,
            ErrorKind::BsonDecode(_) => true,
            ErrorKind::BsonEncode(_) => true,
            ErrorKind::InternalError { .. } => true,
            ErrorKind::InvalidHostname { .. } => true,
            ErrorKind::ParseError { .. } => true,
            #[cfg(feature = "tokio-runtime")]
            ErrorKind::TokioTimeoutElapsed(_) => true,
            _ => false,
        }
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

/// The types of errors that can occur.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Wrapper around [`std::net::AddrParseError`](https://doc.rust-lang.org/std/net/struct.AddrParseError.html).
    #[error(display = "{}", _0)]
    AddrParse(#[error(source)] std::net::AddrParseError),

    /// An invalid argument was provided to a database operation.
    #[error(
        display = "An invalid argument was provided to a database operation: {}",
        message
    )]
    #[non_exhaustive]
    ArgumentError { message: String },

    #[cfg(feature = "async-std-runtime")]
    #[error(display = "{}", _0)]
    AsyncStdTimeout(#[error(source)] async_std::future::TimeoutError),

    /// An error occurred while the [`Client`](../struct.Client.html) attempted to authenticate a
    /// connection.
    #[error(display = "{}", message)]
    #[non_exhaustive]
    AuthenticationError { message: String },

    /// Wrapper around `bson::de::Error`.
    #[error(display = "{}", _0)]
    BsonDecode(#[error(source)] crate::bson::de::Error),

    /// Wrapper around `bson::ser::Error`.
    #[error(display = "{}", _0)]
    BsonEncode(#[error(source)] crate::bson::ser::Error),

    /// An error occurred when trying to execute a write operation consisting of multiple writes.
    #[error(
        display = "An error occurred when trying to execute a write operation: {:?}",
        _0
    )]
    BulkWriteError(BulkWriteFailure),

    /// The server returned an error to an attempted operation.
    #[error(display = "Command failed {}", _0)]
    CommandError(CommandError),

    // `trust_dns` does not implement the `Error` trait on their errors, so we have to manually
    // implement `From` rather than using the `source annotation.
    /// Wrapper around `trust_dns_resolver::error::ResolveError`.
    #[error(display = "{}", _0)]
    DnsResolve(trust_dns_resolver::error::ResolveError),

    #[error(display = "Internal error: {}", message)]
    #[non_exhaustive]
    InternalError { message: String },

    /// Wrapper around `webpki::InvalidDNSNameError`.
    #[error(display = "{}", _0)]
    InvalidDnsName(#[error(source)] webpki::InvalidDNSNameError),

    /// A hostname could not be parsed.
    #[error(display = "Unable to parse hostname: {}", hostname)]
    #[non_exhaustive]
    InvalidHostname { hostname: String },

    /// Wrapper around [`std::io::Error`](https://doc.rust-lang.org/std/io/struct.Error.html).
    #[error(display = "{}", _0)]
    Io(#[error(source)] std::io::Error),

    #[error(display = "No DNS results for domain {}", _0)]
    NoDnsResults(StreamAddress),

    /// A database operation failed to send or receive a reply.
    #[error(
        display = "A database operation failed to send or receive a reply: {}",
        message
    )]
    #[non_exhaustive]
    OperationError { message: String },

    #[error(display = "{}", _0)]
    OutOfRangeError(#[error(source)] OutOfRangeError),

    /// Data from a file could not be parsed.
    #[error(display = "Unable to parse {} data from {}", data_type, file_path)]
    #[non_exhaustive]
    ParseError {
        data_type: String,
        file_path: String,
    },

    /// The server returned an invalid reply to a database operation.
    #[error(
        display = "The server returned an invalid reply to a database operation: {}",
        message
    )]
    #[non_exhaustive]
    ResponseError { message: String },

    /// The Client was not able to select a server for the operation.
    #[error(display = "{}", message)]
    #[non_exhaustive]
    ServerSelectionError { message: String },

    /// An error occurred during SRV record lookup.
    #[error(display = "An error occurred during SRV record lookup: {}", message)]
    #[non_exhaustive]
    SrvLookupError { message: String },

    /// A timeout occurred before a Tokio task could be completed.
    #[cfg(feature = "tokio-runtime")]
    #[error(display = "{}", _0)]
    TokioTimeoutElapsed(#[error(source)] tokio::time::Elapsed),

    #[error(display = "{}", _0)]
    RustlsConfig(#[error(source)] rustls::TLSError),

    /// An error occurred during TXT record lookup
    #[error(display = "An error occurred during TXT record lookup: {}", message)]
    #[non_exhaustive]
    TxtLookupError { message: String },

    /// The Client timed out while checking out a connection from connection pool.
    #[error(
        display = "Timed out while checking out a connection from connection pool with address {}",
        address
    )]
    #[non_exhaustive]
    WaitQueueTimeoutError { address: StreamAddress },

    /// An error occurred when trying to execute a write operation
    #[error(
        display = "An error occurred when trying to execute a write operation: {:?}",
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

    pub(crate) fn is_network_error(&self) -> bool {
        match self {
            ErrorKind::Io(..) => true,
            _ => false,
        }
    }

    pub(crate) fn is_authentication_error(&self) -> bool {
        match self {
            ErrorKind::AuthenticationError { .. } => true,
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
#[non_exhaustive]
pub struct CommandError {
    /// Identifies the type of error.
    pub code: i32,

    /// The name associated with the error code.
    #[serde(rename = "codeName", default)]
    pub code_name: String,

    /// A description of the error that occurred.
    #[serde(rename = "errmsg")]
    pub message: String,

    /// The error labels that the server returned.
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
#[non_exhaustive]
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
#[non_exhaustive]
pub struct WriteError {
    /// Identifies the type of write error.
    pub code: i32,

    /// The name associated with the error code.
    ///
    /// Note that the server will not return this in some cases, hence `code_name` being an
    /// `Option`.
    pub code_name: Option<String>,

    /// A description of the error that occurred.
    pub message: String,
}

/// An error that occurred during a write operation consisting of multiple writes that wasn't due to
/// being unable to satisfy a write concern.
#[derive(Debug, PartialEq, Clone, Deserialize)]
#[non_exhaustive]
pub struct BulkWriteError {
    /// Index into the list of operations that this error corresponds to.
    pub index: usize,

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

/// The set of errors that occurred during a write operation.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BulkWriteFailure {
    /// The error(s) that occurred on account of a non write concern failure.
    pub write_errors: Option<Vec<BulkWriteError>>,

    /// The error that occurred on account of write concern failure.
    pub write_concern_error: Option<WriteConcernError>,
}

impl BulkWriteFailure {
    pub(crate) fn new() -> Self {
        BulkWriteFailure {
            write_errors: None,
            write_concern_error: None,
        }
    }
}

/// An error that occurred when trying to execute a write operation.
#[derive(Clone, Debug)]
#[non_exhaustive]
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
