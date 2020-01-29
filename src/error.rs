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
pub enum ErrorKind {
    /// Wrapper around [`std::net::AddrParseError`](https://doc.rust-lang.org/std/net/struct.AddrParseError.html).
    #[error(display = "{}", _0)]
    AddrParse(#[error(source)] std::net::AddrParseError),

    /// An invalid argument was provided to a database operation.
    #[error(
        display = "An invalid argument was provided to a database operation: {}",
        message
    )]
    ArgumentError { message: String },

    /// An error occurred while the [`Client`](../struct.Client.html) attempted to authenticate a
    /// connection.
    #[error(display = "{}", message)]
    AuthenticationError { message: String },

    /// Wrapper around `bson::DecoderError`.
    #[error(display = "{}", _0)]
    BsonDecode(#[error(source)] bson::DecoderError),

    /// Wrapper around `bson::EncoderError`.
    #[error(display = "{}", _0)]
    BsonEncode(#[error(source)] bson::EncoderError),

    /// An error occurred when trying to execute a write operation consisting of multiple writes.
    #[error(
        display = "An error occurred when trying to execute a write operation: {:?}",
        _0
    )]
    BulkWriteError(BulkWriteFailure),

    /// The server returned an error to an attempted operation.
    #[error(display = "Command failed {}", _0)]
    CommandError(CommandError),

    /// Wrapper around `webpki::InvalidDNSNameError`.
    #[error(display = "{}", _0)]
    DnsName(#[error(source)] webpki::InvalidDNSNameError),

    // `trust_dns` does not implement the `Error` trait on their errors, so we have to manually
    // implement `From` rather than using the `source annotation.
    /// Wrapper around `trust_dns_resolver::error::ResolveError`.
    #[error(display = "{}", _0)]
    DnsResolve(trust_dns_resolver::error::ResolveError),

    /// A hostname could not be parsed.
    #[error(display = "Unable to parse hostname: {}", hostname)]
    InvalidHostname { hostname: String },

    /// Wrapper around [`std::io::Error`](https://doc.rust-lang.org/std/io/struct.Error.html).
    #[error(display = "{}", _0)]
    Io(#[error(source)] std::io::Error),

    /// A database operation failed to send or receive a reply.
    #[error(
        display = "A database operation failed to send or receive a reply: {}",
        message
    )]
    OperationError { message: String },

    #[error(display = "{}", _0)]
    OutOfRangeError(#[error(source)] OutOfRangeError),

    /// Data from a file could not be parsed.
    #[error(display = "Unable to parse {} data from {}", data_type, file_path)]
    ParseError {
        data_type: String,
        file_path: String,
    },

    /// The server returned an invalid reply to a database operation.
    #[error(
        display = "The server returned an invalid reply to a database operation: {}",
        message
    )]
    ResponseError { message: String },

    /// The Client was not able to select a server for the operation.
    #[error(display = "{}", message)]
    ServerSelectionError { message: String },

    /// An error occurred during SRV record lookup.
    #[error(display = "An error occurred during SRV record lookup: {}", message)]
    SrvLookupError { message: String },

    /// An error occurred during TXT record lookup
    #[error(display = "An error occurred during TXT record lookup: {}", message)]
    TxtLookupError { message: String },

    /// The Client timed out while checking out a connection from connection pool.
    #[error(
        display = "Timed out while checking out a connection from connection pool with address {}",
        address
    )]
    WaitQueueTimeoutError { address: StreamAddress },

    /// An error occurred when trying to execute a write operation
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
