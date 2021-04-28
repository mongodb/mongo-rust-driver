//! Contains the `Error` and `Result` types that `mongodb` uses.

use std::{fmt, sync::Arc};

use serde::Deserialize;
use thiserror::Error;

use crate::{bson::Document, options::StreamAddress};

const RECOVERING_CODES: [i32; 5] = [11600, 11602, 13436, 189, 91];
const NOTMASTER_CODES: [i32; 3] = [10107, 13435, 10058];
const SHUTTING_DOWN_CODES: [i32; 2] = [11600, 91];
const RETRYABLE_READ_CODES: [i32; 11] =
    [11600, 11602, 10107, 13435, 13436, 189, 91, 7, 6, 89, 9001];
const RETRYABLE_WRITE_CODES: [i32; 12] = [
    11600, 11602, 10107, 13435, 13436, 189, 91, 7, 6, 89, 9001, 262,
];

/// The result type for all methods that can return an error in the `mongodb` crate.
pub type Result<T> = std::result::Result<T, Error>;

/// An error that can occur in the `mongodb` crate. The inner
/// [`ErrorKind`](enum.ErrorKind.html) is wrapped in an `Arc` to allow the errors to be
/// cloned.
#[derive(Clone, Debug, Error)]
#[error("{kind}")]
#[non_exhaustive]
pub struct Error {
    /// The type of error that occurred.
    pub kind: ErrorKind,
    labels: Vec<String>,
}

impl Error {
    pub(crate) fn pool_cleared_error(address: &StreamAddress) -> Self {
        ErrorKind::ConnectionPoolClearedError {
            message: format!(
                "Connection pool for {} cleared during operation execution",
                address
            ),
        }
        .into()
    }

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

    pub(crate) fn is_state_change_error(&self) -> bool {
        self.is_recovering() || self.is_not_master()
    }

    pub(crate) fn is_auth_error(&self) -> bool {
        matches!(self.kind, ErrorKind::AuthenticationError { .. })
    }

    pub(crate) fn is_command_error(&self) -> bool {
        matches!(self.kind, ErrorKind::CommandError(_))
    }

    pub(crate) fn is_network_timeout(&self) -> bool {
        matches!(self.kind, ErrorKind::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::TimedOut)
    }

    /// Whether this error is an "ns not found" error or not.
    pub(crate) fn is_ns_not_found(&self) -> bool {
        matches!(self.kind, ErrorKind::CommandError(ref err) if err.code == 26)
    }

    /// Whether a read operation should be retried if this error occurs.
    pub(crate) fn is_read_retryable(&self) -> bool {
        if self.is_network_error() {
            return true;
        }
        match &self.kind.code() {
            Some(code) => {
                RETRYABLE_READ_CODES.contains(&code)
            }
            None => false,
        }
    }

    pub(crate) fn is_write_retryable(&self) -> bool {
        self.contains_label("RetryableWriteError")
    }

    /// Whether a "RetryableWriteError" label should be added to this error. If max_wire_version
    /// indicates a 4.4+ server, a label should only be added if the error is a network error.
    /// Otherwise, a label should be added if the error is a network error or the error code
    /// matches one of the retryable write codes.
    pub(crate) fn should_add_retryable_write_label(&self, max_wire_version: i32) -> bool {
        if max_wire_version > 8 {
            return self.is_network_error();
        }
        if self.is_network_error() {
            return true;
        }
        match &self.kind.code() {
            Some(code) => RETRYABLE_WRITE_CODES.contains(&code),
            None => false,
        }
    }

    /// Whether an error originated from the server.
    pub(crate) fn is_server_error(&self) -> bool {
        matches!(
            self.kind,
            ErrorKind::AuthenticationError { .. }
                | ErrorKind::BulkWriteError(_)
                | ErrorKind::CommandError(_)
                | ErrorKind::WriteError(_)
        )
    }

    /// Returns the labels for this error.
    pub fn labels(&self) -> &[String] {
        match self.kind {
            ErrorKind::CommandError(ref err) => &err.labels,
            ErrorKind::WriteError(ref err) => match err {
                WriteFailure::WriteError(_) => &self.labels,
                WriteFailure::WriteConcernError(ref err) => &err.labels,
            },
            ErrorKind::BulkWriteError(ref err) => match err.write_concern_error {
                Some(ref err) => &err.labels,
                None => &self.labels,
            },
            _ => &self.labels,
        }
    }

    /// Whether this error contains the specified label.
    pub fn contains_label<T: AsRef<str>>(&self, label: T) -> bool {
        self.labels()
            .iter()
            .any(|actual_label| actual_label.as_str() == label.as_ref())
    }

    /// Returns a copy of this Error with the specified label added.
    pub(crate) fn with_label<T: AsRef<str>>(mut self, label: T) -> Self {
        let label = label.as_ref().to_string();
        match self.kind {
            ErrorKind::CommandError(ref err) => {
                let mut err = err.clone();
                err.labels.push(label);
                ErrorKind::CommandError(err).into()
            }
            ErrorKind::WriteError(ref err) => match err {
                WriteFailure::WriteError(_) => {
                    self.labels.push(label);
                    self
                }
                WriteFailure::WriteConcernError(ref err) => {
                    let mut err = err.clone();
                    err.labels.push(label);
                    ErrorKind::WriteError(WriteFailure::WriteConcernError(err)).into()
                }
            },
            ErrorKind::BulkWriteError(ref err) => match err.write_concern_error {
                Some(ref write_concern_error) => {
                    let mut err = err.clone();
                    let mut write_concern_error = write_concern_error.clone();
                    write_concern_error.labels.push(label);
                    err.write_concern_error = Some(write_concern_error);
                    ErrorKind::BulkWriteError(err).into()
                }
                None => {
                    self.labels.push(label);
                    self
                }
            },
            _ => {
                self.labels.push(label);
                self
            }
        }
    }
}

impl<E> From<E> for Error
where
    ErrorKind: From<E>,
{
    fn from(err: E) -> Self {
        Self {
            kind: err.into(),
            labels: Vec::new(),
        }
    }
}

impl From<bson::de::Error> for ErrorKind {
    fn from(err: bson::de::Error) -> Self {
        Self::BsonDecode(err)
    }
}

impl From<bson::ser::Error> for ErrorKind {
    fn from(err: bson::ser::Error) -> Self {
        Self::BsonEncode(err)
    }
}

impl From<std::io::Error> for ErrorKind {
    fn from(err: std::io::Error) -> Self {
        Self::Io(Arc::new(err))
    }
}

impl From<std::io::ErrorKind> for ErrorKind {
    fn from(err: std::io::ErrorKind) -> Self {
        Self::Io(Arc::new(err.into()))
    }
}

impl std::ops::Deref for Error {
    type Target = ErrorKind;

    fn deref(&self) -> &Self::Target {
        &self.kind
    }
}

/// The types of errors that can occur.
#[allow(missing_docs)]
#[derive(Clone, Debug, Error)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Wrapper around [`std::net::AddrParseError`](https://doc.rust-lang.org/std/net/struct.AddrParseError.html).
    #[error("{0}")]
    AddrParse(#[from] std::net::AddrParseError),

    /// An invalid argument was provided to a database operation.
    #[error("An invalid argument was provided to a database operation: {message}")]
    #[non_exhaustive]
    ArgumentError { message: String },

    /// An error occurred while the [`Client`](../struct.Client.html) attempted to authenticate a
    /// connection.
    #[error("{message}")]
    #[non_exhaustive]
    AuthenticationError { message: String },

    /// Wrapper around `bson::de::Error`.
    #[error("{0}")]
    BsonDecode(crate::bson::de::Error),

    /// Wrapper around `bson::ser::Error`.
    #[error("{0}")]
    BsonEncode(crate::bson::ser::Error),

    /// An error occurred when trying to execute a write operation consisting of multiple writes.
    #[error("An error occurred when trying to execute a write operation: {0:?}")]
    BulkWriteError(BulkWriteFailure),

    /// The server returned an error to an attempted operation.
    #[error("Command failed {0}")]
    CommandError(CommandError),

    // `trust_dns` does not implement the `Error` trait on their errors, so we have to manually
    // implement `From` rather than using the `source annotation.
    /// Wrapper around `trust_dns_resolver::error::ResolveError`.
    #[error("{0}")]
    DnsResolve(trust_dns_resolver::error::ResolveError),

    #[error("Internal error: {message}")]
    #[non_exhaustive]
    InternalError { message: String },

    /// Wrapper around `webpki::InvalidDNSNameError`.
    #[error("{0}")]
    InvalidDnsName(#[from] webpki::InvalidDNSNameError),

    /// A hostname could not be parsed.
    #[error("Unable to parse hostname: {hostname}")]
    #[non_exhaustive]
    InvalidHostname { hostname: String },

    /// Wrapper around [`std::io::Error`](https://doc.rust-lang.org/std/io/struct.Error.html).
    #[error("{0}")]
    Io(Arc<std::io::Error>),

    #[error("No DNS results for domain {0}")]
    NoDnsResults(StreamAddress),

    /// A database operation failed to send or receive a reply.
    #[error("A database operation failed to send or receive a reply: {message}")]
    #[non_exhaustive]
    OperationError { message: String },

    /// Data from a file could not be parsed.
    #[error("Unable to parse {data_type} data from {file_path}")]
    #[non_exhaustive]
    ParseError {
        data_type: String,
        file_path: String,
    },

    /// The connection pool for a server was cleared during operation execution due to
    /// a concurrent error, causing the operation to fail.
    #[error("{message}")]
    #[non_exhaustive]
    ConnectionPoolClearedError { message: String },

    /// The server returned an invalid reply to a database operation.
    #[error("The server returned an invalid reply to a database operation: {message}")]
    #[non_exhaustive]
    ResponseError { message: String },

    /// The Client was not able to select a server for the operation.
    #[error("{message}")]
    #[non_exhaustive]
    ServerSelectionError { message: String },

    /// An error occurred during SRV record lookup.
    #[error("An error occurred during SRV record lookup: {message}")]
    #[non_exhaustive]
    SrvLookupError { message: String },

    /// The Client does not support sessions.
    #[error("Attempted to start a session on a deployment that does not support sessions")]
    SessionsNotSupported,

    #[error("{0}")]
    RustlsConfig(#[from] rustls::TLSError),

    /// An error occurred during TXT record lookup
    #[error("An error occurred during TXT record lookup: {message}")]
    #[non_exhaustive]
    TxtLookupError { message: String },

    /// The Client timed out while checking out a connection from connection pool.
    #[error(
        "Timed out while checking out a connection from connection pool with address {address}"
    )]
    #[non_exhaustive]
    WaitQueueTimeoutError { address: StreamAddress },

    /// An error occurred when trying to execute a write operation
    #[error("An error occurred when trying to execute a write operation: {0:?}")]
    WriteError(WriteFailure),
}

impl From<trust_dns_resolver::error::ResolveError> for ErrorKind {
    fn from(error: trust_dns_resolver::error::ResolveError) -> Self {
        Self::DnsResolve(error)
    }
}

impl ErrorKind {
    pub(crate) fn is_non_timeout_network_error(&self) -> bool {
        matches!(self, ErrorKind::Io(ref io_err) if io_err.kind() != std::io::ErrorKind::TimedOut)
    }

    pub(crate) fn is_network_error(&self) -> bool {
        matches!(
            self,
            ErrorKind::Io(..) | ErrorKind::ConnectionPoolClearedError { .. }
        )
    }

    /// Gets the code from this error for performing SDAM updates, if applicable.
    /// Any codes contained in WriteErrors are ignored.
    pub(crate) fn code(&self) -> Option<i32> {
        match self {
            ErrorKind::CommandError(command_error) => {
                Some(command_error.code)
            },
            // According to SDAM spec, write concern error codes MUST also be checked, and writeError codes
            // MUST NOT be checked.
            ErrorKind::BulkWriteError(BulkWriteFailure { write_concern_error: Some(wc_error), .. }) => {
                Some(wc_error.code)
            }
            ErrorKind::WriteError(WriteFailure::WriteConcernError(wc_error)) => Some(wc_error.code),
            _ => None
        }
    }

    /// Gets the server's message for this error, if applicable, for use in testing.
    /// If this error is a BulkWriteError, the messages are concatenated.
    #[cfg(test)]
    pub(crate) fn server_message(&self) -> Option<String> {
        match self {
            ErrorKind::CommandError(command_error) => {
                Some(command_error.message.clone())
            },
            // since this is used primarily for errorMessageContains assertions in the unified runner, we just
            // concatenate all the relevant server messages into one for bulk errors.
            ErrorKind::BulkWriteError(BulkWriteFailure { write_concern_error, write_errors }) => {
                let mut msg = "".to_string();
                if let Some(wc_error) = write_concern_error {
                    msg.push_str(wc_error.message.as_str());
                }
                if let Some(write_errors) = write_errors {
                    for we in write_errors {
                        msg.push_str(we.message.as_str());
                    }
                }
                Some(msg)
            }
            ErrorKind::WriteError(WriteFailure::WriteConcernError(wc_error)) => Some(wc_error.message.clone()),
            ErrorKind::WriteError(WriteFailure::WriteError(write_error)) => Some(write_error.message.clone()),
            _ => None
        }
    }

    /// Gets the code name from this error, if applicable.
    #[cfg(test)]
    pub(crate) fn code_name(&self) -> Option<&str> {
        match self {
            ErrorKind::CommandError(ref cmd_err) => Some(cmd_err.code_name.as_str()),
            ErrorKind::WriteError(ref failure) => match failure {
                WriteFailure::WriteConcernError(ref wce) => Some(wce.code_name.as_str()),
                WriteFailure::WriteError(ref we) => we.code_name.as_deref(),
            },
            ErrorKind::BulkWriteError(ref bwe) => bwe
                .write_concern_error
                .as_ref()
                .map(|wce| wce.code_name.as_str()),
            _ => None,
        }
    }

    /// If this error corresponds to a "not master" error as per the SDAM spec.
    pub(crate) fn is_not_master(&self) -> bool {
        self.code().map(|code| NOTMASTER_CODES.contains(&code)).unwrap_or(false)
    }

    /// If this error corresponds to a "node is recovering" error as per the SDAM spec.
    pub(crate) fn is_recovering(&self) -> bool {
        self.code()
            .map(|code| RECOVERING_CODES.contains(&code))
            .unwrap_or(false)
    }

    /// If this error corresponds to a "node is shutting down" error as per the SDAM spec.
    pub(crate) fn is_shutting_down(&self) -> bool {
        self.code()
            .map(|code| SHUTTING_DOWN_CODES.contains(&code))
            .unwrap_or(false)
    }
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
    #[serde(rename = "codeName", default)]
    pub code_name: String,

    /// A description of the error that occurred.
    #[serde(rename = "errmsg")]
    pub message: String,

    /// A document identifying the write concern setting related to the error.
    #[serde(rename = "errInfo")]
    pub details: Option<Document>,

    /// The error labels that the server returned.
    #[serde(rename = "errorLabels", default)]
    pub labels: Vec<String>,
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
    #[serde(default)]
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
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
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
    /// An error that occurred due to not being able to satisfy a write concern.
    WriteConcernError(WriteConcernError),

    /// An error that occurred during a write operation that wasn't due to being unable to satisfy
    /// a write concern.
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
    match error.kind {
        ErrorKind::BulkWriteError(ref bulk_failure) => {
            match WriteFailure::from_bulk_failure(bulk_failure.clone()) {
                Ok(failure) => ErrorKind::WriteError(failure).into(),
                Err(e) => e,
            }
        }
        _ => error,
    }
}
