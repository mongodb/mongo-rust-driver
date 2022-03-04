//! Contains the `Error` and `Result` types that `mongodb` uses.

use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Debug},
    sync::Arc,
};

use bson::Bson;
use serde::Deserialize;
use thiserror::Error;

use crate::{bson::Document, options::ServerAddress};

const RECOVERING_CODES: [i32; 5] = [11600, 11602, 13436, 189, 91];
const NOTMASTER_CODES: [i32; 3] = [10107, 13435, 10058];
const SHUTTING_DOWN_CODES: [i32; 2] = [11600, 91];
const RETRYABLE_READ_CODES: [i32; 11] =
    [11600, 11602, 10107, 13435, 13436, 189, 91, 7, 6, 89, 9001];
const RETRYABLE_WRITE_CODES: [i32; 12] = [
    11600, 11602, 10107, 13435, 13436, 189, 91, 7, 6, 89, 9001, 262,
];
const UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL_CODES: [i32; 3] = [50, 64, 91];

/// Retryable write error label. This label will be added to an error when the error is
/// write-retryable.
pub const RETRYABLE_WRITE_ERROR: &str = "RetryableWriteError";
/// Transient transaction error label. This label will be added to a network error or server
/// selection error that occurs during a transaction.
pub const TRANSIENT_TRANSACTION_ERROR: &str = "TransientTransactionError";
/// Unknown transaction commit result error label. This label will be added to a server selection
/// error, network error, write-retryable error, MaxTimeMSExpired error, or write concern
/// failed/timeout during a commitTransaction.
pub const UNKNOWN_TRANSACTION_COMMIT_RESULT: &str = "UnknownTransactionCommitResult";

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
    pub kind: Box<ErrorKind>,
    labels: HashSet<String>,
    pub(crate) wire_version: Option<i32>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, labels: Option<impl IntoIterator<Item = String>>) -> Self {
        let mut labels: HashSet<String> = labels
            .map(|labels| labels.into_iter().collect())
            .unwrap_or_default();
        if let Some(wc) = kind.get_write_concern_error() {
            labels.extend(wc.labels.clone());
        }
        Self {
            kind: Box::new(kind),
            labels,
            wire_version: None,
        }
    }

    pub(crate) fn pool_cleared_error(address: &ServerAddress, cause: &Error) -> Self {
        ErrorKind::ConnectionPoolCleared {
            message: format!(
                "Connection pool for {} cleared because another operation failed with: {}",
                address, cause
            ),
        }
        .into()
    }

    /// Creates an `AuthenticationError` for the given mechanism with the provided reason.
    pub(crate) fn authentication_error(mechanism_name: &str, reason: &str) -> Self {
        ErrorKind::Authentication {
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

    pub(crate) fn internal(message: impl Into<String>) -> Error {
        ErrorKind::Internal {
            message: message.into(),
        }
        .into()
    }

    pub(crate) fn invalid_argument(message: impl Into<String>) -> Error {
        ErrorKind::InvalidArgument {
            message: message.into(),
        }
        .into()
    }

    pub(crate) fn is_state_change_error(&self) -> bool {
        self.is_recovering() || self.is_not_master()
    }

    pub(crate) fn is_auth_error(&self) -> bool {
        matches!(self.kind.as_ref(), ErrorKind::Authentication { .. })
    }

    pub(crate) fn is_command_error(&self) -> bool {
        matches!(self.kind.as_ref(), ErrorKind::Command(_))
    }

    pub(crate) fn is_network_timeout(&self) -> bool {
        matches!(self.kind.as_ref(), ErrorKind::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::TimedOut)
    }

    /// Whether this error is an "ns not found" error or not.
    pub(crate) fn is_ns_not_found(&self) -> bool {
        matches!(self.kind.as_ref(), ErrorKind::Command(ref err) if err.code == 26)
    }

    pub(crate) fn is_server_selection_error(&self) -> bool {
        matches!(self.kind.as_ref(), ErrorKind::ServerSelection { .. })
    }

    /// Whether a read operation should be retried if this error occurs.
    pub(crate) fn is_read_retryable(&self) -> bool {
        if self.is_network_error() {
            return true;
        }
        match self.code() {
            Some(code) => RETRYABLE_READ_CODES.contains(&code),
            None => false,
        }
    }

    pub(crate) fn is_write_retryable(&self) -> bool {
        self.contains_label(RETRYABLE_WRITE_ERROR)
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
        match &self.code() {
            Some(code) => RETRYABLE_WRITE_CODES.contains(code),
            None => false,
        }
    }

    pub(crate) fn should_add_unknown_transaction_commit_result_label(&self) -> bool {
        if self.contains_label(TRANSIENT_TRANSACTION_ERROR) {
            return false;
        }
        if self.is_network_error() || self.is_server_selection_error() || self.is_write_retryable()
        {
            return true;
        }
        match self.code() {
            Some(code) => UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL_CODES.contains(&code),
            None => false,
        }
    }

    /// Whether an error originated from the server.
    pub(crate) fn is_server_error(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            ErrorKind::Authentication { .. }
                | ErrorKind::BulkWrite(_)
                | ErrorKind::Command(_)
                | ErrorKind::Write(_)
        )
    }

    /// Returns the labels for this error.
    pub fn labels(&self) -> &HashSet<String> {
        &self.labels
    }

    /// Whether this error contains the specified label.
    pub fn contains_label<T: AsRef<str>>(&self, label: T) -> bool {
        self.labels().contains(label.as_ref())
    }

    /// Adds the given label to this error.
    pub(crate) fn add_label<T: AsRef<str>>(&mut self, label: T) {
        let label = label.as_ref().to_string();
        self.labels.insert(label);
    }

    pub(crate) fn from_resolve_error(error: trust_dns_resolver::error::ResolveError) -> Self {
        ErrorKind::DnsResolve {
            message: error.to_string(),
        }
        .into()
    }

    pub(crate) fn is_non_timeout_network_error(&self) -> bool {
        matches!(self.kind.as_ref(), ErrorKind::Io(ref io_err) if io_err.kind() != std::io::ErrorKind::TimedOut)
    }

    pub(crate) fn is_network_error(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            ErrorKind::Io(..) | ErrorKind::ConnectionPoolCleared { .. }
        )
    }

    /// Gets the code from this error for performing SDAM updates, if applicable.
    /// Any codes contained in WriteErrors are ignored.
    pub(crate) fn code(&self) -> Option<i32> {
        match self.kind.as_ref() {
            ErrorKind::Command(command_error) => Some(command_error.code),
            // According to SDAM spec, write concern error codes MUST also be checked, and
            // writeError codes MUST NOT be checked.
            ErrorKind::BulkWrite(BulkWriteFailure {
                write_concern_error: Some(wc_error),
                ..
            }) => Some(wc_error.code),
            ErrorKind::Write(WriteFailure::WriteConcernError(wc_error)) => Some(wc_error.code),
            _ => None,
        }
    }

    /// Gets the message for this error, if applicable, for use in testing.
    /// If this error is a BulkWriteError, the messages are concatenated.
    #[cfg(test)]
    pub(crate) fn message(&self) -> Option<String> {
        match self.kind.as_ref() {
            ErrorKind::Command(command_error) => Some(command_error.message.clone()),
            // since this is used primarily for errorMessageContains assertions in the unified
            // runner, we just concatenate all the relevant server messages into one for
            // bulk errors.
            ErrorKind::BulkWrite(BulkWriteFailure {
                write_concern_error,
                write_errors,
                inserted_ids: _,
            }) => {
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
            ErrorKind::Write(WriteFailure::WriteConcernError(wc_error)) => {
                Some(wc_error.message.clone())
            }
            ErrorKind::Write(WriteFailure::WriteError(write_error)) => {
                Some(write_error.message.clone())
            }
            ErrorKind::Transaction { message } => Some(message.clone()),
            ErrorKind::IncompatibleServer { message } => Some(message.clone()),
            _ => None,
        }
    }

    /// Gets the code name from this error, if applicable.
    #[cfg(test)]
    pub(crate) fn code_name(&self) -> Option<&str> {
        match self.kind.as_ref() {
            ErrorKind::Command(ref cmd_err) => Some(cmd_err.code_name.as_str()),
            ErrorKind::Write(ref failure) => match failure {
                WriteFailure::WriteConcernError(ref wce) => Some(wce.code_name.as_str()),
                WriteFailure::WriteError(ref we) => we.code_name.as_deref(),
            },
            ErrorKind::BulkWrite(ref bwe) => bwe
                .write_concern_error
                .as_ref()
                .map(|wce| wce.code_name.as_str()),
            _ => None,
        }
    }

    /// If this error corresponds to a "not master" error as per the SDAM spec.
    pub(crate) fn is_not_master(&self) -> bool {
        self.code()
            .map(|code| NOTMASTER_CODES.contains(&code))
            .unwrap_or(false)
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

    pub(crate) fn is_pool_cleared(&self) -> bool {
        matches!(self.kind.as_ref(), ErrorKind::ConnectionPoolCleared { .. })
    }

    /// If this error is resumable as per the change streams spec.
    pub(crate) fn is_resumable(&self) -> bool {
        if !self.is_server_error() {
            return true;
        }
        let code = self.code();
        if code == Some(43) {
            return true;
        }
        if matches!(self.wire_version, Some(v) if v >= 9)
            && self.contains_label("ResumableChangeStreamError")
        {
            return true;
        }
        if let (Some(code), true) = (code, matches!(self.wire_version, Some(v) if v < 9)) {
            if [
                6, 7, 89, 91, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436, 63, 150, 13388,
                234, 133,
            ]
            .iter()
            .any(|c| *c == code)
            {
                return true;
            }
        }
        false
    }
}

impl<E> From<E> for Error
where
    ErrorKind: From<E>,
{
    fn from(err: E) -> Self {
        Error::new(err.into(), None::<Option<String>>)
    }
}

impl From<bson::de::Error> for ErrorKind {
    fn from(err: bson::de::Error) -> Self {
        Self::BsonDeserialization(err)
    }
}

impl From<bson::ser::Error> for ErrorKind {
    fn from(err: bson::ser::Error) -> Self {
        Self::BsonSerialization(err)
    }
}

impl From<bson::raw::Error> for ErrorKind {
    fn from(err: bson::raw::Error) -> Self {
        Self::InvalidResponse {
            message: err.to_string(),
        }
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

#[cfg(feature = "openssl-tls")]
impl From<openssl::error::ErrorStack> for ErrorKind {
    fn from(err: openssl::error::ErrorStack) -> Self {
        Self::OpenSSL(err.into())
    }
}

#[cfg(feature = "openssl-tls")]
impl From<openssl::ssl::Error> for ErrorKind {
    fn from(err: openssl::ssl::Error) -> Self {
        Self::OpenSSL(err)
    }
}

/// The types of errors that can occur.
#[allow(missing_docs)]
#[derive(Clone, Debug, Error)]
#[non_exhaustive]
pub enum ErrorKind {
    /// An invalid argument was provided.
    #[error("An invalid argument was provided: {message}")]
    #[non_exhaustive]
    InvalidArgument { message: String },

    /// An error occurred while the [`Client`](../struct.Client.html) attempted to authenticate a
    /// connection.
    #[error("{message}")]
    #[non_exhaustive]
    Authentication { message: String },

    /// Wrapper around `bson::de::Error`.
    #[error("{0}")]
    BsonDeserialization(crate::bson::de::Error),

    /// Wrapper around `bson::ser::Error`.
    #[error("{0}")]
    BsonSerialization(crate::bson::ser::Error),

    /// An error occurred when trying to execute a write operation consisting of multiple writes.
    #[error("An error occurred when trying to execute a write operation: {0:?}")]
    BulkWrite(BulkWriteFailure),

    /// The server returned an error to an attempted operation.
    #[error("Command failed {0}")]
    Command(CommandError),

    /// An error occurred during DNS resolution.
    #[error("An error occurred during DNS resolution: {message}")]
    #[non_exhaustive]
    DnsResolve { message: String },

    #[error("Internal error: {message}")]
    #[non_exhaustive]
    Internal { message: String },

    /// Wrapper around [`std::io::Error`](https://doc.rust-lang.org/std/io/struct.Error.html).
    #[error("{0}")]
    Io(Arc<std::io::Error>),

    /// The connection pool for a server was cleared during operation execution due to
    /// a concurrent error, causing the operation to fail.
    #[error("{message}")]
    #[non_exhaustive]
    ConnectionPoolCleared { message: String },

    /// The server returned an invalid reply to a database operation.
    #[error("The server returned an invalid reply to a database operation: {message}")]
    #[non_exhaustive]
    InvalidResponse { message: String },

    /// The Client was not able to select a server for the operation.
    #[error("{message}")]
    #[non_exhaustive]
    ServerSelection { message: String },

    /// The Client does not support sessions.
    #[error("Attempted to start a session on a deployment that does not support sessions")]
    SessionsNotSupported,

    #[error("{message}")]
    #[non_exhaustive]
    InvalidTlsConfig { message: String },

    /// An error occurred when trying to execute a write operation.
    #[error("An error occurred when trying to execute a write operation: {0:?}")]
    Write(WriteFailure),

    /// An error occurred during a transaction.
    #[error("{message}")]
    #[non_exhaustive]
    Transaction { message: String },

    /// The server does not support the operation.
    #[error("The server does not support a database operation: {message}")]
    #[non_exhaustive]
    IncompatibleServer { message: String },

    /// No resume token was present in a change stream document.
    #[error("Cannot provide resume functionality when the resume token is missing")]
    MissingResumeToken,

    /// Wrapper around [`openssl::ssl::Error`].
    #[cfg(feature = "openssl-tls")]
    #[error("{0}")]
    OpenSSL(openssl::ssl::Error)
}

impl ErrorKind {
    // This is only used as part of a workaround to Atlas Proxy not returning
    // toplevel error labels.
    // TODO CLOUDP-105256 Remove this when Atlas Proxy error label behavior is fixed.
    fn get_write_concern_error(&self) -> Option<&WriteConcernError> {
        match self {
            ErrorKind::BulkWrite(BulkWriteFailure {
                write_concern_error,
                ..
            }) => write_concern_error.as_ref(),
            ErrorKind::Write(WriteFailure::WriteConcernError(err)) => Some(err),
            _ => None,
        }
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

    /// Labels categorizing the error.
    // TODO CLOUDP-105256 Remove this when the Atlas Proxy properly returns
    // error labels at the top level.
    #[serde(rename = "errorLabels", default)]
    pub(crate) labels: Vec<String>,
}

/// An error that occurred during a write operation that wasn't due to being unable to satisfy a
/// write concern.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[non_exhaustive]
pub struct WriteError {
    /// Identifies the type of write error.
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

    /// A document providing more information about the write error (e.g. details
    /// pertaining to document validation).
    #[serde(rename = "errInfo")]
    pub details: Option<Document>,
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

    /// A document providing more information about the write error (e.g. details
    /// pertaining to document validation).
    #[serde(rename = "errInfo")]
    pub details: Option<Document>,
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

    #[serde(skip)]
    pub(crate) inserted_ids: HashMap<usize, Bson>,
}

impl BulkWriteFailure {
    pub(crate) fn new() -> Self {
        BulkWriteFailure {
            write_errors: None,
            write_concern_error: None,
            inserted_ids: Default::default(),
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
                details: bulk_write_error.details,
            };
            Ok(WriteFailure::WriteError(write_error))
        } else if let Some(wc_error) = bulk.write_concern_error {
            Ok(WriteFailure::WriteConcernError(wc_error))
        } else {
            Err(ErrorKind::InvalidResponse {
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
        ErrorKind::BulkWrite(bulk_failure) => match WriteFailure::from_bulk_failure(bulk_failure) {
            Ok(failure) => Error::new(ErrorKind::Write(failure), Some(error.labels)),
            Err(e) => e,
        },
        _ => error,
    }
}

/// Flag a load-balanced mode mismatch.  With debug assertions enabled, it will panic; otherwise,
/// it will return the argument, or `()` if none is given.
// TODO RUST-230 Log an error in the non-panic branch for mode mismatch.
macro_rules! load_balanced_mode_mismatch {
    ($e:expr) => {{
        if cfg!(debug_assertions) {
            panic!("load-balanced mode mismatch")
        }
        return $e;
    }};
    () => {
        load_balanced_mode_mismatch!(())
    };
}

pub(crate) use load_balanced_mode_mismatch;
