//! Contains the `Error` and `Result` types that `mongodb` uses.

use std::{
    any::Any,
    collections::{HashMap, HashSet},
    fmt::{self, Debug},
    sync::Arc,
};

use bson::Bson;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{bson::Document, options::ServerAddress, sdam::TopologyVersion};

const RECOVERING_CODES: [i32; 5] = [11600, 11602, 13436, 189, 91];
const NOTWRITABLEPRIMARY_CODES: [i32; 3] = [10107, 13435, 10058];
const SHUTTING_DOWN_CODES: [i32; 2] = [11600, 91];
const RETRYABLE_READ_CODES: [i32; 13] = [
    11600, 11602, 10107, 13435, 13436, 189, 91, 7, 6, 89, 9001, 134, 262,
];
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
#[error("Kind: {kind}, labels: {labels:?}")]
#[non_exhaustive]
pub struct Error {
    /// The type of error that occurred.
    pub kind: Box<ErrorKind>,
    labels: HashSet<String>,
    pub(crate) wire_version: Option<i32>,
    #[source]
    pub(crate) source: Option<Box<Error>>,
}

impl Error {
    /// Create a new `Error` wrapping an arbitrary value.  Can be used to abort transactions in
    /// callbacks for [`ClientSession::with_transaction`](crate::ClientSession::with_transaction).
    pub fn custom(e: impl Any + Send + Sync) -> Self {
        Self::new(ErrorKind::Custom(Arc::new(e)), None::<Option<String>>)
    }

    /// Retrieve a reference to a value provided to `Error::custom`.  Returns `None` if this is not
    /// a custom error or if the payload types mismatch.
    pub fn get_custom<E: Any>(&self) -> Option<&E> {
        if let ErrorKind::Custom(c) = &*self.kind {
            c.downcast_ref()
        } else {
            None
        }
    }

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
            source: None,
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

    /// Construct a generic network timeout error.
    pub(crate) fn network_timeout() -> Error {
        ErrorKind::Io(Arc::new(std::io::ErrorKind::TimedOut.into())).into()
    }

    pub(crate) fn invalid_argument(message: impl Into<String>) -> Error {
        ErrorKind::InvalidArgument {
            message: message.into(),
        }
        .into()
    }

    pub(crate) fn is_state_change_error(&self) -> bool {
        self.is_recovering() || self.is_notwritableprimary()
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

    pub(crate) fn is_max_time_ms_expired_error(&self) -> bool {
        self.sdam_code() == Some(50)
    }

    /// Whether a read operation should be retried if this error occurs.
    pub(crate) fn is_read_retryable(&self) -> bool {
        if self.is_network_error() {
            return true;
        }
        match self.sdam_code() {
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
        match &self.sdam_code() {
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
        match self.sdam_code() {
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

    #[cfg(all(test, feature = "in-use-encryption-unstable"))]
    pub(crate) fn is_csfle_error(&self) -> bool {
        matches!(self.kind.as_ref(), ErrorKind::Encryption(..))
    }

    /// Gets the code from this error for performing SDAM updates, if applicable.
    /// Any codes contained in WriteErrors are ignored.
    pub(crate) fn sdam_code(&self) -> Option<i32> {
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
        .or_else(|| self.source.as_ref().and_then(|s| s.sdam_code()))
    }

    /// Gets the code from this error.
    #[allow(unused)]
    pub(crate) fn code(&self) -> Option<i32> {
        match self.kind.as_ref() {
            ErrorKind::Command(command_error) => Some(command_error.code),
            ErrorKind::BulkWrite(BulkWriteFailure {
                write_concern_error: Some(wc_error),
                ..
            }) => Some(wc_error.code),
            ErrorKind::Write(e) => Some(e.code()),
            _ => None,
        }
        .or_else(|| self.source.as_ref().and_then(|s| s.sdam_code()))
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
            ErrorKind::InvalidArgument { message } => Some(message.clone()),
            #[cfg(feature = "in-use-encryption-unstable")]
            ErrorKind::Encryption(err) => err.message.clone(),
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

    /// If this error corresponds to a "not writable primary" error as per the SDAM spec.
    pub(crate) fn is_notwritableprimary(&self) -> bool {
        self.sdam_code()
            .map(|code| NOTWRITABLEPRIMARY_CODES.contains(&code))
            .unwrap_or(false)
    }

    /// If this error corresponds to a "node is recovering" error as per the SDAM spec.
    pub(crate) fn is_recovering(&self) -> bool {
        self.sdam_code()
            .map(|code| RECOVERING_CODES.contains(&code))
            .unwrap_or(false)
    }

    /// If this error corresponds to a "node is shutting down" error as per the SDAM spec.
    pub(crate) fn is_shutting_down(&self) -> bool {
        self.sdam_code()
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
        let code = self.sdam_code();
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

    pub(crate) fn is_incompatible_server(&self) -> bool {
        matches!(self.kind.as_ref(), ErrorKind::IncompatibleServer { .. })
    }

    #[allow(unused)]
    pub(crate) fn is_invalid_argument(&self) -> bool {
        matches!(self.kind.as_ref(), ErrorKind::InvalidArgument { .. })
    }

    pub(crate) fn with_source<E: Into<Option<Error>>>(mut self, source: E) -> Self {
        self.source = source.into().map(Box::new);
        self
    }

    pub(crate) fn topology_version(&self) -> Option<TopologyVersion> {
        match self.kind.as_ref() {
            ErrorKind::Command(c) => c.topology_version,
            _ => None,
        }
    }

    /// Per the CLAM spec, for sensitive commands we must redact everything besides the
    /// error labels, error code, and error code name from errors received in response to
    /// sensitive commands. Currently, the only field besides those that we expose is the
    /// error message.
    pub(crate) fn redact(&mut self) {
        // This is intentionally written without a catch-all branch so that if new error
        // kinds are added we remember to reason about whether they need to be redacted.
        match *self.kind {
            ErrorKind::BulkWrite(ref mut bwe) => {
                if let Some(ref mut wes) = bwe.write_errors {
                    for we in wes {
                        we.redact();
                    }
                }
                if let Some(ref mut wce) = bwe.write_concern_error {
                    wce.redact();
                }
            }
            ErrorKind::Command(ref mut command_error) => {
                command_error.redact();
            }
            ErrorKind::Write(ref mut write_error) => match write_error {
                WriteFailure::WriteConcernError(wce) => {
                    wce.redact();
                }
                WriteFailure::WriteError(we) => {
                    we.redact();
                }
            },
            ErrorKind::InvalidArgument { .. }
            | ErrorKind::BsonDeserialization(_)
            | ErrorKind::BsonSerialization(_)
            | ErrorKind::DnsResolve { .. }
            | ErrorKind::Io(_)
            | ErrorKind::Internal { .. }
            | ErrorKind::ConnectionPoolCleared { .. }
            | ErrorKind::InvalidResponse { .. }
            | ErrorKind::ServerSelection { .. }
            | ErrorKind::SessionsNotSupported
            | ErrorKind::InvalidTlsConfig { .. }
            | ErrorKind::Transaction { .. }
            | ErrorKind::IncompatibleServer { .. }
            | ErrorKind::MissingResumeToken
            | ErrorKind::Authentication { .. }
            | ErrorKind::Custom(_)
            | ErrorKind::Shutdown
            | ErrorKind::GridFs(_) => {}
            #[cfg(feature = "in-use-encryption-unstable")]
            ErrorKind::Encryption(_) => {}
        }
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

#[cfg(feature = "in-use-encryption-unstable")]
impl From<mongocrypt::error::Error> for ErrorKind {
    fn from(err: mongocrypt::error::Error) -> Self {
        Self::Encryption(err)
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
    #[error("Command failed: {0}")]
    // note that if this Display impl changes, COMMAND_ERROR_REGEX in the unified runner matching
    // logic will need to be updated.
    Command(CommandError),

    /// An error occurred during DNS resolution.
    #[error("An error occurred during DNS resolution: {message}")]
    #[non_exhaustive]
    DnsResolve { message: String },

    /// A GridFS error occurred.
    #[error("{0:?}")]
    #[non_exhaustive]
    GridFs(GridFsErrorKind),

    #[error("Internal error: {message}")]
    #[non_exhaustive]
    Internal { message: String },

    /// Wrapper around [`std::io::Error`](https://doc.rust-lang.org/std/io/struct.Error.html).
    #[error("I/O error: {0}")]
    // note that if this Display impl changes, IO_ERROR_REGEX in the unified runner matching logic
    // will need to be updated.
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

    /// An error occurred during encryption or decryption.
    #[cfg(feature = "in-use-encryption-unstable")]
    #[error("An error occurred during client-side encryption: {0}")]
    Encryption(mongocrypt::error::Error),

    /// A custom value produced by user code.
    #[error("Custom user error")]
    Custom(Arc<dyn Any + Send + Sync>),

    /// A method was called on a client that was shut down.
    #[error("Client has been shut down")]
    Shutdown,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CommandError {
    /// Identifies the type of error.
    pub code: i32,

    /// The name associated with the error code.
    #[serde(rename = "codeName", default)]
    pub code_name: String,

    /// A description of the error that occurred.
    #[serde(rename = "errmsg", default = "String::new")]
    pub message: String,

    /// The topology version reported by the server in the error response.
    #[serde(rename = "topologyVersion")]
    pub(crate) topology_version: Option<TopologyVersion>,
}

impl CommandError {
    // If any new fields are added to CommandError, this implementation must be updated to
    // redact them per the CLAM spec.
    fn redact(&mut self) {
        self.message = "REDACTED".to_string();
    }
}

impl fmt::Display for CommandError {
    // note that if this Display impl changes, COMMAND_ERROR_REGEX in the unified runner matching
    // logic will need to be updated.
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "Error code {} ({}): {}",
            self.code, self.code_name, self.message
        )
    }
}

/// An error that occurred due to not being able to satisfy a write concern.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct WriteConcernError {
    /// Identifies the type of write concern error.
    pub code: i32,

    /// The name associated with the error code.
    #[serde(rename = "codeName", default)]
    pub code_name: String,

    /// A description of the error that occurred.
    #[serde(rename = "errmsg", default = "String::new")]
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

impl WriteConcernError {
    // If any new fields are added to WriteConcernError, this implementation must be updated to
    // redact them per the CLAM spec.
    fn redact(&mut self) {
        self.message = "REDACTED".to_string();
        self.details = None;
    }
}

/// An error that occurred during a write operation that wasn't due to being unable to satisfy a
/// write concern.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    #[serde(rename = "errmsg", default = "String::new")]
    pub message: String,

    /// A document providing more information about the write error (e.g. details
    /// pertaining to document validation).
    #[serde(rename = "errInfo")]
    pub details: Option<Document>,
}

impl WriteError {
    // If any new fields are added to WriteError, this implementation must be updated to redact them
    // per the CLAM spec.
    fn redact(&mut self) {
        self.message = "REDACTED".to_string();
        self.details = None;
    }
}

/// An error that occurred during a write operation consisting of multiple writes that wasn't due to
/// being unable to satisfy a write concern.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
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
    #[serde(rename = "errmsg", default = "String::new")]
    pub message: String,

    /// A document providing more information about the write error (e.g. details
    /// pertaining to document validation).
    #[serde(rename = "errInfo")]
    pub details: Option<Document>,
}

impl BulkWriteError {
    // If any new fields are added to BulkWriteError, this implementation must be updated to redact
    // them per the CLAM spec.
    fn redact(&mut self) {
        self.message = "REDACTED".to_string();
        self.details = None;
    }
}

/// The set of errors that occurred during a write operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
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

    pub(crate) fn code(&self) -> i32 {
        match self {
            Self::WriteConcernError(e) => e.code,
            Self::WriteError(e) => e.code,
        }
    }
}

/// An error that occurred during a GridFS operation.
#[derive(Clone, Debug)]
#[allow(missing_docs)]
#[non_exhaustive]
pub enum GridFsErrorKind {
    /// The file with the given identifier was not found.
    #[non_exhaustive]
    FileNotFound { identifier: GridFsFileIdentifier },

    /// The file with the given revision was not found.
    #[non_exhaustive]
    RevisionNotFound { revision: i32 },

    /// The chunk at index `n` was missing.
    #[non_exhaustive]
    MissingChunk { n: u32 },

    /// An operation was attempted on a [`GridFsUploadStream`](crate::gridfs::GridFsUploadStream)
    /// that has already been shut down.
    UploadStreamClosed,

    /// The chunk at index `n` was the incorrect size.
    #[non_exhaustive]
    WrongSizeChunk {
        actual_size: usize,
        expected_size: u32,
        n: u32,
    },

    /// An incorrect number of chunks was present for the file.
    #[non_exhaustive]
    WrongNumberOfChunks {
        actual_number: u32,
        expected_number: u32,
    },

    /// An error occurred when aborting a file upload.
    #[non_exhaustive]
    AbortError {
        /// The original error. Only present if the abort occurred because of an error during a
        /// GridFS operation.
        original_error: Option<Error>,

        /// The error that occurred when attempting to remove any orphaned chunks.
        delete_error: Error,
    },

    /// A close operation was attempted on a
    /// [`GridFsUploadStream`](crate::gridfs::GridFsUploadStream) while a write was still in
    /// progress.
    WriteInProgress,
}

/// An identifier for a file stored in a GridFS bucket.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum GridFsFileIdentifier {
    /// The name of the file. Not guaranteed to be unique.
    Filename(String),

    /// The file's unique [`Bson`] ID.
    Id(Bson),
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
