use std::{fmt, sync::Arc};

use err_derive::Error;

use crate::options::StreamAddress;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Error)]
#[error(display = "{}", kind)]
pub struct Error {
    pub kind: Arc<ErrorKind>,
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

    #[error(display = "{}", _0)]
    BsonDecode(#[error(source)] bson::DecoderError),

    #[error(display = "{}", _0)]
    BsonEncode(#[error(source)] bson::EncoderError),

    #[error(display = "Command failed {}", inner)]
    CommandError { inner: CommandError },

    #[error(display = "{}", _0)]
    DnsName(#[error(source)] webpki::InvalidDNSNameError),

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

    #[error(
        display = "Timed out while checking out a connection from connection pool with address {}",
        address
    )]
    WaitQueueTimeoutError { address: StreamAddress },

    #[error(
        display = "An error occurred when trying to execute a write operation: {}",
        inner
    )]
    WriteError { inner: WriteFailure },
}

/// An error that occurred due to a database command failing.
#[derive(Clone, Debug)]
pub struct CommandError {
    code: u32,
    code_name: String,
    message: String,
    labels: Vec<String>,
}

impl fmt::Display for CommandError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "({}): {})", self.code_name, self.message)
    }
}

/// An error that occurred due to not being able to satisfy a write concern.
#[derive(Clone, Debug)]
pub struct WriteConcernError {
    /// Identifies the type of write concern error.
    pub code: i32,

    /// The name associated with the error code.
    pub code_name: String,

    /// A description of the error that occurred.
    pub message: String,
}

/// An error that occurred during a write operation that wasn't due to being unable to satisfy a
/// write concern.
#[derive(Clone, Debug)]
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

/// An error that occurred when trying to execute a write operation.
#[derive(Clone, Debug)]
pub enum WriteFailure {
    WriteConcernError(WriteConcernError),
    WriteError(WriteError),
}

impl fmt::Display for WriteFailure {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        unimplemented!()
    }
}
