pub mod cmap;
pub mod command;

use crate::error::{CommandError, WriteFailure};

/// In order to easily send events to multiple handlers, we need to be able to copy the events.
/// Certain events (e.g. `CommandFailedEvent`) need to contain information about an error that a
/// driver returned, but due to some external errors not implementing Clone (e.g. `std::io::Error`),
/// we define `Failure` as a type that contains all the same information as our error type but that
/// can be properly cloned and sent to each handler.
#[derive(Clone, Debug)]
pub enum Failure {
    Argument(String),
    Command(CommandError),
    Io(std::io::ErrorKind),
    OperationError(String),
    ResponseError(String),
    ServerSelectionError(String),
    WriteError(WriteFailure),

    // Error-chain uses a `__Nonexhaustive` variant that's hidden from the docs to force users to
    // match the wildcard pattern when checking the error kind, so we do the same thing here.
    #[doc(hidden)]
    __Nonexhaustive,
}
