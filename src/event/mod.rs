mod command;

pub use command::*;

use crate::error::{CommandError, WriteFailure};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HandlerId(u32);

impl HandlerId {
    pub(crate) fn new(u: u32) -> Self {
        HandlerId(u)
    }
}

#[derive(Clone, Debug)]
pub enum Failure {
    Argument(String),
    Command(CommandError),
    Io(std::io::ErrorKind),
    OperationError(String),
    ResponseError(String),
    ServerSelectionError(String),
    WriteError(WriteFailure),
}
