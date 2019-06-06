use std::time::Duration;

use bson::Document;

use crate::{error::Error, pool::ConnectionInfo};

/// An event that triggers when a database command is initiated.
#[derive(Debug)]
pub struct CommandStartedEvent {
    /// The command being run.
    pub command: Document,

    /// The name of the database the command is being run against.
    pub db: String,

    /// The type of command being run, e.g. "find" or "isMaster".
    pub command_name: String,

    /// The driver-generated identifier for the request. Applications can use this to identify the
    /// corresponding event triggered by the completion of this command (i.e. either
    /// `CommandSucceededEvent` or `CommandFailedEvent`).
    pub request_id: i32,

    /// Information about the connect the command will be run on.
    pub connection: ConnectionInfo,
}

/// An event that triggers when a database command completes without an error.
#[derive(Debug)]
pub struct CommandSucceededEvent {
    /// The total execution time of the command (including the network round-trip).
    pub duration: Duration,

    /// The server's reply to the command.
    pub reply: Document,

    /// The type of command that was run, e.g. "find" or "isMaster".
    pub command_name: String,

    /// The driver-generated identifier for the request. Applications can use this to identify the
    /// corresponding `ComamndStartedEvent` that triggered earlier.
    pub request_id: i32,

    /// Information about the connect the command will be run on.
    pub connection_id: ConnectionInfo,
}

/// An event that triggers when a command failed to complete successfully.
#[derive(Debug)]
pub struct CommandFailedEvent {
    /// The total execution time of the command (including the network round-trip).
    pub duration: Duration,

    /// The type of command that was run, e.g. "find" or "isMaster".
    pub command_name: String,

    /// The error that the driver returned due to the event failing.
    pub failure: Error,

    /// The driver-generated identifier for the request. Applications can use this to identify the
    /// corresponding `ComamndStartedEvent` that triggered earlier.
    pub request_id: i32,

    /// Information about the connect the command will be run on.
    pub connection_id: ConnectionInfo,
}

/// Applications can implement this trait to specify custom logic to run on each command event sent
/// by the driver.
pub trait CommandEventHandler {
    /// A `Client` will call this method on each registered handler whenever a database command is
    /// initiated.
    fn handle_command_started_event(&mut self, event: CommandStartedEvent) {}

    /// A `Client` will call this method on each registered handler whenever a database command
    /// successfully completes.
    fn handle_command_succeeded_event(&mut self, event: CommandSucceededEvent) {}

    /// A `Client` will call this method on each registered handler whenever a database command
    /// fails to complete successfully.
    fn handle_command_failed_event(&mut self, event: CommandFailedEvent) {}
}
