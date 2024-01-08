//! Contains the events and functionality to monitor the commands and responses that a `Client`
//! sends and receives from the server.

use std::time::Duration;

use serde::Serialize;

use crate::{
    bson::{oid::ObjectId, Document},
    error::Error,
    serde_util,
};

pub use crate::cmap::ConnectionInfo;

/// An event that triggers when a database command is initiated.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CommandStartedEvent {
    /// The command being run.
    pub command: Document,

    /// The name of the database the command is being run against.
    pub db: String,

    /// The type of command being run, e.g. "find" or "hello".
    pub command_name: String,

    /// The driver-generated identifier for the request. Applications can use this to identify the
    /// corresponding event triggered by the completion of this command (i.e. either
    /// [`CommandSucceededEvent`](struct.CommandSucceededEvent.html) or
    /// [`CommandFailedEvent`](struct.CommandFailedEvent.html)).
    pub request_id: i32,

    /// Information about the connect the command will be run on.
    pub connection: ConnectionInfo,

    /// If the client connection is to a load balancer, the id of the selected backend.
    pub service_id: Option<ObjectId>,
}

/// An event that triggers when a database command completes without an error.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CommandSucceededEvent {
    /// The total execution time of the command (including the network round-trip).
    pub duration: Duration,

    /// The server's reply to the command.
    pub reply: Document,

    /// The type of command that was run, e.g. "find" or "hello".
    pub command_name: String,

    /// The driver-generated identifier for the request. Applications can use this to identify the
    /// corresponding [`CommandStartedEvent`](struct.CommandStartedEvent.html) that triggered
    /// earlier.
    pub request_id: i32,

    /// Information about the connect the command will be run on.
    pub connection: ConnectionInfo,

    /// If the client connection is to a load balancer, the id of the selected backend.
    pub service_id: Option<ObjectId>,
}

/// An event that triggers when a command failed to complete successfully.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CommandFailedEvent {
    /// The total execution time of the command (including the network round-trip).
    pub duration: Duration,

    /// The type of command that was run, e.g. "find" or "hello".
    pub command_name: String,

    /// The error that the driver returned due to the event failing.
    #[serde(serialize_with = "serde_util::serialize_error_as_string")]
    pub failure: Error,

    /// The driver-generated identifier for the request. Applications can use this to identify the
    /// corresponding [`CommandStartedEvent`](struct.CommandStartedEvent.html) that triggered
    /// earlier.
    pub request_id: i32,

    /// Information about the connect the command will be run on.
    pub connection: ConnectionInfo,

    /// If the client connection is to a load balancer, the id of the selected backend.
    pub service_id: Option<ObjectId>,
}

#[derive(Clone, Debug, Serialize)]
#[allow(missing_docs)]
#[serde(untagged)]
#[non_exhaustive]
pub enum CommandEvent {
    Started(CommandStartedEvent),
    Succeeded(CommandSucceededEvent),
    Failed(CommandFailedEvent),
}