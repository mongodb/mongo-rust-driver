//! Contains the events and functionality to monitor the commands and responses that a `Client`
//! sends and receives from the server.

use std::time::Duration;

use serde::Serialize;

use crate::{
    bson::{oid::ObjectId, Document},
    bson_util::serialize_error_as_string,
    error::Error,
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
    #[serde(serialize_with = "serialize_error_as_string")]
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

/// Applications can implement this trait to specify custom logic to run on each command event sent
/// by the driver.
///
/// ```rust
/// # use std::sync::Arc;
/// #
/// # use mongodb::{
/// #     error::Result,
/// #     event::command::{
/// #         CommandEventHandler,
/// #         CommandFailedEvent
/// #     },
/// #     options::ClientOptions,
/// # };
/// # #[cfg(any(feature = "sync", feature = "tokio-sync"))]
/// # use mongodb::sync::Client;
/// # #[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
/// # use mongodb::Client;
/// #
/// struct FailedCommandLogger;
///
/// impl CommandEventHandler for FailedCommandLogger {
///     fn handle_command_failed_event(&self, event: CommandFailedEvent) {
///         eprintln!("Failed command: {:?}", event);
///     }
/// }
///
/// # fn do_stuff() -> Result<()> {
/// let handler: Arc<dyn CommandEventHandler> = Arc::new(FailedCommandLogger);
/// let options = ClientOptions::builder()
///                   .command_event_handler(handler)
///                   .build();
/// let client = Client::with_options(options)?;
///
/// // Do things with the client, and failed command events will be logged to stderr.
/// # Ok(())
/// # }
/// ```
pub trait CommandEventHandler: Send + Sync {
    /// A [`Client`](../../struct.Client.html) will call this method on each registered handler
    /// whenever a database command is initiated.
    fn handle_command_started_event(&self, _event: CommandStartedEvent) {}

    /// A [`Client`](../../struct.Client.html) will call this method on each registered handler
    /// whenever a database command successfully completes.
    fn handle_command_succeeded_event(&self, _event: CommandSucceededEvent) {}

    /// A [`Client`](../../struct.Client.html) will call this method on each registered handler
    /// whenever a database command fails to complete successfully.
    fn handle_command_failed_event(&self, _event: CommandFailedEvent) {}
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum CommandEvent {
    Started(CommandStartedEvent),
    Succeeded(CommandSucceededEvent),
    Failed(CommandFailedEvent),
}

/// Passes the specified event to the corresponding method on the provided handler.
pub(crate) fn handle_command_event(handler: &dyn CommandEventHandler, event: CommandEvent) {
    match event {
        CommandEvent::Started(event) => handler.handle_command_started_event(event),
        CommandEvent::Succeeded(event) => handler.handle_command_succeeded_event(event),
        CommandEvent::Failed(event) => handler.handle_command_failed_event(event),
    }
}
