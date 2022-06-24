//! Contains the events and functionality for monitoring Server Discovery and Monitoring.

mod topology_description;

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    bson::{oid::ObjectId, Document},
    bson_util::serialize_error_as_string,
    error::Error,
    options::ServerAddress,
};

pub use crate::sdam::public::TopologyType;
pub use topology_description::TopologyDescription;

/// A description of the most up-to-date information known about a server. Further details can be
/// found in the [Server Discovery and Monitoring specification](https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst).
pub type ServerDescription = crate::sdam::public::ServerInfo<'static>;

/// Published when a server description changes.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ServerDescriptionChangedEvent {
    /// The address of the server.
    pub address: ServerAddress,

    /// The unique ID of the topology.
    pub topology_id: ObjectId,

    /// The server's previous description.
    pub previous_description: ServerDescription,

    /// The server's new description.
    pub new_description: ServerDescription,
}

impl ServerDescriptionChangedEvent {
    #[cfg(test)]
    pub(crate) fn is_marked_unknown_event(&self) -> bool {
        self.previous_description
            .description
            .server_type
            .is_available()
            && self.new_description.description.server_type == crate::ServerType::Unknown
    }
}

/// Published when a server is initialized.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ServerOpeningEvent {
    /// The address of the server.
    pub address: ServerAddress,

    /// The unique ID of the topology.
    #[serde(skip, default = "ObjectId::new")]
    pub topology_id: ObjectId,
}

/// Published when a server is closed.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ServerClosedEvent {
    /// The address of the server.
    pub address: ServerAddress,

    /// The unique ID of the topology.
    #[serde(skip, default = "ObjectId::new")]
    pub topology_id: ObjectId,
}

/// Published when a topology description changes.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TopologyDescriptionChangedEvent {
    /// The ID of the topology.
    pub topology_id: ObjectId,

    /// The topology's previous description.
    pub previous_description: TopologyDescription,

    /// The topology's new description.
    pub new_description: TopologyDescription,
}

/// Published when a topology is initialized.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TopologyOpeningEvent {
    /// The unique ID of the topology.
    #[serde(skip, default = "ObjectId::new")]
    pub topology_id: ObjectId,
}

/// Published when a topology is closed. Note that this event will not be published until the client
/// associated with the topology is dropped.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TopologyClosedEvent {
    /// The unique ID of the topology.
    #[serde(skip, default = "ObjectId::new")]
    pub topology_id: ObjectId,
}

/// Published when a server monitor's `hello` or legacy hello command is started.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ServerHeartbeatStartedEvent {
    /// The address of the server.
    pub server_address: ServerAddress,
    // TODO RUST-560 add awaited field
}

/// Published when a server monitor's `hello` or legacy hello command succeeds.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ServerHeartbeatSucceededEvent {
    /// The execution time of the event.
    pub duration: Duration,

    /// The reply to the `hello` or legacy hello command.
    pub reply: Document,

    /// The address of the server.
    pub server_address: ServerAddress,
    // TODO RUST-560 add awaited field
}

/// Published when a server monitor's `hello` or legacy hello command fails.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ServerHeartbeatFailedEvent {
    /// The execution time of the event.
    pub duration: Duration,

    /// The failure that occurred.
    #[serde(serialize_with = "serialize_error_as_string")]
    pub failure: Error,

    /// The address of the server.
    pub server_address: ServerAddress,
    // TODO RUST-560 add awaited field
}

#[derive(Clone, Debug)]
pub(crate) enum SdamEvent {
    ServerDescriptionChanged(Box<ServerDescriptionChangedEvent>),
    ServerOpening(ServerOpeningEvent),
    ServerClosed(ServerClosedEvent),
    TopologyDescriptionChanged(Box<TopologyDescriptionChangedEvent>),
    TopologyOpening(TopologyOpeningEvent),
    TopologyClosed(TopologyClosedEvent),
    ServerHeartbeatStarted(ServerHeartbeatStartedEvent),
    ServerHeartbeatSucceeded(ServerHeartbeatSucceededEvent),
    ServerHeartbeatFailed(ServerHeartbeatFailedEvent),
}

/// Applications can implement this trait to specify custom logic to run on each SDAM event sent
/// by the driver.
///
/// ```rust
/// # use std::sync::Arc;
/// #
/// # use mongodb::{
/// #     error::Result,
/// #     event::sdam::{
/// #         SdamEventHandler,
/// #         ServerHeartbeatFailedEvent,
/// #     },
/// #     options::ClientOptions,
/// # };
/// # #[cfg(any(feature = "sync", feature = "tokio-sync"))]
/// # use mongodb::sync::Client;
/// # #[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
/// # use mongodb::Client;
/// #
/// struct FailedHeartbeatLogger;
///
/// impl SdamEventHandler for FailedHeartbeatLogger {
///     fn handle_server_heartbeat_failed_event(&self, event: ServerHeartbeatFailedEvent) {
///         eprintln!("Failed server heartbeat: {:?}", event);
///     }
/// }
///
/// # fn do_stuff() -> Result<()> {
/// let handler: Arc<dyn SdamEventHandler> = Arc::new(FailedHeartbeatLogger);
/// let options = ClientOptions::builder()
///                   .sdam_event_handler(handler)
///                   .build();
/// let client = Client::with_options(options)?;
///
/// // Do things with the client, and failed server heartbeats will be logged to stderr.
/// # Ok(())
/// # }
/// ```
pub trait SdamEventHandler: Send + Sync {
    /// A [`Client`](../../struct.Client.html) will call this method on each registered handler when
    /// a server description changes.
    fn handle_server_description_changed_event(&self, _event: ServerDescriptionChangedEvent) {}

    /// A [`Client`](../../struct.Client.html) will call this method on each registered handler when
    /// a server is initialized.
    fn handle_server_opening_event(&self, _event: ServerOpeningEvent) {}

    /// A [`Client`](../../struct.Client.html) will call this method on each registered handler when
    /// a server is closed.
    fn handle_server_closed_event(&self, _event: ServerClosedEvent) {}

    /// A [`Client`](../../struct.Client.html) will call this method on each registered handler when
    /// its topology description changes.
    fn handle_topology_description_changed_event(&self, _event: TopologyDescriptionChangedEvent) {}

    /// A [`Client`](../../struct.Client.html) will call this method on each registered handler when
    /// its topology is initialized.
    fn handle_topology_opening_event(&self, _event: TopologyOpeningEvent) {}

    /// A [`Client`](../../struct.Client.html) will call this method on each registered handler when
    /// its topology closes.  Note that this method will not be called until the
    /// [`Client`](../../struct.Client.html) is dropped.
    fn handle_topology_closed_event(&self, _event: TopologyClosedEvent) {}

    /// A [`Client`](../../struct.Client.html) will call this method on each registered handler when
    /// a server heartbeat begins.
    fn handle_server_heartbeat_started_event(&self, _event: ServerHeartbeatStartedEvent) {}

    /// A [`Client`](../../struct.Client.html) will call this method on each registered handler when
    /// a server heartbeat succeeds.
    fn handle_server_heartbeat_succeeded_event(&self, _event: ServerHeartbeatSucceededEvent) {}

    /// A [`Client`](../../struct.Client.html) will call this method on each registered handler when
    /// a server heartbeat fails.
    fn handle_server_heartbeat_failed_event(&self, _event: ServerHeartbeatFailedEvent) {}
}

pub(crate) fn handle_sdam_event(handler: &dyn SdamEventHandler, event: SdamEvent) {
    match event {
        SdamEvent::ServerClosed(event) => handler.handle_server_closed_event(event),
        SdamEvent::ServerDescriptionChanged(e) => {
            handler.handle_server_description_changed_event(*e)
        }
        SdamEvent::ServerOpening(e) => handler.handle_server_opening_event(e),
        SdamEvent::TopologyDescriptionChanged(e) => {
            handler.handle_topology_description_changed_event(*e)
        }
        SdamEvent::TopologyOpening(e) => handler.handle_topology_opening_event(e),
        SdamEvent::TopologyClosed(e) => handler.handle_topology_closed_event(e),
        SdamEvent::ServerHeartbeatStarted(e) => handler.handle_server_heartbeat_started_event(e),
        SdamEvent::ServerHeartbeatSucceeded(e) => {
            handler.handle_server_heartbeat_succeeded_event(e)
        }
        SdamEvent::ServerHeartbeatFailed(e) => handler.handle_server_heartbeat_failed_event(e),
    }
}
