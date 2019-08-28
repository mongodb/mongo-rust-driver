use serde::Deserialize;

pub use crate::cmap::options::ConnectionPoolOptions;

/// Event emitted when a connection pool is created.
#[derive(Debug, Deserialize, PartialEq)]
pub struct PoolCreatedEvent {
    /// The address of the server that the pool's connections will connect to.
    #[serde(skip)]
    pub address: String,

    /// The options used for the pool.
    pub options: Option<ConnectionPoolOptions>,
}

/// Event emitted when a connection pool is cleared.
#[derive(Debug, Deserialize, PartialEq)]
pub struct PoolClearedEvent {
    /// The address of the server that the pool's connections will connect to.
    #[serde(skip)]
    pub address: String,
}

/// Event emitted when a connection pool is closed.
#[derive(Debug, Deserialize, PartialEq)]
pub struct PoolClosedEvent {
    /// The address of the server that the pool's connections were connected to.
    #[serde(skip)]
    pub address: String,
}

/// Event emitted when a connection is created.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionCreatedEvent {
    /// The address of the server that the connection will connect to.
    #[serde(default)]
    pub address: String,

    /// The unique ID of the connection. This is not used for anything internally, but can be used
    /// to identify other events related to this connection.
    #[serde(default)]
    pub connection_id: u32,
}

/// Event emitted when a connection is ready to be used. This indicates that all the necessary
/// prerequisites for using a connection (handshake, authentication, etc.) have been completed.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionReadyEvent {
    /// The address of the server that the connection is connected to.
    #[serde(default)]
    pub address: String,

    /// The unique ID of the connection. This is not used for anything internally, but can be used
    /// to identify other events related to this connection.
    #[serde(default)]
    pub connection_id: u32,
}

/// Event emitted when a connection is closed.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionClosedEvent {
    /// The address of the server that the connection was connected to.
    #[serde(default)]
    pub address: String,

    /// The unique ID of the connection. This is not used for anything internally, but can be used
    /// to identify other events related to this connection.
    #[serde(default)]
    pub connection_id: u32,

    /// The reason that the connection was closed.
    pub reason: ConnectionClosedReason,
}

/// The reasons that a connection may be closed.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionClosedReason {
    /// The connection pool has been cleared since the connection was created.
    Stale,

    /// The connection has been available for longer than `max_idle_time` without being used.
    Idle,

    /// An error occurred while using the connection.
    Error,

    /// The pool that the connection belongs to has been closed.
    PoolClosed,
}

/// Event emitted when a thread begins checking out a connection to use for an operation.
#[derive(Debug, Deserialize, PartialEq)]
pub struct ConnectionCheckoutStartedEvent {
    /// The address of the server that the connection will connect to.
    #[serde(skip)]
    pub address: String,
}

/// Event emitted when a thread is unable to check out a connection.
#[derive(Debug, Deserialize, PartialEq)]
pub struct ConnectionCheckoutFailedEvent {
    /// The address of the server that the connection would have connected to.
    #[serde(skip)]
    pub address: String,

    /// The reason a connection was unable to be checked out.
    pub reason: ConnectionCheckoutFailedReason,
}

/// The reasons a connection may not be able to be checked out.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionCheckoutFailedReason {
    /// The `wait_queue_timeout` has elapsed while waiting for a connection to be available.
    Timeout,

    /// An error occurred while trying to establish a connection (e.g. during the handshake or
    /// authentication).
    ConnectionError,
}

/// Event emitted when a connection is successfully checked out.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionCheckedOutEvent {
    /// The address of the server that the connection will connect to.
    #[serde(skip)]
    pub address: String,

    /// The unique ID of the connection. This is not used for anything internally, but can be used
    /// to identify other events related to this connection.
    #[serde(default)]
    pub connection_id: u32,
}

/// Event emitted when a connection is checked back into a connection pool.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionCheckedInEvent {
    /// The address of the server that the connection was connected to.
    #[serde(skip)]
    pub address: String,

    /// The unique ID of the connection. This is not used for anything internally, but can be used
    /// to identify other events related to this connection.
    #[serde(default)]
    pub connection_id: u32,
}

/// Applications can implement this trait to specify custom logic to run on each CMAP event sent
/// by the driver.
///
/// ```rust
/// # use mongodb::{Client, event::cmap::{CmapEventHandler, ConnectionCheckoutFailedEvent}};
///
/// struct FailedCheckoutLogger {}
///
/// impl CmapEventHandler for FailedCheckoutLogger {
///     fn handle_connection_checkout_failed_event(&self, event: ConnectionCheckoutFailedEvent) {
///         eprintln!("Failed connection checkout: {:?}", event);
///     }
/// }
///
/// # fn main() {
/// // TODO: Construct client with event handler by using `ClientOptions`.
///
/// // Do things with the client, and failed checkout events will be logged to stderr.
/// # }
/// ```
pub trait CmapEventHandler: Send + Sync {
    /// A `Client` will call this method on each registered handler whenever a connection pool is
    /// created.
    fn handle_pool_created_event(&self, event: PoolCreatedEvent) {}

    /// A `Client` will call this method on each registered handler whenever a connection pool is
    /// cleared.
    fn handle_pool_cleared_event(&self, event: PoolClearedEvent) {}

    /// A `Client` will call this method on each registered handler whenever a connection pool is
    /// closed.
    fn handle_pool_closed_event(&self, event: PoolClosedEvent) {}

    /// A `Client` will call this method on each registered handler whenever a connection is
    /// created.
    fn handle_connection_created_event(&self, event: ConnectionCreatedEvent) {}

    /// A `Client` will call this method on each registered handler whenever a connection is ready
    /// to be used.
    fn handle_connection_ready_event(&self, event: ConnectionReadyEvent) {}

    /// A `Client` will call this method on each registered handler whenever a connection is closed.
    fn handle_connection_closed_event(&self, event: ConnectionClosedEvent) {}

    /// A `Client` will call this method on each registered handler whenever a thread begins
    /// checking out a connection to use for an operation.
    fn handle_connection_checkout_started_event(&self, event: ConnectionCheckoutStartedEvent) {}

    /// A `Client` will call this method on each registered handler whenever a thread is unable to
    /// check out a connection.
    fn handle_connection_checkout_failed_event(&self, event: ConnectionCheckoutFailedEvent) {}

    /// A `Client` will call this method on each registered handler whenever a connection is
    /// successfully checked out.
    fn handle_connection_checked_out_event(&self, event: ConnectionCheckedOutEvent) {}

    /// A `Client` will call this method on each registered handler whenever a connection is checked
    /// back into a connection pool.
    fn handle_connection_checked_in_event(&self, event: ConnectionCheckedInEvent) {}
}
