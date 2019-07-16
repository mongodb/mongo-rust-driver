use serde::Deserialize;

pub use crate::cmap::options::ConnectionPoolOptions;

#[derive(Debug, Deserialize, PartialEq)]
pub struct PoolCreatedEvent {
    #[serde(default)]
    pub address: String,

    pub options: Option<ConnectionPoolOptions>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct PoolClearedEvent {
    #[serde(default)]
    pub address: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct PoolClosedEvent {
    #[serde(default)]
    pub address: String,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionCreatedEvent {
    #[serde(default)]
    pub address: String,

    #[serde(default)]
    pub connection_id: u32,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionReadyEvent {
    #[serde(default)]
    pub address: String,

    #[serde(default)]
    pub connection_id: u32,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionClosedEvent {
    #[serde(default)]
    pub address: String,

    #[serde(default)]
    pub connection_id: u32,

    pub reason: ConnectionClosedReason,
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum ConnectionClosedReason {
    #[serde(rename = "stale")]
    Stale,

    #[serde(rename = "idle")]
    Idle,

    #[serde(rename = "error")]
    Error,

    #[serde(rename = "poolClosed")]
    PoolClosed,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ConnectionCheckoutStartedEvent {
    #[serde(default)]
    pub address: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ConnectionCheckoutFailedEvent {
    #[serde(default)]
    address: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum ConnectionCheckoutFailedReason {
    #[serde(rename = "poolClosed")]
    PoolClosed,

    #[serde(rename = "timeout")]
    Timeout,

    #[serde(rename = "connectionError")]
    ConnectionError,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionCheckedOutEvent {
    #[serde(default)]
    pub address: String,

    #[serde(default)]
    pub connection_id: u32,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionCheckedInEvent {
    #[serde(default)]
    pub address: String,

    #[serde(default)]
    pub connection_id: u32,
}

pub trait CmapEventHandler: Send + Sync {
    fn handle_pool_created_event(&self, event: PoolCreatedEvent) {}
    fn handle_pool_cleared_event(&self, event: PoolClearedEvent) {}
    fn handle_pool_closed_event(&self, event: PoolClosedEvent) {}
    fn handle_connection_created_event(&self, event: ConnectionCreatedEvent) {}
    fn handle_connection_ready_event(&self, event: ConnectionReadyEvent) {}
    fn handle_connection_closed_event(&self, event: ConnectionClosedEvent) {}
    fn handle_connection_checkout_started_event(&self, event: ConnectionCheckoutStartedEvent) {}
    fn handle_connection_checkout_failed_event(&self, event: ConnectionCheckoutFailedEvent) {}
    fn handle_connection_checked_out_event(&self, event: ConnectionCheckedOutEvent) {}
    fn handle_connection_checked_in_event(&self, event: ConnectionCheckedInEvent) {}
}
