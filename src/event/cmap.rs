pub use crate::cmap::options::ConnectionPoolOptions;

#[derive(Debug)]
pub struct PoolCreatedEvent {
    pub address: String,

    pub options: Option<ConnectionPoolOptions>,
}

#[derive(Debug)]
pub struct PoolClearedEvent {
    pub address: String,
}

#[derive(Debug)]
pub struct PoolClosedEvent {
    pub address: String,
}

#[derive(Debug)]
pub struct ConnectionCreatedEvent {
    pub address: String,
    pub connection_id: u32,
}

#[derive(Debug)]
pub struct ConnectionReadyEvent {
    pub address: String,
    pub connection_id: u32,
}

#[derive(Debug)]
pub struct ConnectionClosedEvent {
    pub address: String,
    pub connection_id: u32,
    pub reason: ConnectionClosedReason,
}

#[derive(Debug)]
pub enum ConnectionClosedReason {
    Stale,
    Idle,
    Error,
    PoolClosed,
}

#[derive(Debug)]
pub struct ConnectionCheckoutStartedEvent {
    pub address: String,
}

#[derive(Debug)]
pub struct ConnectionCheckoutFailedEvent {
    address: String,
}

#[derive(Debug)]
pub enum ConnectionCheckoutFailedReason {
    PoolClosed,
    Timeout,
    ConnectionError,
}

#[derive(Debug)]
pub struct ConnectionCheckedOutEvent {
    pub address: String,
    pub connection_id: u32,
}

#[derive(Debug)]
pub struct ConnectionCheckedInEvent {
    pub address: String,
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
