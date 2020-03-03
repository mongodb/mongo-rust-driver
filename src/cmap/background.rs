use std::{
    time::Duration,
    sync::Weak,
};

use futures_timer::Delay;

use super::{ConnectionPool, ConnectionPoolInner};
use crate::{RUNTIME, event::cmap::ConnectionClosedReason};

/// Initializes the background thread for a connection pool. A weak reference is used to ensure that
/// the connection pool is not kept alive by the background thread; the background thread will
/// terminate if the weak reference cannot be converted to a strong reference.
pub(crate) fn start_background_task(pool: Weak<ConnectionPoolInner>) {
    RUNTIME.execute(async move {
        loop {
            match pool.upgrade() {
                Some(pool) => perform_checks(pool.into()).await,
                None => return,
            };
            
            Delay::new(Duration::from_millis(10)).await;
        }
    });
}

/// Cleans up any stale or idle connections and adds new connections if the total number is below
/// the min pool size.
async fn perform_checks(pool: ConnectionPool) {
    // We remove the perished connections first to ensure that the number of connections does not
    // dip under the min pool size due to the removals.
    pool.remove_perished_connections().await;
    pool.ensure_min_connections().await;
}

impl ConnectionPool {
    /// Iterate over the connections and remove any that are stale or idle.
    async fn remove_perished_connections(&self) {
        let mut connections = self.inner.connections.lock().await;
        let checked_in_connections = &mut connections.checked_in_connections;
        let generation = *self.inner.generation.read().await;
        let mut i = 0;
        
        while i < checked_in_connections.len() {
            if checked_in_connections[i].is_stale(generation) {
                self.inner.close_connection(checked_in_connections.remove(i), ConnectionClosedReason::Stale).await;
            } else if checked_in_connections[i].is_idle(self.inner.max_idle_time) {
                self.inner.close_connection(checked_in_connections.remove(i), ConnectionClosedReason::Idle).await;
            } else {
                i += 1;
            }
        }
    }

    /// Add connections until the min pool size it met. We explicitly release the lock at the end of
    /// each iteration and acquire it again during the next one to ensure that the this method doesn't
    /// block other threads from acquiring connections.
    async fn ensure_min_connections(&self) {
        if let Some(min_pool_size) = self.inner.min_pool_size {
            loop {
                let mut connections = self.inner.connections.lock().await;
                if connections.total_connection_count < min_pool_size {
                    match self.create_connection(&mut connections).await {
                        Ok(connection) => connections.checked_in_connections.push(connection),
                        e @ Err(_) => {
                            // Since we encountered an error, we return early from this function and
                            // put the background thread back to sleep. Next time it wakes up, any
                            // stale connections will be closed, and the thread can try to create new
                            // ones after that.
                            return;
                        }
                    }
                } else {
                    return;
                }
            }
        }
    }
}
