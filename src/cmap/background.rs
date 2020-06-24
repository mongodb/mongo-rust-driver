use std::{sync::Weak, time::Duration};

use super::{ConnectionPool, ConnectionPoolInner};
use crate::{event::cmap::ConnectionClosedReason, RUNTIME};

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

            RUNTIME.delay_for(Duration::from_millis(10)).await;
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
        let mut connection_manager = self.inner.connection_manager.lock().await;

        let mut i = 0;
        while i < connection_manager.checked_in_connections.len() {
            if connection_manager.checked_in_connections[i].is_stale(connection_manager.generation)
            {
                let connection = connection_manager.checked_in_connections.remove(i);
                connection_manager.close_connection(connection, ConnectionClosedReason::Stale);
            } else if connection_manager.checked_in_connections[i].is_idle(self.inner.max_idle_time)
            {
                let connection = connection_manager.checked_in_connections.remove(i);
                connection_manager.close_connection(connection, ConnectionClosedReason::Idle);
            } else {
                i += 1;
            }
        }
    }

    /// Add connections until the min pool size it met. We explicitly release the lock at the end of
    /// each iteration and acquire it again during the next one to ensure that the this method
    /// doesn't block other threads from acquiring connections.
    async fn ensure_min_connections(&self) {
        if let Some(min_pool_size) = self.inner.min_pool_size {
            loop {
                let mut connection_manager = self.inner.connection_manager.lock().await;
                if connection_manager.total_connection_count < min_pool_size {
                    match connection_manager.create_connection().await {
                        Ok(connection) => {
                            connection_manager.checked_in_connections.push(connection)
                        }
                        Err(_) => {
                            // Since we encountered an error, we return early from this function and
                            // put the background thread back to sleep. Next time it wakes up, any
                            // stale connections will be closed, and the thread can try to create
                            // new ones after that.
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
