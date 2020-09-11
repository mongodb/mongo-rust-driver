use std::{
    sync::{atomic::Ordering, Weak},
    time::Duration,
};

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
    pool.inner.remove_perished_connections().await;
    pool.inner.ensure_min_connections().await;
}

impl ConnectionPoolInner {
    /// Iterate over the connections and remove any that are stale or idle.
    async fn remove_perished_connections(&self) {
        let mut available_connections = self.available_connections.lock().await;

        let mut i = 0;
        while i < available_connections.len() {
            let connection = &available_connections[i];
            if connection.is_stale(self.generation.load(Ordering::SeqCst)) {
                let connection = available_connections.remove(i);
                self.close_connection(connection, ConnectionClosedReason::Stale);
            } else if connection.is_idle(self.max_idle_time) {
                let connection = available_connections.remove(i);
                self.close_connection(connection, ConnectionClosedReason::Idle);
            } else {
                i += 1;
            }
        }
    }

    /// Add connections until the min pool size it met. We explicitly release the lock at the end of
    /// each iteration and acquire it again during the next one to ensure that the this method
    /// doesn't block other threads from acquiring connections.
    async fn ensure_min_connections(&self) {
        if let Some(min_pool_size) = self.min_pool_size {
            loop {
                if self.total_connection_count.load(Ordering::SeqCst) < min_pool_size {
                    // Reserve a spot via the wait queue. This will prevent too many threads from
                    // concurrently creating connections such that max_pool_size is exceeded.
                    let _wait_queue_handle = match self.wait_queue.try_skip_queue() {
                        None => {
                            // This branch is rarely taken because it was just verified that
                            // total_connection_count < min_pool_size, and min_pool_size <= max_pool_size.
                            // A connection could have been created by an operation thread between the check
                            // and the attempt to reserve a spot, in which case we just return early because
                            // total_connection_count == max_pool_size.
                            return;
                        }
                        Some(handle) => handle,
                    };

                    let connection = self.create_pending_connection();
                    match self.establish_connection(connection).await {
                        Ok(connection) => {
                            let mut available_connections = self.available_connections.lock().await;
                            available_connections.push(connection)
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
