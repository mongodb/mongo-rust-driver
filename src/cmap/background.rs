use std::{
    sync::{atomic::Ordering, Arc, Weak},
    time::Duration,
};

use super::ConnectionPoolInner;

/// Initializes the background thread for a connection pool. A weak reference is used to ensure that
/// the connection pool is not kept alive by the background thread; the background thread will
/// terminate if the weak reference cannot be converted to a strong reference.
pub(crate) fn start_background_thread(pool: Weak<ConnectionPoolInner>) {
    std::thread::spawn(move || loop {
        match pool.upgrade() {
            Some(pool) => perform_checks(pool),
            None => return,
        };

        // This number was chosen arbitrarily; the Java driver uses 10 ms, but our CMAP spec tests
        // failed when we used that value.
        std::thread::sleep(Duration::from_millis(100));
    });
}

/// Cleans up any stale or idle connections and adds new connections if the total number is below
/// the min pool size.
fn perform_checks(pool: Arc<ConnectionPoolInner>) {
    // We remove the perished connections first to ensure that the number of connections does not
    // dip under the min pool size due to the removals.
    remove_perished_connections_from_pool(&pool);
    ensure_min_connections_in_pool(&pool);
}

/// Iterate over the connections and remove any that are stale or idle.
fn remove_perished_connections_from_pool(pool: &Arc<ConnectionPoolInner>) {
    let mut connections = pool.connections.write().unwrap();
    let mut i = 0;

    while i < connections.len() {
        if connections[i].is_stale(pool.generation.load(Ordering::SeqCst))
            || connections[i].is_idle(pool.max_idle_time)
        {
            connections.remove(i);
        } else {
            i += 1;
        }
    }
}

/// Add connections until the min pool size it met. We explicitly release the lock at the end of
/// each iteration and acquire it again during the next one to ensure that the this method doesn't
/// block other threads from acquiring connections.
fn ensure_min_connections_in_pool(pool: &Arc<ConnectionPoolInner>) {
    if let Some(min_pool_size) = pool.min_pool_size {
        loop {
            let mut connections = pool.connections.write().unwrap();

            if pool.total_connection_count.load(Ordering::SeqCst) < min_pool_size {
                // If setting up a connection fails for some reason, there's no way to return the
                // error to the user, and we don't want to background thread to get stuck, so we try
                // to create a new connection once more. If it fails again, we
                // return from the function so that the background thread doesn't
                // get stuck forever.
                if let Ok(connection) = pool
                    .create_connection()
                    .or_else(|_| pool.create_connection())
                {
                    connections.push(connection);
                } else {
                    return;
                }
            } else {
                return;
            }
        }
    }
}
