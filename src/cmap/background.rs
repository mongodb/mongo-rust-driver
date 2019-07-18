use std::{
    sync::{atomic::Ordering, Arc, Weak},
    time::Duration,
};

use super::ConnectionPoolInner;

pub(crate) fn start_background_thread(pool: Weak<ConnectionPoolInner>) {
    std::thread::spawn(move || loop {
        perform_checks(&pool);

        std::thread::sleep(Duration::from_millis(100));
    });
}

fn perform_checks(pool: &Weak<ConnectionPoolInner>) {
    let pool = match pool.upgrade() {
        Some(pool) => pool,
        None => return,
    };

    remove_perished_connections_from_pool(&pool);
    ensure_min_connections_in_pool(&pool);
}

fn remove_perished_connections_from_pool(pool: &Arc<ConnectionPoolInner>) {
    let mut connections = pool.connections.write().unwrap();
    let mut i = 0;

    while i < connections.len() {
        if connections[i].is_stale(pool.generation.load(Ordering::SeqCst))
            || connections[i].is_idle(pool.max_idle_time)
        {
            connections.remove(i);
        }

        i += 1;
    }
}

fn ensure_min_connections_in_pool(pool: &Arc<ConnectionPoolInner>) {
    if let Some(min_pool_size) = pool.min_pool_size {
        loop {
            let mut connections = pool.connections.write().unwrap();

            if pool.total_connection_count.load(Ordering::SeqCst) < min_pool_size {
                connections.push(pool.create_connection());
            } else {
                return;
            }
        }
    }
}
