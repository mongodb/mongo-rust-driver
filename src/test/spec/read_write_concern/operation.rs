use tokio::sync::RwLockWriteGuard;

use crate::test::{spec::v2_runner::run_v2_tests, LOCK};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _lock: RwLockWriteGuard<_> = LOCK.run_exclusively().await;
    run_v2_tests(&["read-write-concern", "operation"]).await;
}
