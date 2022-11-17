use tokio::sync::RwLockWriteGuard;

use crate::test::{run_spec_test_with_path, LOCK};

use super::super::run_v2_test;

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _lock: RwLockWriteGuard<_> = LOCK.run_exclusively().await;
    run_spec_test_with_path(&["read-write-concern", "operation"], run_v2_test).await;
}
