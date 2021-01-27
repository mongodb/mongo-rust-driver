use tokio::sync::RwLockWriteGuard;

use crate::test::{run_spec_test, run_v2_test, LOCK};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test(&["retryable-reads"], run_v2_test).await;
}
