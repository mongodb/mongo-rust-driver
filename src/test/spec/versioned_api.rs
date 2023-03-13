use tokio::sync::RwLockWriteGuard;

use crate::test::{spec::unified_runner::run_unified_tests, LOCK};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;
    run_unified_tests(&["versioned-api"]).await;
}
