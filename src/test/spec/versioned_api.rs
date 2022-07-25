use tokio::sync::RwLockWriteGuard;

use crate::test::{run_spec_test, LOCK};

use super::run_unified_format_test;

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;
    run_spec_test(&["versioned-api"], run_unified_format_test).await;
}
