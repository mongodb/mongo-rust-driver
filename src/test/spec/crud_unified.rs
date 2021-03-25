use tokio::sync::RwLockWriteGuard;

use crate::test::{run_spec_test, spec::run_unified_format_test, LOCK};

// The drop implementation on FailPointGuard blocks in the foreground, so we need to use the
// multi-thread runtime to avoid panicking.
#[cfg_attr(
    feature = "tokio-runtime",
    tokio::test(flavor = "multi_thread", worker_threads = 1)
)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test(&["crud", "unified"], run_unified_format_test).await;
}
