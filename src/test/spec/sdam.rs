use tokio::sync::RwLockWriteGuard;

use super::{run_spec_test_with_path, run_unified_format_test_filtered};
use crate::test::LOCK;

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test_with_path(
        &["server-discovery-and-monitoring", "unified"],
        |path, t| {
            run_unified_format_test_filtered(path, t, |test| {
                // skipped because we don't support socketTimeoutMS
                test.description.as_str() == "Cancel server check"
            })
        },
    ).await;
}
