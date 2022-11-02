use tokio::sync::RwLockWriteGuard;

use crate::test::LOCK;

use super::{run_spec_test_with_path, run_unified_format_test};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test_with_path(
        &["client-side-encryption", "unified"],
        run_unified_format_test,
    )
    .await;
}
