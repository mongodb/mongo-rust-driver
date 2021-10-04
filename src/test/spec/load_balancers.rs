use std::path::PathBuf;

use tokio::sync::RwLockWriteGuard;

use crate::test::{run_spec_test_with_path, spec::unified_runner::TestFile, LOCK};

use super::run_unified_format_test_filtered;

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    async fn run_test(path: PathBuf, test_file: TestFile) {
        // The Rust driver doesn't support wait queue timeouts.
        if path.ends_with("wait-queue-timeouts.json") {
            return;
        }
        run_unified_format_test_filtered(test_file, |tc| {
            // TODO RUST-142 unskip this when change streams are implemented.
            if tc.description == "change streams pin to a connection" {
                return false;
            }
            true
        })
        .await;
    }
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test_with_path(&["load-balancers"], run_test).await;
}
