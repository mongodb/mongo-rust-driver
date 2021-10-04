use std::path::PathBuf;

use tokio::sync::RwLockWriteGuard;

use crate::test::{LOCK, run_spec_test_with_path, spec::unified_runner::TestFile};

use super::run_unified_format_test_filtered;

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    async fn run_test(path: PathBuf, test_file: TestFile) {
        if !path.ends_with("cursors.json") { return; }
        run_unified_format_test_filtered(test_file, |tc|
            tc.description == "listIndexes pins the cursor to a connection"
        ).await;
    }
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test_with_path(&["load-balancers"], run_test).await;
}
