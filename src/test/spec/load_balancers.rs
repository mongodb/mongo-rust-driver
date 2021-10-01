use tokio::sync::RwLockWriteGuard;

use crate::test::{LOCK, run_spec_test, spec::unified_runner::TestFile};

use super::run_unified_format_test_filtered;

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    async fn run_test(test_file: TestFile) {
        run_unified_format_test_filtered(test_file, |tc|
            tc.description == "only connections for a specific serviceId are closed when pools are cleared"
        ).await;
        //run_unified_format_test_filtered(test_file, |_| true).await;
    }
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test(&["load-balancers"], run_test).await;
}
