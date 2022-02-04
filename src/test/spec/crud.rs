use tokio::sync::RwLockWriteGuard;

use crate::test::{run_spec_test, LOCK};

use super::{run_unified_format_test_filtered, unified_runner::TestCase};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test(&["crud", "unified"], |file| {
        run_unified_format_test_filtered(file, test_predicate)
    })
    .await;
}

fn test_predicate(test: &TestCase) -> bool {
    // The Rust driver doesn't support unacknowledged writes.
    test.description
        != "Unacknowledged write using dollar-prefixed or dotted keys may be silently rejected on \
            pre-5.0 server"
}
