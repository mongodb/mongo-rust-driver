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
    let lower = test.description.to_lowercase();

    !lower.contains("unacknowledged")
    // TODO: RUST-1071: unskip comment tests
        && (!lower.contains("comment")
            || lower.contains("estimateddocumentcount"))
    // TODO: RUST-663: unskip aggregate $out and $merge tests
        && !(lower.contains("aggregate with $out includes read preference for 5.0+ server"))
        && !(lower.contains("aggregate with $out omits read preference for pre-5.0 server"))
        && !(lower.contains("aggregate with $merge includes read preference for 5.0+ server"))
        && !(lower.contains("aggregate with $merge omits read preference for pre-5.0 server"))
}
