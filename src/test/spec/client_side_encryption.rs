use tokio::sync::RwLockWriteGuard;

use crate::test::LOCK;

use super::{run_spec_test_with_path, run_unified_format_test_filtered, unified_runner::TestCase};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test_with_path(&["client_side_encryption", "unified"], |path, file| {
        run_unified_format_test_filtered(path, file, test_predicate)
    })
    .await;
}

fn test_predicate(_test: &TestCase) -> bool {
    true
    /*
    // The Rust driver doesn't support unacknowledged writes.
    let lower = test.description.to_lowercase();

    !lower.contains("unacknowledged")
        // TODO: RUST-663: unskip aggregate $out and $merge tests
        && !(lower.contains("aggregate with $out includes read preference for 5.0+ server"))
        && !(lower.contains("aggregate with $out omits read preference for pre-5.0 server"))
        && !(lower.contains("aggregate with $merge includes read preference for 5.0+ server"))
        && !(lower.contains("aggregate with $merge omits read preference for pre-5.0 server"))
        */
}
