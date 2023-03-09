use tokio::sync::RwLockWriteGuard;

use crate::test::{spec::unified_runner::run_unified_tests, LOCK};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    run_unified_tests(&["crud", "unified"])
        // The Rust driver does not support unacknowledged writes (and does not intend to in the future).
        .skipped_files(&[
            "bulkWrite-deleteMany-hint-unacknowledged.json",
            "bulkWrite-deleteOne-hint-unacknowledged.json",
            "bulkWrite-replaceOne-hint-unacknowledged.json",
            "bulkWrite-updateMany-hint-unacknowledged.json",
            "bulkWrite-updateOne-hint-unacknowledged.json",
            "deleteMany-hint-unacknowledged.json",
            "deleteOne-hint-unacknowledged.json",
            "findOneAndDelete-hint-unacknowledged.json",
            "findOneAndReplace-hint-unacknowledged.json",
            "findOneAndUpdate-hint-unacknowledged.json",
            "replaceOne-hint-unacknowledged.json",
            "updateMany-hint-unacknowledged.json",
            "updateOne-hint-unacknowledged.json",
        ])
        // TODO RUST-663: Unskip these tests.
        .skipped_tests(&[
            "Aggregate with $out includes read preference for 5.0+ server",
            "Aggregate with $out omits read preference for pre-5.0 server",
            "Aggregate with $merge includes read preference for 5.0+ server",
            "Aggregate with $merge omits read preference for pre-5.0 server",
        ])
        .await;
}
