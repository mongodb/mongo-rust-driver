use crate::test::spec::unified_runner::run_unified_tests;

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    run_unified_tests(&["crud", "unified"])
        .skip_files(&[
            // The Rust driver does not support unacknowledged writes (and does not intend to in
            // the future).
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
        .skip_tests(&[
            // Unacknowledged write; see above.
            "Unacknowledged write using dollar-prefixed or dotted keys may be silently rejected \
             on pre-5.0 server",
            // TODO RUST-663: Unskip these tests.
            "Aggregate with $out includes read preference for 5.0+ server",
            "Aggregate with $out omits read preference for pre-5.0 server",
            "Aggregate with $merge includes read preference for 5.0+ server",
            "Aggregate with $merge omits read preference for pre-5.0 server",
            "Database-level aggregate with $out omits read preference for pre-5.0 server",
            "Database-level aggregate with $merge omits read preference for pre-5.0 server",
        ])
        .await;
}
