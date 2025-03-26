use crate::test::{spec::unified_runner::run_unified_tests, SERVERLESS};

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    let skipped_files = vec![
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
        // TODO RUST-1405: unskip the errorResponse tests
        "client-bulkWrite-errorResponse.json",
        "bulkWrite-errorResponse.json",
        "updateOne-errorResponse.json",
        "insertOne-errorResponse.json",
        "deleteOne-errorResponse.json",
        "aggregate-merge-errorResponse.json",
        "findOneAndUpdate-errorResponse.json",
    ];

    let mut skipped_tests = vec![
        // Unacknowledged write; see above.
        "Unacknowledged write using dollar-prefixed or dotted keys may be silently rejected on \
         pre-5.0 server",
        "Requesting unacknowledged write with verboseResults is a client-side error",
        "Requesting unacknowledged write with ordered is a client-side error",
        // TODO RUST-663: Unskip these tests.
        "Aggregate with $out includes read preference for 5.0+ server",
        "Aggregate with $out omits read preference for pre-5.0 server",
        "Aggregate with $merge includes read preference for 5.0+ server",
        "Aggregate with $merge omits read preference for pre-5.0 server",
        "Database-level aggregate with $out omits read preference for pre-5.0 server",
        "Database-level aggregate with $merge omits read preference for pre-5.0 server",
        // TODO RUST-2071: unskip this test
        "Find with batchSize equal to limit",
    ];
    // TODO: remove this manual skip when this test is fixed to skip on serverless
    if *SERVERLESS {
        skipped_tests.push("inserting _id with type null via clientBulkWrite");
    }

    run_unified_tests(&["crud", "unified"])
        .skip_files(&skipped_files)
        .skip_tests(&skipped_tests)
        .await;
}
