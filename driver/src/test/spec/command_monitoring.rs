use crate::test::spec::unified_runner::run_unified_tests;

#[tokio::test(flavor = "multi_thread")]
async fn command_monitoring_unified() {
    run_unified_tests(&["command-logging-and-monitoring", "monitoring"])
        .skip_files(&[
            // TODO RUST-1599: Unskip this file
            "find.json",
            // The driver does not support unacknowledged writes.
            "unacknowledged-client-bulkWrite.json",
        ])
        .skip_tests(&[
            // This test relies on old OP_QUERY behavior that many drivers still use for < 4.4, but
            // we do not use, due to never implementing OP_QUERY.
            "A successful find event with a getmore and the server kills the cursor (<= 4.4)",
        ])
        .await;
}
