use crate::test::spec::unified_runner::run_unified_tests;

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    // TODO:
    //  server.type
    //  output db.mongodb.cursor_id
    run_unified_tests(&["open-telemetry", "operation"])
        .skip_tests(&[
            "List collections",   // expects `cursor: {}` in `db.query.text`
            "update one element", // expects `txnNumber: 1` in `db.query.text`
            "insert one element", // expects `txnNumber: 1` in `db.query.text`
        ])
        .await;
}
