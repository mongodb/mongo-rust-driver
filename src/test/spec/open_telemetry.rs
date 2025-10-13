use crate::test::{log_uncaptured, spec::unified_runner::run_unified_tests};

#[tokio::test(flavor = "multi_thread")]
async fn run_unified_operation() {
    // TODO:
    //  server.type
    //  output db.mongodb.cursor_id
    //  test parsing for db.mongodb.lsid
    let mut skip = vec![
        "List collections",   // expects `cursor: {}` in `db.query.text`
        "update one element", // expects `txnNumber: 1` in `db.query.text`
        "insert one element", // expects `txnNumber: 1` in `db.query.text`
    ];
    if crate::test::server_version_lte(4, 2).await {
        log_uncaptured("skipping \"findOneAndUpdate\" on server 4.2");
        skip.push("findOneAndUpdate"); // uses unsupported `comment` field
    }
    run_unified_tests(&["open-telemetry", "operation"])
        .skip_tests(&skip)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_unified_transaction() {
    run_unified_tests(&["open-telemetry", "transaction"]).await;
}
