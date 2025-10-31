use crate::test::spec::unified_runner::run_unified_tests;

#[tokio::test(flavor = "multi_thread")]
async fn run_unified_operation() {
    run_unified_tests(&["open-telemetry", "operation"]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_unified_transaction() {
    run_unified_tests(&["open-telemetry", "transaction"]).await;
}
