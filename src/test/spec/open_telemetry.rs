use crate::test::spec::unified_runner::run_unified_tests;

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    run_unified_tests(&["open-telemetry", "operation"]).await;
}
