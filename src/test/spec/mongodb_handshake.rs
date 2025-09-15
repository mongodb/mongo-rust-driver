use super::unified_runner::run_unified_tests;

#[tokio::test]
async fn run_unified() {
    run_unified_tests(&["mongodb-handshake", "unified"]).await;
}
