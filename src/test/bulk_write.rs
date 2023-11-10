use crate::test::spec::unified_runner::run_unified_tests;

#[tokio::test]
async fn run_unified() {
    run_unified_tests(&["crud", "unified", "new-bulk-write"]).await;
}
