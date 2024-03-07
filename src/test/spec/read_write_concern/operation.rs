use crate::test::spec::v2_runner::run_v2_tests;

#[tokio::test]
async fn run() {
    run_v2_tests(&["read-write-concern", "operation"]).await;
}
