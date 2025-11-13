use crate::test::spec::unified_runner::run_unified_tests;

#[tokio::test(flavor = "multi_thread")] // multi_thread required for FailPoint
async fn run_unified() {
    run_unified_tests(&["run-command", "unified"])
        .skip_tests(&[
            // TODO RUST-1649: unskip when withTransaction is implemented
            "attaches transaction fields to given command",
        ])
        .await;
}
