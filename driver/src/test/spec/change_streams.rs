use crate::test::spec::unified_runner::run_unified_tests;

#[tokio::test(flavor = "multi_thread")] // multi_thread required for FailPoint
async fn run_unified() {
    run_unified_tests(&["change-streams", "unified"])
        .skip_files(&[
            // TODO RUST-1281: unskip this file
            "change-streams-showExpandedEvents.json",
        ])
        .skip_tests(&[
            // TODO RUST-1658: unskip these tests
            "Test with document comment",
            "Test with string comment",
            "Test that comment is set on getMore",
            "Test with document comment - pre 4.4",
            "Test that comment is not set on getMore - pre 4.4",
        ])
        .await;
}
