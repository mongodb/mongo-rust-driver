mod connection_string;
mod document;

use crate::test::spec::unified_runner::run_unified_tests;

#[tokio::test]
async fn operation() {
    run_unified_tests(&["read-write-concern", "operation"]).await;
}
