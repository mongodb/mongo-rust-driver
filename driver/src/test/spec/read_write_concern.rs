mod connection_string;
mod document;

use crate::test::spec::unified_runner::run_unified_tests;

#[tokio::test]
async fn operation() {
    run_unified_tests(&["read-write-concern", "operation"]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn write_concern_errors() {
    run_unified_tests(&["read-write-concern", "write-concern-errors"])
        // TODO RUST-1441: run this test
        .skip_files(&["client-side-encryption.json"])
        // our unified test operation implementation of rename uses run_command, which does not
        // parse write concern errors
        .skip_tests(&["rename reports a writeConcernError"])
        .await;
}
