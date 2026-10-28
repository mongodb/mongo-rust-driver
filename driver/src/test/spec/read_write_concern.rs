mod connection_string;
mod document;

use crate::test::spec::unified_runner::run_unified_tests;

#[tokio::test]
async fn operation() {
    run_unified_tests(&["read-write-concern", "operation"]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn write_concern_errors() {
    let mut skipped_files = vec![];
    if cfg!(not(feature = "in-use-encryption")) {
        skipped_files.push("client-side-encryption.json");
    }

    run_unified_tests(&["read-write-concern", "write-concern-errors"])
        .skip_files(&skipped_files)
        .skip_tests(&[
            // our unified test operation implementation of rename uses run_command, which does
            // not parse write concern errors
            "rename reports a writeConcernError",
        ])
        .await;
}
