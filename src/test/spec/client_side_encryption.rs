use crate::test::spec::{unified_runner::run_unified_tests, v2_runner::run_v2_tests};

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    let mut skipped_tests = vec![];
    if cfg!(not(feature = "openssl-tls")) {
        skipped_tests.push("create datakey with KMIP KMS provider");
        skipped_tests.push("create datakey with KMIP delegated KMS provider");
    }

    run_unified_tests(&["client-side-encryption", "unified"])
        .skip_tests(&skipped_tests)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_legacy() {
    // TODO RUST-528: unskip this file
    let mut skipped_files = vec!["timeoutMS.json"];
    if cfg!(not(feature = "openssl-tls")) {
        skipped_files.push("kmipKMS.json");
    }

    run_v2_tests(&["client-side-encryption", "legacy"])
        .skip_files(&skipped_files)
        .await;
}
