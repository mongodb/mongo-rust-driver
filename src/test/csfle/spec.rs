use crate::test::spec::unified_runner::run_unified_tests;

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    let mut skipped_tests = vec![];
    if cfg!(not(feature = "openssl-tls")) {
        skipped_tests.extend_from_slice(&[
            "Insert a document with auto encryption using KMIP KMS provider",
            "create datakey with KMIP KMS provider",
            "create datakey with KMIP delegated KMS provider",
            "create datakey with named KMIP KMS provider",
            "Insert a document with auto encryption using KMIP delegated KMS provider",
        ]);
    }

    run_unified_tests(&["client-side-encryption", "unified"])
        .skip_tests(&skipped_tests)
        // TODO RUST-582: unskip this test
        .skip_files(&["timeoutMS.json"])
        .await;
}
