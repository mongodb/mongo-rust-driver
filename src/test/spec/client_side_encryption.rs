use tokio::sync::RwLockWriteGuard;

use crate::test::{
    spec::{unified_runner::run_unified_tests, v2_runner::run_v2_tests},
    LOCK,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    #[cfg(not(feature = "openssl-tls"))]
    let skipped_tests = &["create datakey with KMIP KMS provider"];
    #[cfg(feature = "openssl-tls")]
    let skipped_tests = &[];

    run_unified_tests(&["client-side-encryption", "unified"])
        .skipped_tests(skipped_tests)
        .await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_legacy() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    // TODO RUST-528: Unskip timeoutMS.json when CSOT is implemented.
    #[cfg(not(feature = "openssl-tls"))]
    let skipped_tests = &["timeoutMS.json", "kmipKMS.json"];
    #[cfg(feature = "openssl-tls")]
    let skipped_tests = &["timeoutMS.json"];

    run_v2_tests(&["client-side-encryption", "legacy"])
        .skipped_files(skipped_tests)
        .await;
}
