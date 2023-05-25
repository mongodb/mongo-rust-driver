use tokio::sync::RwLockWriteGuard;

use crate::test::{
    spec::{unified_runner::run_unified_tests, v2_runner::run_v2_tests},
    LOCK,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    let skipped_tests = if cfg!(not(feature = "openssl-tls")) {
        Some(vec!["create datakey with KMIP KMS provider"])
    } else {
        None
    };

    run_unified_tests(&["client-side-encryption", "unified"])
        .skip_tests(skipped_tests)
        .await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_legacy() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    // TODO RUST-528: unskip this file
    let mut skipped_files = vec!["timeoutMS.json"];
    if cfg!(not(feature = "openssl-tls")) {
        skipped_files.push("kmipKMS.json");
    }

    run_v2_tests(&["client-side-encryption", "legacy"])
        .skip_files(skipped_files)
        .await;
}
