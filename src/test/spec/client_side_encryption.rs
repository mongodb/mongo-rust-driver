use tokio::sync::RwLockWriteGuard;

use crate::test::{log_uncaptured, LOCK};

use super::{
    run_spec_test_with_path,
    run_unified_format_test_filtered,
    run_v2_test,
    unified_runner::TestCase,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test_with_path(&["client-side-encryption", "unified"], |path, test| {
        run_unified_format_test_filtered(path, test, spec_predicate)
    })
    .await;
}

#[allow(unused_variables)]
fn spec_predicate(test: &TestCase) -> bool {
    #[cfg(not(feature = "openssl-tls"))]
    {
        if test.description == "create datakey with KMIP KMS provider" {
            crate::test::log_uncaptured(format!(
                "Skipping {:?}: KMIP test requires openssl-tls",
                test.description
            ));
            return false;
        }
    }
    true
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_legacy() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    run_spec_test_with_path(&["client-side-encryption", "legacy"], |path, test| async {
        if path.ends_with("timeoutMS.json") {
            log_uncaptured(format!(
                "Skipping {}: requires client side operations timeout",
                path.display()
            ));
            return;
        }
        #[cfg(not(feature = "openssl-tls"))]
        if path.ends_with("kmipKMS.json") {
            log_uncaptured(format!(
                "Skipping {}: KMIP requires openssl",
                path.display()
            ));
            return;
        }
        run_v2_test(path, test).await;
    })
    .await;
}
