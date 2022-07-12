use crate::test::{run_spec_test, LOCK};

use super::{run_unified_format_test_filtered, unified_runner::TestCase};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))] // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard = LOCK.run_exclusively().await;
    // TODO SERVER-65497: remove this skip.
    run_spec_test(&["gridfs"], |f| {
        run_unified_format_test_filtered(f, |_| false)
    })
    .await;
}

fn test_predicate(test: &TestCase) -> bool {
    let lower = test.description.to_lowercase();

    // The Rust driver doesn't support the disableMD5 and contentType options for upload.
    !lower.contains("sans md5") && !lower.contains("contentType")
}