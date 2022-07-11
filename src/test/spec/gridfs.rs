use crate::test::{run_spec_test, TestClient, LOCK};

use super::{run_unified_format_test_filtered, unified_runner::TestCase};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))] // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard = LOCK.run_exclusively().await;
    let client = TestClient::new().await;
    // TODO SERVER-65497: remove this skip.
    run_spec_test(&["gridfs", "unified"], |f| {
        run_unified_format_test_filtered(f, || true)
    })
    .await;
}
