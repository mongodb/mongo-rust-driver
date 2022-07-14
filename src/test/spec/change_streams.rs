use crate::test::{run_spec_test, TestClient, LOCK};

use super::{run_spec_test_with_path, run_unified_format_test_filtered, unified_runner::TestCase};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))] // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard = LOCK.run_exclusively().await;
    let client = TestClient::new().await;
    // TODO RUST-1412: remove this skip.
    let is_gte_6_1 = client.server_version_gte(6, 1);
    let pred = |tc: &TestCase| {
        !(is_gte_6_1 && tc.description == "change stream resumes after StaleShardVersion")
    };

    run_spec_test_with_path(&["change-streams", "unified"], |path, f| {
        run_unified_format_test_filtered(path, f, pred)
    })
    .await;
}
