use crate::test::{run_spec_test, TestClient};

use super::{run_unified_format_test_filtered, unified_runner::TestCase};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))] // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let client = TestClient::new().await;
    // TODO SERVER-65497: remove this skip.
    let skip = client.is_sharded() && client.server_version_gte(6, 0);
    let pred =
        |tc: &TestCase| !skip || tc.description != "Test new structure in ns document MUST NOT err";

    run_spec_test(&["change-streams", "unified"], |f| {
        run_unified_format_test_filtered(f, pred)
    })
    .await;
}
