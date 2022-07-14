use crate::test::{spec::run_spec_test, LOCK};

use super::{run_spec_test_with_path, run_unified_format_test_filtered, unified_runner::TestCase};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_monitoring_unified() {
    let _guard = LOCK.run_exclusively().await;

    let pred = |tc: &TestCase|
        // This test relies on old OP_QUERY behavior that many drivers still use for < 4.4, but we do not use, due to never implementing OP_QUERY.
        tc.description != "A successful find event with a getmore and the server kills the cursor (<= 4.4)";

    run_spec_test_with_path(&["command-monitoring", "unified"], |path, f| {
        run_unified_format_test_filtered(path, f, pred)
    })
    .await;
}
