use crate::test::{
    spec::unified_runner::TestCase,
    run_spec_test,
    LOCK,
};

use super::run_unified_format_test_filtered;

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard = LOCK.run_exclusively().await;
    run_spec_test(&["change-streams", "unified"], |t| run_unified_format_test_filtered(t, test_filter)).await;
}

fn test_filter(_test: &TestCase) -> bool {
    true
}