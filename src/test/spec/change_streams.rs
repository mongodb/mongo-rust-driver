use crate::test::{
    spec::unified_runner::TestCase,
    run_spec_test_with_path,
    LOCK,
};

use super::run_unified_format_test_filtered;

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))] // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard = LOCK.run_exclusively().await;
    run_spec_test_with_path(&["change-streams", "unified"], |path, t| {
        async move {
            if !path.ends_with("change-streams-resume-errorLabels.json") {
                return;
            }
            run_unified_format_test_filtered(t, test_filter).await
        }
    }).await;
}

fn test_filter(_test: &TestCase) -> bool {
    true
}