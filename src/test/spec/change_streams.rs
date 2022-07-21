use crate::test::{run_spec_test, LOCK};

use super::run_unified_format_test;

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))] // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard = LOCK.run_exclusively().await;
    run_spec_test(&["change-streams", "unified"], |f| {
        run_unified_format_test(f)
    })
    .await;
}
