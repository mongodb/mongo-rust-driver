use crate::test::{run_spec_test, run_v2_test, LOCK};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard = LOCK.run_exclusively().await;
    run_spec_test(&["retryable-reads"], run_v2_test).await;
}
