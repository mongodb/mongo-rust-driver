use crate::test::spec::unified_runner::run_unified_tests;

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    run_unified_tests(&["index-management"]).await;
}
