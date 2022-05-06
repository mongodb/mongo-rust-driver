use crate::test::run_spec_test;

use super::run_unified_format_test;

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    run_spec_test(&["collection-management"], run_unified_format_test).await;
}
