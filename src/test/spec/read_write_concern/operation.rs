use crate::test::run_spec_test;

use super::super::run_v2_test;

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    run_spec_test(&["read-write-concern", "operation"], run_v2_test).await;
}
