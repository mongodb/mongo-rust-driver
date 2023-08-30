use crate::test::spec::unified_runner::run_unified_tests;

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    run_unified_tests(&["collection-management"])
        // The driver does not support modifyCollection.
        .skip_files(&["modifyCollection-pre_and_post_images.json", "modifyCollection-errorResponse.json"])
        .await;
}
