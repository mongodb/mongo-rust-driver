use crate::test::{spec::unified_runner::run_unified_tests, LOCK};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard = LOCK.run_exclusively().await;
    run_unified_tests(&["collection-management"])
        // The driver does not support modifyCollection.
        .skip_files(&["modifyCollection-pre_and_post_images.json", "modifyCollection-errorResponse.json"])
        .await;
}
