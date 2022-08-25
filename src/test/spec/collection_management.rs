use crate::test::{log_uncaptured, LOCK};

use super::{run_spec_test_with_path, run_unified_format_test};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard = LOCK.run_exclusively().await;
    run_spec_test_with_path(&["collection-management"], |path, test| async {
        if path.ends_with("modifyCollection-pre_and_post_images.json") {
            // modifyCollection is unsupported.
            log_uncaptured("skipping modifyCollection-pre_and_post_images");
            return;
        }
        run_unified_format_test(path, test).await;
    })
    .await;
}
