use crate::test::{
    run_spec_test_with_path,
    spec::unified_runner::{run_unified_format_test_filtered, TestCase},
    LOCK,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard = LOCK.run_concurrently().await;
    run_spec_test_with_path(&["gridfs"], |path, f| {
        run_unified_format_test_filtered(path, f, test_predicate)
    })
    .await;
}

fn test_predicate(test: &TestCase) -> bool {
    let lower = test.description.to_lowercase();

    // The Rust driver doesn't support the disableMD5 and contentType options for upload.
    !lower.contains("sans md5") && !lower.contains("contenttype")
}
