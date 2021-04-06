use std::time::Duration;

use bson::doc;
use tokio::sync::RwLockWriteGuard;

use crate::{
    test::{
        run_spec_test,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
        CLIENT_OPTIONS,
        LOCK,
    },
    RUNTIME,
};

use super::run_v2_test;

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test(&["retryable-reads"], run_v2_test).await;
}

/// Test ensures that the connection used in the first attempt of a retry is released back into the
/// pool before the second attempt.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn retry_releases_connection() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    let mut client_options = CLIENT_OPTIONS.clone();
    client_options.hosts.drain(1..);
    client_options.retry_reads = Some(true);
    client_options.max_pool_size = Some(1);

    let client = TestClient::with_options(Some(client_options)).await;
    if !client.supports_fail_command().await {
        println!("skipping retry_releases_connection due to failCommand not being supported");
        return;
    }

    let collection = client
        .database("retry_releases_connection")
        .collection("retry_releases_connection");
    collection.insert_one(doc! { "x": 1 }, None).await.unwrap();

    let options = FailCommandOptions::builder().error_code(91).build();
    let failpoint = FailPoint::fail_command(&["find"], FailPointMode::Times(1), Some(options));
    let _fp_guard = client.enable_failpoint(failpoint, None).await.unwrap();

    RUNTIME
        .timeout(Duration::from_secs(1), collection.find_one(doc! {}, None))
        .await
        .expect("operation should not time out")
        .expect("find should succeed");
}
