use serde::{Deserialize, Serialize};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    bson::{doc, Document},
    test::{log_uncaptured, TestClient, LOCK},
    Collection,
};

use super::{run_spec_test_with_path, run_unified_format_test, run_v2_test};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_legacy() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    run_spec_test_with_path(&["transactions", "legacy"], run_v2_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    // TODO RUST-902: Reduce transactionLifetimeLimitSeconds.
    run_spec_test_with_path(&["transactions", "unified"], run_unified_format_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
// This test checks that deserializing an operation correctly still retrieves the recovery token.
async fn deserialize_recovery_token() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    #[derive(Debug, Serialize)]
    struct A {
        num: i32,
    }

    #[derive(Debug, Deserialize)]
    struct B {
        _str: String,
    }

    let client = TestClient::new().await;
    if !client.is_sharded() || client.server_version_lt(4, 2) {
        log_uncaptured("skipping deserialize_recovery_token due to test topology");
        return;
    }

    let mut session = client.start_session(None).await.unwrap();

    // Insert a document with schema A.
    client
        .database(function_name!())
        .collection::<Document>(function_name!())
        .drop(None)
        .await
        .unwrap();
    client
        .database(function_name!())
        .create_collection(function_name!(), None)
        .await
        .unwrap();
    let coll = client
        .database(function_name!())
        .collection(function_name!());
    coll.insert_one(A { num: 4 }, None).await.unwrap();

    // Attempt to execute Find on a document with schema B.
    let coll: Collection<B> = client
        .database(function_name!())
        .collection(function_name!());
    session.start_transaction(None).await.unwrap();
    assert!(session.transaction.recovery_token.is_none());
    let result = coll.find_one_with_session(None, None, &mut session).await;
    assert!(result.is_err()); // Assert that the deserialization failed.

    // Nevertheless, the recovery token should have been retrieved from the ok: 1 response.
    assert!(session.transaction.recovery_token.is_some());
}
