use serde::{Deserialize, Serialize};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    bson::{doc, serde_helpers::serialize_u64_as_i32, Document},
    client::session::TransactionState,
    test::{run_spec_test, TestClient, LOCK},
};

use super::run_v2_test;

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    // TODO RUST-122: Unskip tests on sharded clusters
    if TestClient::new().await.is_sharded() {
        return;
    }
    run_spec_test(&["transactions"], run_v2_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
/// This test replicates the test cases in errors-client.json as we do not have the document
/// validation required to trigger client-side errors in those tests.
async fn client_errors() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    #[derive(Debug, Deserialize, Serialize)]
    struct A {
        #[serde(serialize_with = "serialize_u64_as_i32")]
        num: u64,
    }

    let client = TestClient::new().await;
    if !client.is_replica_set() || client.server_version_lt(4, 0) {
        return;
    }

    let mut session = client.start_session(None).await.unwrap();
    session.start_transaction(None).await.unwrap();

    // Collections cannot be created during a transaction pre-4.4 (including implicitly during the
    // insert_one_with_session calls)
    if client.server_version_lt(4, 4) {
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
    }
    let coll = client
        .database(function_name!())
        .collection(function_name!());

    // trigger a client error via unsigned integer deserialization
    let a = A { num: u64::MAX };
    let result = coll.insert_one_with_session(a, None, &mut session).await;
    assert!(result.is_err());
    assert_eq!(session.transaction.state, TransactionState::Starting);

    let a = A { num: 0 };
    let result = coll.insert_one_with_session(a, None, &mut session).await;
    assert!(result.is_ok());
    assert_eq!(session.transaction.state, TransactionState::InProgress);

    // trigger a client error via unsigned integer deserialization
    let a = A { num: u64::MAX };
    let result = coll.insert_one_with_session(a, None, &mut session).await;
    assert!(result.is_err());
    assert_eq!(session.transaction.state, TransactionState::InProgress);
}
