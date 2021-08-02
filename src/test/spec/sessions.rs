use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    bson::doc,
    error::ErrorKind,
    test::{run_spec_test, TestClient, LOCK},
};

use super::{run_unified_format_test, run_v2_test};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test(&["sessions", "unified"], run_unified_format_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_legacy() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test(&["sessions", "legacy"], run_v2_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn explicit_session_created_on_same_client() {
    let _guard: RwLockReadGuard<_> = LOCK.run_concurrently().await;

    let client0 = TestClient::new().await;
    let client1 = TestClient::new().await;

    let mut session0 = client0.start_session(None).await.unwrap();
    let mut session1 = client1.start_session(None).await.unwrap();

    let db = client0.database(function_name!());
    let err = db
        .list_collections_with_session(None, None, &mut session1)
        .await
        .unwrap_err();
    match *err.kind {
        ErrorKind::InvalidArgument { message } => assert!(message.contains("session provided")),
        other => panic!("expected InvalidArgument error, got {:?}", other),
    }

    let coll = client1
        .database(function_name!())
        .collection(function_name!());
    let err = coll
        .insert_one_with_session(doc! {}, None, &mut session0)
        .await
        .unwrap_err();
    match *err.kind {
        ErrorKind::InvalidArgument { message } => assert!(message.contains("session provided")),
        other => panic!("expected InvalidArgument error, got {:?}", other),
    }
}
