use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    bson::doc,
    options::{ServerApi, ServerApiVersion},
    test::{run_spec_test, EventClient, TestClient, CLIENT_OPTIONS, LOCK},
};

use super::run_unified_format_test;

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;
    // TODO RUST-768 Unskip these tests on 5.0
    if TestClient::new().await.server_version_gte(5, 0) {
        return;
    }
    run_spec_test(&["versioned-api"], run_unified_format_test).await;
}

// TODO RUST-817 Remove this test in favor of transaction-handling.json versioned API spec test when
// transactions are implemented in the unified runner
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn transaction_handling() {
    let _guard: RwLockReadGuard<_> = LOCK.run_concurrently().await;

    let version = ServerApi::builder().version(ServerApiVersion::V1).build();

    let mut options = CLIENT_OPTIONS.clone();
    options.server_api = Some(version);
    let client = EventClient::with_options(options).await;
    if !client.is_replica_set() || client.server_version_lt(5, 0) {
        return;
    }

    let mut session = client.start_session(None).await.unwrap();
    session.start_transaction(None).await.unwrap();

    let coll = client
        .database(function_name!())
        .collection(function_name!());

    coll.insert_one_with_session(doc! { "x": 1 }, None, &mut session)
        .await
        .unwrap();
    session.commit_transaction().await.unwrap();
    session.commit_transaction().await.unwrap();

    session.start_transaction(None).await.unwrap();

    coll.insert_one_with_session(doc! { "y": 2 }, None, &mut session)
        .await
        .unwrap();
    session.abort_transaction().await.unwrap();
    session
        .abort_transaction()
        .await
        .expect_err("aborting twice should fail");

    let events = client.get_all_command_started_events();
    for event in events {
        assert!(event.command.contains_key("apiVersion"));
    }
}
