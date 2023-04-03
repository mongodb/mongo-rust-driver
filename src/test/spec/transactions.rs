use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};
use futures_util::FutureExt;

use crate::{
    bson::{doc, Document},
    test::{
        log_uncaptured,
        spec::{unified_runner::run_unified_tests, v2_runner::run_v2_tests},
        TestClient,
        LOCK, FailPointMode, FailCommandOptions, FailPoint, CLIENT_OPTIONS,
    },
    Collection, error::{Error, Result, TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT},
};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_legacy() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_v2_tests(&["transactions", "legacy"]).await;
}


#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_legacy_convenient_api() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_v2_tests(&["transactions-convenient-api"]).await;
}

// TODO RUST-902: Reduce transactionLifetimeLimitSeconds.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_unified_tests(&["transactions", "unified"]).await;
}

// This test checks that deserializing an operation correctly still retrieves the recovery token.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn convenient_api_custom_error() {
    let _guard: _ = LOCK.run_concurrently().await;
    let client = crate::Client::test_builder().event_client().build().await;
    if !client.supports_transactions() {
        log_uncaptured("Skipping convenient_api_custom_error: no transaction support.");
        return;
    }
    let mut session = client.start_session(None).await.unwrap();
    let coll = client.database("test_convenient").collection::<Document>("test_convenient");

    struct MyErr;
    let result: Result<()> = session.with_transaction(
        coll,
        |session, coll| async move {
            coll.find_one_with_session(None, None, session).await?;
            Err(Error::custom(MyErr))
        }.boxed(),
        None,
    ).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().get_custom::<MyErr>().is_some());
    let events = client.get_all_command_started_events();
    assert_eq!(2, events.len());
    assert_eq!("find", events[0].command_name);
    assert_eq!("abortTransaction", events[1].command_name);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn convenient_api_returned_value() {
    let _guard: _ = LOCK.run_concurrently().await;
    let client = crate::Client::test_builder().event_client().build().await;
    if !client.supports_transactions() {
        log_uncaptured("Skipping convenient_api_returned_value: no transaction support.");
        return;
    }
    let mut session = client.start_session(None).await.unwrap();
    let coll = client.database("test_convenient").collection::<Document>("test_convenient");

    let value = session.with_transaction(
        coll,
        |session, coll| async move {
            coll.find_one_with_session(None, None, session).await?;
            Ok(42)
        }.boxed(),
        None,
    ).await.unwrap();

    assert_eq!(42, value);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn convenient_api_retry_timeout_callback() {
    let _guard: _ = LOCK.run_concurrently().await;
    let client = crate::Client::test_builder().event_client().build().await;
    if !client.supports_transactions() {
        log_uncaptured("Skipping convenient_api_retry_timeout_callback: no transaction support.");
        return;
    }
    let mut session = client.start_session(None).await.unwrap();
    session.convenient_transaction_timeout = Some(Duration::ZERO);
    let coll = client.database("test_convenient").collection::<Document>("test_convenient");

    let result: Result<()> = session.with_transaction(
        coll,
        |session, coll| async move {
            coll.find_one_with_session(None, None, session).await?;
            let mut err = Error::custom(42);
            err.add_label(TRANSIENT_TRANSACTION_ERROR);
            Err(err)
        }.boxed(),
        None,
    ).await;

    let err = result.unwrap_err();
    assert_eq!(&42, err.get_custom::<i32>().unwrap());
    assert!(err.contains_label(TRANSIENT_TRANSACTION_ERROR));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn convenient_api_retry_timeout_commit_unknown() {
    let _guard: _ = LOCK.run_exclusively().await;
    let client = crate::Client::test_builder()
        .options({
            let mut options = CLIENT_OPTIONS.get().await.clone();
            options.direct_connection = Some(true);
            options.hosts.drain(1..);    
            options
        })
        .event_client()
        .build()
        .await;
    if !client.supports_transactions() {
        log_uncaptured("Skipping convenient_api_retry_timeout_commit_unknown: no transaction support.");
        return;
    }
    let mut session = client.start_session(None).await.unwrap();
    session.convenient_transaction_timeout = Some(Duration::ZERO);
    let coll = client.database("test_convenient").collection::<Document>("test_convenient");

    let _fp = FailPoint::fail_command(
        &["commitTransaction"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .error_code(251)
            .error_labels(vec![UNKNOWN_TRANSACTION_COMMIT_RESULT.to_string()])
            .build(),
    )
    .enable(&client, None)
    .await.unwrap();

    let result = session.with_transaction(
        coll,
        |session, coll| async move {
            coll.find_one_with_session(None, None, session).await?;
            Ok(())
        }.boxed(),
        None,
    ).await;

    let err = result.unwrap_err();
    assert!(err.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT));
}


#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn convenient_api_retry_timeout_commit_transient() {
    let _guard: _ = LOCK.run_exclusively().await;
    let client = crate::Client::test_builder()
        .options({
            let mut options = CLIENT_OPTIONS.get().await.clone();
            options.direct_connection = Some(true);
            options.hosts.drain(1..);    
            options
        })
        .event_client()
        .build()
        .await;
    if !client.supports_transactions() {
        log_uncaptured("Skipping convenient_api_retry_timeout_commit_transient: no transaction support.");
        return;
    }
    let mut session = client.start_session(None).await.unwrap();
    session.convenient_transaction_timeout = Some(Duration::ZERO);
    let coll = client.database("test_convenient").collection::<Document>("test_convenient");

    let _fp = FailPoint::fail_command(
        &["commitTransaction"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .error_code(251)
            .error_labels(vec![TRANSIENT_TRANSACTION_ERROR.to_string()])
            .build(),
    )
    .enable(&client, None)
    .await.unwrap();

    let result = session.with_transaction(
        coll,
        |session, coll| async move {
            coll.find_one_with_session(None, None, session).await?;
            Ok(())
        }.boxed(),
        None,
    ).await;

    let err = result.unwrap_err();
    assert!(err.contains_label(TRANSIENT_TRANSACTION_ERROR));
}