use std::time::Duration;

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};

use crate::{
    bson::{doc, Document},
    error::{Error, Result, TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT},
    test::{
        get_client_options,
        log_uncaptured,
        spec::{unified_runner::run_unified_tests, v2_runner::run_v2_tests},
        util::fail_point::{FailPoint, FailPointMode},
        TestClient,
    },
    Client,
    Collection,
};

#[tokio::test(flavor = "multi_thread")]
async fn run_legacy() {
    run_v2_tests(&["transactions", "legacy"])
        // TODO RUST-582: unskip this file
        .skip_files(&["error-labels-blockConnection.json"])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_legacy_convenient_api() {
    run_v2_tests(&["transactions-convenient-api"]).await;
}

// TODO RUST-902: Reduce transactionLifetimeLimitSeconds.
#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    run_unified_tests(&["transactions", "unified"])
        // TODO RUST-1656: unskip these files
        .skip_files(&["retryable-abort-handshake.json", "retryable-commit-handshake.json"])
        .await;
}

// This test checks that deserializing an operation correctly still retrieves the recovery token.
#[tokio::test(flavor = "multi_thread")]
#[function_name::named]
async fn deserialize_recovery_token() {
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

    let mut session = client.start_session().await.unwrap();

    // Insert a document with schema A.
    client
        .database(function_name!())
        .collection::<Document>(function_name!())
        .drop()
        .await
        .unwrap();
    client
        .database(function_name!())
        .create_collection(function_name!())
        .await
        .unwrap();
    let coll = client
        .database(function_name!())
        .collection(function_name!());
    coll.insert_one(A { num: 4 }).await.unwrap();

    // Attempt to execute Find on a document with schema B.
    let coll: Collection<B> = client
        .database(function_name!())
        .collection(function_name!());
    session.start_transaction().await.unwrap();
    assert!(session.transaction.recovery_token.is_none());
    let result = coll.find_one(doc! {}).session(&mut session).await;
    assert!(result.is_err()); // Assert that the deserialization failed.

    // Nevertheless, the recovery token should have been retrieved from the ok: 1 response.
    assert!(session.transaction.recovery_token.is_some());
}

#[tokio::test]
async fn convenient_api_custom_error() {
    #[allow(deprecated)]
    let client = Client::test_builder().monitor_events().build().await;
    if !client.supports_transactions() {
        log_uncaptured("Skipping convenient_api_custom_error: no transaction support.");
        return;
    }
    let mut session = client.start_session().await.unwrap();
    let coll = client
        .database("test_convenient")
        .collection::<Document>("test_convenient");

    struct MyErr;
    let result: Result<()> = session
        .start_transaction()
        .and_run(coll, |session, coll| {
            async move {
                coll.find_one(doc! {}).session(session).await?;
                Err(Error::custom(MyErr))
            }
            .boxed()
        })
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().get_custom::<MyErr>().is_some());
    #[allow(deprecated)]
    let events = client.events.get_all_command_started_events();
    let commands: Vec<_> = events.iter().map(|ev| &ev.command_name).collect();
    assert_eq!(&["find", "abortTransaction"], &commands[..]);
}

#[tokio::test]
async fn convenient_api_returned_value() {
    #[allow(deprecated)]
    let client = Client::test_builder().monitor_events().build().await;
    if !client.supports_transactions() {
        log_uncaptured("Skipping convenient_api_returned_value: no transaction support.");
        return;
    }
    let mut session = client.start_session().await.unwrap();
    let coll = client
        .database("test_convenient")
        .collection::<Document>("test_convenient");

    let value = session
        .start_transaction()
        .and_run(coll, |session, coll| {
            async move {
                coll.find_one(doc! {}).session(session).await?;
                Ok(42)
            }
            .boxed()
        })
        .await
        .unwrap();

    assert_eq!(42, value);
}

#[tokio::test]
async fn convenient_api_retry_timeout_callback() {
    #[allow(deprecated)]
    let client = Client::test_builder().monitor_events().build().await;
    if !client.supports_transactions() {
        log_uncaptured("Skipping convenient_api_retry_timeout_callback: no transaction support.");
        return;
    }
    let mut session = client.start_session().await.unwrap();
    session.convenient_transaction_timeout = Some(Duration::ZERO);
    let coll = client
        .database("test_convenient")
        .collection::<Document>("test_convenient");

    let result: Result<()> = session
        .start_transaction()
        .and_run(coll, |session, coll| {
            async move {
                coll.find_one(doc! {}).session(session).await?;
                let mut err = Error::custom(42);
                err.add_label(TRANSIENT_TRANSACTION_ERROR);
                Err(err)
            }
            .boxed()
        })
        .await;

    let err = result.unwrap_err();
    assert_eq!(&42, err.get_custom::<i32>().unwrap());
    assert!(err.contains_label(TRANSIENT_TRANSACTION_ERROR));
}

#[tokio::test(flavor = "multi_thread")]
async fn convenient_api_retry_timeout_commit_unknown() {
    let mut options = get_client_options().await.clone();
    if Client::test_builder().build().await.is_sharded() {
        options.direct_connection = Some(true);
        options.hosts.drain(1..);
    }
    #[allow(deprecated)]
    let client = Client::test_builder()
        .options(options)
        .monitor_events()
        .build()
        .await;
    if !client.supports_transactions() {
        log_uncaptured(
            "Skipping convenient_api_retry_timeout_commit_unknown: no transaction support.",
        );
        return;
    }
    let mut session = client.start_session().await.unwrap();
    session.convenient_transaction_timeout = Some(Duration::ZERO);
    let coll = client
        .database("test_convenient")
        .collection::<Document>("test_convenient");

    let fail_point = FailPoint::fail_command(&["commitTransaction"], FailPointMode::Times(1))
        .error_code(251)
        .error_labels(vec![UNKNOWN_TRANSACTION_COMMIT_RESULT]);
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    let result = session
        .start_transaction()
        .and_run(coll, |session, coll| {
            async move {
                coll.find_one(doc! {}).session(session).await?;
                Ok(())
            }
            .boxed()
        })
        .await;

    let err = result.unwrap_err();
    assert_eq!(Some(251), err.sdam_code());
}

#[tokio::test(flavor = "multi_thread")]
async fn convenient_api_retry_timeout_commit_transient() {
    let mut options = get_client_options().await.clone();
    if Client::test_builder().build().await.is_sharded() {
        options.direct_connection = Some(true);
        options.hosts.drain(1..);
    }
    #[allow(deprecated)]
    let client = Client::test_builder()
        .options(options)
        .monitor_events()
        .build()
        .await;
    if !client.supports_transactions() {
        log_uncaptured(
            "Skipping convenient_api_retry_timeout_commit_transient: no transaction support.",
        );
        return;
    }
    let mut session = client.start_session().await.unwrap();
    session.convenient_transaction_timeout = Some(Duration::ZERO);
    let coll = client
        .database("test_convenient")
        .collection::<Document>("test_convenient");

    let fail_point = FailPoint::fail_command(&["commitTransaction"], FailPointMode::Times(1))
        .error_code(251)
        .error_labels(vec![TRANSIENT_TRANSACTION_ERROR]);
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    let result = session
        .start_transaction()
        .and_run(coll, |session, coll| {
            async move {
                coll.find_one(doc! {}).session(session).await?;
                Ok(())
            }
            .boxed()
        })
        .await;

    let err = result.unwrap_err();
    assert!(err.contains_label(TRANSIENT_TRANSACTION_ERROR));
}
