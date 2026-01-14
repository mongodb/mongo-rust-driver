use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::{
    bson::{doc, Document},
    error::{Error, Result, TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT},
    options::{CollectionOptions, WriteConcern},
    test::{
        get_client_options,
        log_uncaptured,
        spec::unified_runner::run_unified_tests,
        topology_is_sharded,
        transactions_supported,
        util::fail_point::{FailPoint, FailPointMode},
    },
    Client,
    Collection,
};

// TODO RUST-902: Reduce transactionLifetimeLimitSeconds.
#[tokio::test(flavor = "multi_thread")]
async fn run_unified_base_api() {
    run_unified_tests(&["transactions", "unified"])
        // TODO RUST-1656: unskip these files
        .skip_files(&["retryable-abort-handshake.json", "retryable-commit-handshake.json"])
        // The driver doesn't support socketTimeoutMS
        .skip_tests(&["add RetryableWriteError and UnknownTransactionCommitResult labels to connection errors"])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_unified_convenient_api() {
    run_unified_tests(&["transactions-convenient-api", "unified"]).await;
}

// This test checks that deserializing an operation correctly still retrieves the recovery token.
#[tokio::test(flavor = "multi_thread")]
#[function_name::named]
async fn deserialize_recovery_token() {
    if !topology_is_sharded().await {
        log_uncaptured("skipping deserialize_recovery_token due to test topology");
        return;
    }

    #[derive(Debug, Serialize)]
    struct A {
        num: i32,
    }

    #[derive(Debug, Deserialize)]
    struct B {
        _str: String,
    }

    let client = Client::for_test().await;

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
    if !transactions_supported().await {
        log_uncaptured("Skipping convenient_api_custom_error: no transaction support.");
        return;
    }

    let client = Client::for_test().monitor_events().await;
    let mut session = client.start_session().await.unwrap();
    let coll = client
        .database("test_convenient")
        .collection::<Document>("test_convenient");

    struct MyErr;
    let result: Result<()> = session
        .start_transaction()
        .and_run2(async move |session| {
            coll.find_one(doc! {}).session(session).await?;
            Err(Error::custom(MyErr))
        })
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().get_custom::<MyErr>().is_some());

    let events = client.events.get_all_command_started_events();
    let commands: Vec<_> = events.iter().map(|ev| &ev.command_name).collect();
    assert_eq!(&["find", "abortTransaction"], &commands[..]);
}

#[tokio::test]
async fn convenient_api_returned_value() {
    if !transactions_supported().await {
        log_uncaptured("Skipping convenient_api_returned_value: no transaction support.");
        return;
    }

    let client = Client::for_test().monitor_events().await;
    let mut session = client.start_session().await.unwrap();
    let coll = client
        .database("test_convenient")
        .collection::<Document>("test_convenient");

    let value = session
        .start_transaction()
        .and_run2(async move |session| {
            coll.find_one(doc! {}).session(session).await?;
            Ok(42)
        })
        .await
        .unwrap();

    assert_eq!(42, value);
}

#[tokio::test]
async fn convenient_api_retry_timeout_callback() {
    if !transactions_supported().await {
        log_uncaptured("Skipping convenient_api_retry_timeout_callback: no transaction support.");
        return;
    }

    let client = Client::for_test().monitor_events().await;
    let mut session = client.start_session().await.unwrap();
    session.convenient_transaction_timeout = Some(Duration::ZERO);
    let coll = client
        .database("test_convenient")
        .collection::<Document>("test_convenient");

    let result: Result<()> = session
        .start_transaction()
        .and_run2(async move |session| {
            coll.find_one(doc! {}).session(session).await?;
            let mut err = Error::custom(42);
            err.add_label(TRANSIENT_TRANSACTION_ERROR);
            Err(err)
        })
        .await;

    let err = result.unwrap_err();
    assert_eq!(&42, err.get_custom::<i32>().unwrap());
    assert!(err.contains_label(TRANSIENT_TRANSACTION_ERROR));
}

#[tokio::test(flavor = "multi_thread")]
async fn convenient_api_retry_timeout_commit_unknown() {
    if !transactions_supported().await {
        log_uncaptured(
            "Skipping convenient_api_retry_timeout_commit_unknown: no transaction support.",
        );
        return;
    }

    let mut options = get_client_options().await.clone();
    if topology_is_sharded().await {
        options.direct_connection = Some(true);
        options.hosts.drain(1..);
    }

    let client = Client::for_test().options(options).monitor_events().await;
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
        .and_run2(async move |session| {
            coll.find_one(doc! {}).session(session).await?;
            Ok(())
        })
        .await;

    let err = result.unwrap_err();
    assert_eq!(Some(251), err.sdam_code());
}

#[tokio::test(flavor = "multi_thread")]
async fn convenient_api_retry_timeout_commit_transient() {
    if !transactions_supported().await {
        log_uncaptured(
            "Skipping convenient_api_retry_timeout_commit_transient: no transaction support.",
        );
        return;
    }

    let mut options = get_client_options().await.clone();
    if topology_is_sharded().await {
        options.direct_connection = Some(true);
        options.hosts.drain(1..);
    }

    let client = Client::for_test().options(options).monitor_events().await;
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
        .and_run2(async move |session| {
            coll.find_one(doc! {}).session(session).await?;
            Ok(())
        })
        .await;

    let err = result.unwrap_err();
    assert!(err.contains_label(TRANSIENT_TRANSACTION_ERROR));
}

#[tokio::test]
async fn write_concern_not_inherited() {
    if !transactions_supported().await {
        log_uncaptured("Skipping write_concern_not_inherited: no transaction support.");
        return;
    }

    let client = Client::for_test().await;
    let db = client.database("write_concern_not_inherited");
    let coll: Collection<Document> = db.collection_with_options(
        "test",
        CollectionOptions::builder()
            .write_concern(WriteConcern::nodes(0))
            .build(),
    );
    let _ = coll.drop().write_concern(WriteConcern::majority()).await;
    db.create_collection(coll.name()).await.unwrap();

    let mut session = client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    coll.insert_one(doc! { "n": 1 })
        .session(&mut session)
        .await
        .unwrap();
    session.commit_transaction().await.unwrap();

    assert!(coll.find_one(doc! { "n": 1 }).await.unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn convenient_api_retry_backoff_is_enforced() {
    if !transactions_supported().await {
        log_uncaptured("skipping convenient_retry_backoff_is_enforced: transactions not supported");
        return;
    }

    async fn run_test(jitter: f64) -> Duration {
        let mut options = get_client_options().await.clone();
        if topology_is_sharded().await {
            options.hosts.drain(1..);
        }
        let client = Client::for_test().options(options).await;
        let coll = client
            .database("db")
            .collection("convenient_retry_backoff_is_enforced");

        let fail_point = FailPoint::fail_command(&["commitTransaction"], FailPointMode::Times(13))
            .error_code(251);
        let _guard = client.enable_fail_point(fail_point).await.unwrap();

        let mut session = client.start_session().await.unwrap();
        session.convenient_transaction_jitter = Some(jitter);

        let start = Instant::now();
        session
            .start_transaction()
            .and_run2(async move |session| coll.insert_one(doc! {}).session(session).await)
            .await
            .unwrap();
        start.elapsed()
    }

    let no_jitter = run_test(0f64).await;
    let with_jitter = run_test(1f64).await;

    let difference = with_jitter.abs_diff(no_jitter + Duration::from_millis(1800));
    assert!(difference < Duration::from_millis(500));
}
