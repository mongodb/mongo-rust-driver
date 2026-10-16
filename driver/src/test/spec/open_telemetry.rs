use futures::StreamExt;
use opentelemetry::SpanId;

use crate::{
    bson::{doc, Document},
    otel::testing::{ClientTracing, ObserveTracingMessages},
    test::{
        get_client_options,
        log_uncaptured,
        spec::unified_runner::run_unified_tests,
        topology_is_standalone,
        transactions_supported,
        util::fail_point::{FailPoint, FailPointMode},
    },
    Client,
};

#[tokio::test(flavor = "multi_thread")]
async fn run_unified_operation() {
    run_unified_tests(&["open-telemetry", "operation"]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_unified_transaction() {
    run_unified_tests(&["open-telemetry", "transaction"]).await;
}

// Prose Test 3: getMore inside a withTransaction callback nests under the transaction span
#[tokio::test]
async fn with_transaction_get_more_nests() {
    if !transactions_supported().await {
        log_uncaptured("skipping with_transaction_get_more_nests: requires transactions");
        return;
    }

    // 1. Create a MongoClient with tracing enabled.
    let mut options = get_client_options().await.clone();
    let (tracing, tracing_opts) = ClientTracing::new(&ObserveTracingMessages::default());
    options.tracing = Some(tracing_opts);
    let client = Client::for_test().options(options).await;

    // 2. Insert three documents into a test collection.
    let coll = client
        .database("with_transaction_get_more_nests")
        .collection::<Document>("test");
    coll.drop().await.unwrap();
    coll.insert_many([
        doc! {"name": "doc one"},
        doc! {"name": "doc two"},
        doc! {"name": "doc three"},
    ])
    .await
    .unwrap();

    // 3. Start a session and call withTransaction. In the callback, create a cursor over that
    //    collection with find and a batchSize of 2, then iterate the cursor until it is exhausted.
    //    This sends exactly one getMore inside the transaction.
    let mut session = client.start_session().await.unwrap();
    session
        .start_transaction()
        .and_run2(async move |session| {
            let mut cursor = coll
                .find(doc! {})
                .session(&mut *session)
                .batch_size(2)
                .await?;
            let mut stream = cursor.stream(session);
            while let Some(_) = stream.next().await {}

            Ok(())
        })
        .await
        .unwrap();

    // 4. Assert that a transaction span was emitted, and that both the find operation span and the
    //    getMore operation span are nested directly under it.
    let spans = tracing.get_spans();
    let root_spans = spans.get(&SpanId::INVALID).expect("root spans");
    let txn_span = root_spans
        .iter()
        .find(|s| s.name == "transaction")
        .expect("transaction span");
    let child_spans = spans
        .get(&txn_span.span_context.span_id())
        .expect("transaction child spans");
    assert!(child_spans
        .iter()
        .any(|s| s.name == "find with_transaction_get_more_nests.test"));
    assert!(child_spans
        .iter()
        .any(|s| s.name == "getMore with_transaction_get_more_nests.test"));
}

// Prose Test 4: getMore records the cursor id it sent, not the cursor id returned
#[tokio::test]
async fn get_more_cursor_id() {
    // 1. Create a MongoClient with tracing enabled.
    let mut options = get_client_options().await.clone();
    let (tracing, tracing_opts) = ClientTracing::new(&ObserveTracingMessages::default());
    options.tracing = Some(tracing_opts);
    let client = Client::for_test().options(options).await;

    // 2. Insert three documents into a test collection.
    let coll = client
        .database("get_more_cursor_id")
        .collection::<Document>("test");
    coll.drop().await.unwrap();
    coll.insert_many([
        doc! {"name": "doc one"},
        doc! {"name": "doc two"},
        doc! {"name": "doc three"},
    ])
    .await
    .unwrap();

    // 3. Create a cursor over that collection with find and a batchSize of 2. Consume the first
    //    batch, then record the cursor id before iterating further.
    let mut cursor = coll.find(doc! {}).batch_size(2).await.unwrap();
    cursor.next().await.unwrap().unwrap();
    cursor.next().await.unwrap().unwrap();
    let cursor_id = cursor.raw().unwrap().id();

    // 4. Iterate the cursor until it is exhausted. This sends exactly one getMore, and the server's
    //    reply to that getMore returns a cursor id of 0.
    while let Some(_) = cursor.next().await {}

    // 5. Assert that both the getMore operation span and the getMore command span have a
    //    db.mongodb.cursor_id attribute whose value equals the cursor id recorded in step 3.
    let spans = tracing.get_spans();
    let op_span = spans
        .values()
        .flat_map(|v| v.iter())
        .find(|s| s.name == "getMore get_more_cursor_id.test")
        .expect("operation span");
    assert!(op_span
        .attributes
        .iter()
        .any(|a| a.key.as_str() == "db.mongodb.cursor_id"
            && a.value == opentelemetry::Value::I64(cursor_id)));
    let cmd_span = spans
        .get(&op_span.span_context.span_id())
        .expect("operation span children")
        .iter()
        .find(|s| s.name == "getMore")
        .expect("command span");
    assert!(cmd_span
        .attributes
        .iter()
        .any(|a| a.key.as_str() == "db.mongodb.cursor_id"
            && a.value == opentelemetry::Value::I64(cursor_id)));
}

// Prose Test 5: error.type is the exception class name for a non-server error
#[tokio::test(flavor = "multi_thread")]
async fn error_type_is_exception_type_non_server_err() {
    // Fail points can misdirect on replicated topologies.
    if !topology_is_standalone().await {
        log_uncaptured(
            "skipping error_type_is_exception_type_non_server_err: non-standalone topology",
        );
        return;
    }

    // 1. Create a MongoClient with tracing enabled and retryReads disabled.
    let mut options = get_client_options().await.clone();
    let (tracing, tracing_opts) = ClientTracing::new(&ObserveTracingMessages::default());
    options.tracing = Some(tracing_opts);
    options.retry_reads = Some(false);
    let client = Client::for_test().options(options).await;

    // 2. Configure a failCommand fail point on find with closeConnection: true.
    let fail_point =
        FailPoint::fail_command(&["find"], FailPointMode::AlwaysOn).close_connection(true);
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    // 3. Call find on a test collection and let it fail.
    let result = client
        .database("error_type_is_exception_type_non_server_err")
        .collection::<Document>("test")
        .find(doc! {})
        .await;
    assert!(result.is_err());

    // 4. Assert that the command span's error.type attribute equals its exception.type attribute.
    let spans = tracing.get_spans();
    let cmd_span = spans
        .values()
        .flat_map(|v| v.iter())
        .find(|s| s.name == "find")
        .expect("command span");
    let error_type = cmd_span
        .attributes
        .iter()
        .find(|a| a.key.as_str() == "error.type")
        .expect("error.type");
    let exception_type = cmd_span
        .attributes
        .iter()
        .find(|a| a.key.as_str() == "exception.type")
        .expect("exception.type");
    assert_eq!(error_type.value, exception_type.value);

    // 5. Assert that the operation span's error.type attribute equals its exception.type attribute.
    let op_span = spans
        .values()
        .flat_map(|v| v.iter())
        .find(|s| s.name == "find error_type_is_exception_type_non_server_err.test")
        .expect("operation span");
    let error_type = op_span
        .attributes
        .iter()
        .find(|a| a.key.as_str() == "error.type")
        .expect("error.type");
    let exception_type = op_span
        .attributes
        .iter()
        .find(|a| a.key.as_str() == "exception.type")
        .expect("exception.type");
    assert_eq!(error_type.value, exception_type.value);
}

// Prose Test 6: error.type on the operation span is the exception class name for a server error
#[tokio::test(flavor = "multi_thread")]
async fn error_type_is_exception_type_server_err() {
    // Fail points can misdirect on replicated topologies.
    if !topology_is_standalone().await {
        log_uncaptured("skipping error_type_is_exception_type_server_err: non-standalone topology");
        return;
    }

    // 1. Create a MongoClient with tracing enabled.
    let mut options = get_client_options().await.clone();
    let (tracing, tracing_opts) = ClientTracing::new(&ObserveTracingMessages::default());
    options.tracing = Some(tracing_opts);
    let client = Client::for_test().options(options).await;

    // 2. Configure a failCommand fail point on find with a non-retryable errorCode.
    let fail_point = FailPoint::fail_command(&["find"], FailPointMode::AlwaysOn).error_code(1234);
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    // 3. Call find on a test collection and let it fail.
    let result = client
        .database("error_type_is_exception_type_non_server_err")
        .collection::<Document>("test")
        .find(doc! {})
        .await;
    assert!(result.is_err());

    // 4. Assert that the operation span's error.type attribute equals its exception.type attribute,
    // and that both differ from the db.response.status_code attribute on the find command span.
    let spans = tracing.get_spans();
    let cmd_span = spans
        .values()
        .flat_map(|v| v.iter())
        .find(|s| s.name == "find")
        .expect("command span");
    let status_code = cmd_span
        .attributes
        .iter()
        .find(|a| a.key.as_str() == "db.response.status_code")
        .expect("db.response.status_code");
    let op_span = spans
        .values()
        .flat_map(|v| v.iter())
        .find(|s| s.name == "find error_type_is_exception_type_non_server_err.test")
        .expect("operation span");
    let error_type = op_span
        .attributes
        .iter()
        .find(|a| a.key.as_str() == "error.type")
        .expect("error.type");
    let exception_type = op_span
        .attributes
        .iter()
        .find(|a| a.key.as_str() == "exception.type")
        .expect("exception.type");
    assert_eq!(error_type.value, exception_type.value);
    assert_ne!(error_type.value, status_code.value);
    assert_ne!(exception_type.value, status_code.value);
}
