use futures::StreamExt;
use opentelemetry::SpanId;

use crate::{
    bson::{doc, Document},
    otel::testing::{ClientTracing, ObserveTracingMessages},
    test::{
        get_client_options,
        log_uncaptured,
        spec::unified_runner::run_unified_tests,
        transactions_supported,
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
