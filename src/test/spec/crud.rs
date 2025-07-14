use crate::{
    bson::doc,
    test::{log_uncaptured, server_version_lt, spec::unified_runner::run_unified_tests},
    Client,
};

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    let skipped_files = vec![
        // The Rust driver does not support unacknowledged writes (and does not intend to in
        // the future).
        "bulkWrite-deleteMany-hint-unacknowledged.json",
        "bulkWrite-deleteOne-hint-unacknowledged.json",
        "bulkWrite-replaceOne-hint-unacknowledged.json",
        "bulkWrite-updateMany-hint-unacknowledged.json",
        "bulkWrite-updateOne-hint-unacknowledged.json",
        "deleteMany-hint-unacknowledged.json",
        "deleteOne-hint-unacknowledged.json",
        "findOneAndDelete-hint-unacknowledged.json",
        "findOneAndReplace-hint-unacknowledged.json",
        "findOneAndUpdate-hint-unacknowledged.json",
        "replaceOne-hint-unacknowledged.json",
        "updateMany-hint-unacknowledged.json",
        "updateOne-hint-unacknowledged.json",
        // TODO RUST-1405: unskip the errorResponse tests
        "client-bulkWrite-errorResponse.json",
        "bulkWrite-errorResponse.json",
        "updateOne-errorResponse.json",
        "insertOne-errorResponse.json",
        "deleteOne-errorResponse.json",
        "aggregate-merge-errorResponse.json",
        "findOneAndUpdate-errorResponse.json",
    ];

    let skipped_tests = vec![
        // Unacknowledged write; see above.
        "Unacknowledged write using dollar-prefixed or dotted keys may be silently rejected on \
         pre-5.0 server",
        "Requesting unacknowledged write with verboseResults is a client-side error",
        "Requesting unacknowledged write with ordered is a client-side error",
    ];

    run_unified_tests(&["crud", "unified"])
        .skip_files(&skipped_files)
        .skip_tests(&skipped_tests)
        .await;
}

#[tokio::test]
async fn generated_id_first_field() {
    let client = Client::for_test().monitor_events().await;
    let events = &client.events;
    let collection = client.database("db").collection("coll");

    collection.insert_one(doc! { "x": 1 }).await.unwrap();
    let insert_events = events.get_command_started_events(&["insert"]);
    let insert_document = insert_events[0]
        .command
        .get_array("documents")
        .unwrap()
        .first()
        .unwrap()
        .as_document()
        .unwrap();
    let (key, _) = insert_document.iter().next().unwrap();
    assert_eq!(key, "_id");

    if server_version_lt(8, 0).await {
        log_uncaptured("skipping bulk write test in generated_id_first_field");
        return;
    }

    let insert_one_model = collection.insert_one_model(doc! { "y": 2 }).unwrap();
    client.bulk_write(vec![insert_one_model]).await.unwrap();
    let bulk_write_events = events.get_command_started_events(&["bulkWrite"]);
    let insert_operation = bulk_write_events[0]
        .command
        .get_array("ops")
        .unwrap()
        .first()
        .unwrap()
        .as_document()
        .unwrap();
    assert!(insert_operation.contains_key("insert"));
    let insert_document = insert_operation.get_document("document").unwrap();
    let (key, _) = insert_document.iter().next().unwrap();
    assert_eq!(key, "_id");
}
