use std::{sync::Arc, time::Duration};

use crate::{
    bson::doc,
    options::WriteModel,
    test::{log_uncaptured, spec::unified_runner::run_unified_tests, EventHandler},
    Client,
    Namespace,
};

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    run_unified_tests(&["crud", "unified", "new-bulk-write"]).await;
}

#[tokio::test]
async fn max_write_batch_size_batching() {
    let handler = Arc::new(EventHandler::new());
    let client = Client::test_builder()
        .event_handler(handler.clone())
        .build()
        .await;
    let mut subscriber = handler.subscribe();

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping max_write_batch_size_batching: bulkWrite requires 8.0+");
        return;
    }

    let max_write_batch_size = client.server_info.max_write_batch_size.unwrap() as usize;

    let model = WriteModel::InsertOne {
        namespace: Namespace::new("db", "coll"),
        document: doc! { "a": "b" },
    };
    let models = vec![model; max_write_batch_size + 1];

    let result = client.bulk_write(models).await.unwrap();
    assert_eq!(result.inserted_count as usize, max_write_batch_size + 1);

    let (first_started, _) = subscriber
        .wait_for_successful_command_execution(Duration::from_millis(500), "bulkWrite")
        .await
        .expect("no events observed");
    let first_len = first_started.command.get_array("ops").unwrap().len();
    assert_eq!(first_len, max_write_batch_size);

    let (second_started, _) = subscriber
        .wait_for_successful_command_execution(Duration::from_millis(500), "bulkWrite")
        .await
        .expect("no events observed");
    let second_len = second_started.command.get_array("ops").unwrap().len();
    assert_eq!(second_len, 1);
}

#[tokio::test]
async fn max_bson_object_size_with_document_sequences() {
    let handler = Arc::new(EventHandler::new());
    let client = Client::test_builder()
        .event_handler(handler.clone())
        .build()
        .await;
    let mut subscriber = handler.subscribe();

    if client.server_version_lt(8, 0) {
        log_uncaptured(
            "skipping max_bson_object_size_with_document_sequences: bulkWrite requires 8.0+",
        );
        return;
    }

    let max_bson_object_size = client.server_info.max_bson_object_size as usize;

    let document = doc! { "a": "b".repeat(max_bson_object_size / 2) };
    let model = WriteModel::InsertOne {
        namespace: Namespace::new("db", "coll"),
        document,
    };
    let models = vec![model; 2];

    let result = client.bulk_write(models).await.unwrap();
    assert_eq!(result.inserted_count as usize, 2);

    let (started, _) = subscriber
        .wait_for_successful_command_execution(Duration::from_millis(500), "bulkWrite")
        .await
        .expect("no events observed");
    let len = started.command.get_array("ops").unwrap().len();
    assert_eq!(len, 2);
}

#[tokio::test]
async fn max_message_size_bytes_batching() {
    let handler = Arc::new(EventHandler::new());
    let client = Client::test_builder()
        .event_handler(handler.clone())
        .build()
        .await;
    let mut subscriber = handler.subscribe();

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping max_message_size_bytes_batching: bulkWrite requires 8.0+");
        return;
    }

    let max_bson_object_size = client.server_info.max_bson_object_size as usize;
    let max_message_size_bytes = client.server_info.max_message_size_bytes as usize;

    let document = doc! { "a": "b".repeat(max_bson_object_size - 500) };
    let model = WriteModel::InsertOne {
        namespace: Namespace::new("db", "coll"),
        document,
    };
    let num_models = max_message_size_bytes / max_bson_object_size + 1;
    let models = vec![model; num_models];

    let result = client.bulk_write(models).await.unwrap();
    assert_eq!(result.inserted_count as usize, num_models);

    let (first_started, _) = subscriber
        .wait_for_successful_command_execution(Duration::from_millis(500), "bulkWrite")
        .await
        .expect("no events observed");
    let first_ops_len = first_started.command.get_array("ops").unwrap().len();

    let (second_started, _) = subscriber
        .wait_for_successful_command_execution(Duration::from_millis(500), "bulkWrite")
        .await
        .expect("no events observed");
    let second_ops_len = second_started.command.get_array("ops").unwrap().len();

    assert_eq!(first_ops_len + second_ops_len, num_models);
}
