use std::{sync::Arc, time::Duration};

use crate::{
    action::bulk_write::write_models::WriteModel,
    bson::doc,
    test::{spec::unified_runner::run_unified_tests, EventHandler},
    Client,
    Namespace,
};

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    run_unified_tests(&["crud", "unified", "new-bulk-write"]).await;
}

#[tokio::test]
async fn command_batching() {
    let handler = Arc::new(EventHandler::new());
    let client = Client::test_builder()
        .event_handler(handler.clone())
        .build()
        .await;
    let mut subscriber = handler.subscribe();

    let max_object_size = client.server_info.max_bson_object_size as usize;
    let max_message_size = client.server_info.max_message_size_bytes as usize;

    let namespace = Namespace::new("command_batching", "command_batching");
    let large_doc = doc! {"a": "b".repeat(max_object_size / 2)};
    let models = vec![
        WriteModel::InsertOne {
            namespace: namespace.clone(),
            document: large_doc,
        };
        3
    ];
    client.bulk_write(models).await.unwrap();

    let (started, _) = subscriber
        .wait_for_successful_command_execution(Duration::from_millis(500), "bulkWrite")
        .await
        .expect("no events observed");
    let ops = started.command.get_array("ops").unwrap();
    assert_eq!(ops.len(), 3);

    let large_doc = doc! { "a": "b".repeat(max_object_size - 5000) };
    let num_models = max_message_size / max_object_size + 1;
    let models = vec![
        WriteModel::InsertOne {
            namespace: namespace.clone(),
            document: large_doc
        };
        num_models
    ];
    client.bulk_write(models).await.unwrap();

    let (first_started, _) = subscriber
        .wait_for_successful_command_execution(Duration::from_millis(500), "bulkWrite")
        .await
        .expect("no events observed");
    let first_len = first_started.command.get_array("ops").unwrap().len();
    assert!(first_len < num_models);

    let (second_started, _) = subscriber
        .wait_for_successful_command_execution(Duration::from_millis(500), "bulkWrite")
        .await
        .expect("no events observed");
    let second_len = second_started.command.get_array("ops").unwrap().len();
    assert_eq!(first_len + second_len, num_models);
}
