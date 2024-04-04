use std::{sync::Arc, time::Duration};

use rand::{
    distributions::{Alphanumeric, DistString},
    thread_rng,
};

use crate::{
    bson::{doc, Document},
    error::ErrorKind,
    options::WriteModel,
    test::{
        get_client_options,
        log_uncaptured,
        spec::unified_runner::run_unified_tests,
        EventHandler,
        FailPoint,
        FailPointMode,
        TestClient,
    },
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
    let first_len = first_started.command.get_array("ops").unwrap().len();
    assert_eq!(first_len, num_models - 1);

    let (second_started, _) = subscriber
        .wait_for_successful_command_execution(Duration::from_millis(500), "bulkWrite")
        .await
        .expect("no events observed");
    let second_len = second_started.command.get_array("ops").unwrap().len();
    assert_eq!(second_len, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_concern_error_batches() {
    let mut options = get_client_options().await.clone();
    options.retry_writes = Some(false);

    let handler = Arc::new(EventHandler::new());
    let client = Client::test_builder()
        .options(options)
        .event_handler(handler.clone())
        .build()
        .await;
    let mut subscriber = handler.subscribe();

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping write_concern_error_batches: bulkWrite requires 8.0+");
        return;
    }

    let max_write_batch_size = client.server_info.max_write_batch_size.unwrap() as usize;

    let fail_point = FailPoint::new(&["bulkWrite"], FailPointMode::Times(2))
        .write_concern_error(doc! { "code": 91, "errmsg": "Replication is being shut down" });
    let _guard = client.configure_fail_point(fail_point).await.unwrap();

    let models = vec![
        WriteModel::InsertOne {
            namespace: Namespace::new("db", "coll"),
            document: doc! { "a": "b" }
        };
        max_write_batch_size + 1
    ];
    let error = client.bulk_write(models).ordered(false).await.unwrap_err();

    let ErrorKind::ClientBulkWrite(bulk_write_error) = *error.kind else {
        panic!("Expected bulk write error, got {:?}", error);
    };

    assert_eq!(bulk_write_error.write_concern_errors.len(), 2);

    let partial_result = bulk_write_error.partial_result.unwrap();
    assert_eq!(
        partial_result.inserted_count as usize,
        max_write_batch_size + 1
    );
}

#[tokio::test]
async fn write_error_batches() {
    let client = TestClient::new().await;

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping write_error_batches: bulkWrite requires 8.0+");
        return;
    }

    let max_write_batch_size = client.server_info.max_write_batch_size.unwrap() as usize;

    let document = doc! { "_id": 1 };
    let collection = client.database("db").collection("coll");
    collection.drop().await.unwrap();
    collection.insert_one(document.clone(), None).await.unwrap();

    let models = vec![
        WriteModel::InsertOne {
            namespace: collection.namespace(),
            document,
        };
        max_write_batch_size + 1
    ];

    let error = client
        .bulk_write(models.clone())
        .ordered(false)
        .await
        .unwrap_err();

    let ErrorKind::ClientBulkWrite(bulk_write_error) = *error.kind else {
        panic!("Expected bulk write error, got {:?}", error);
    };

    assert_eq!(
        bulk_write_error.write_errors.len(),
        max_write_batch_size + 1
    );

    let error = client.bulk_write(models).await.unwrap_err();

    let ErrorKind::ClientBulkWrite(bulk_write_error) = *error.kind else {
        panic!("Expected bulk write error, got {:?}", error);
    };

    assert_eq!(bulk_write_error.write_errors.len(), 1);
}

#[tokio::test]
async fn cursor_iteration() {
    let handler = Arc::new(EventHandler::new());
    let client = Client::test_builder()
        .event_handler(handler.clone())
        .build()
        .await;
    let mut subscriber = handler.subscribe();

    let max_bson_object_size = client.server_info.max_bson_object_size as usize;
    let max_write_batch_size = client.server_info.max_write_batch_size.unwrap() as usize;
    let id_size = max_bson_object_size / max_write_batch_size;

    let document = doc! { "_id": Alphanumeric.sample_string(&mut thread_rng(), id_size) };
    client
        .database("bulk")
        .collection::<Document>("write")
        .insert_one(&document, None)
        .await
        .unwrap();

    let models = vec![
        WriteModel::InsertOne {
            namespace: Namespace::new("bulk", "write"),
            document
        };
        max_write_batch_size
    ];
    let error = client.bulk_write(models).ordered(false).await.unwrap_err();

    assert!(error.source.is_none());

    let ErrorKind::ClientBulkWrite(bulk_write_error) = *error.kind else {
        panic!("Expected bulk write error, got {:?}", error);
    };

    assert!(bulk_write_error.write_concern_errors.is_empty());

    let write_errors = bulk_write_error.write_errors;
    assert_eq!(write_errors.len(), max_write_batch_size);

    subscriber
        .wait_for_successful_command_execution(Duration::from_millis(500), "getMore")
        .await
        .expect("no getMore observed");
}
