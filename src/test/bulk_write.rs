use crate::{
    bson::doc,
    error::{ClientBulkWriteError, ErrorKind},
    options::WriteModel,
    test::{
        get_client_options,
        log_uncaptured,
        spec::unified_runner::run_unified_tests,
        util::event_buffer::EventBuffer,
        FailPoint,
        FailPointMode,
    },
    Client,
    Namespace,
};

use super::TestClient;

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    run_unified_tests(&["crud", "unified", "new-bulk-write"])
        // TODO RUST-1405: unskip this test
        .skip_files(&["client-bulkWrite-errorResponse.json"])
        .await;
}

#[tokio::test]
async fn max_write_batch_size_batching() {
    let event_buffer = EventBuffer::new();
    let client = Client::test_builder()
        .event_buffer(event_buffer.clone())
        .build()
        .await;

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

    let mut command_started_events = event_buffer
        .get_command_started_events(&["bulkWrite"])
        .into_iter();

    let first_event = command_started_events
        .next()
        .expect("no first event observed");
    let first_len = first_event.command.get_array("ops").unwrap().len();
    assert_eq!(first_len, max_write_batch_size);

    let second_event = command_started_events
        .next()
        .expect("no second event observed");
    let second_len = second_event.command.get_array("ops").unwrap().len();
    assert_eq!(second_len, 1);
}

#[tokio::test]
async fn max_message_size_bytes_batching() {
    let event_buffer = EventBuffer::new();
    let client = Client::test_builder()
        .event_buffer(event_buffer.clone())
        .build()
        .await;

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

    let mut command_started_events = event_buffer
        .get_command_started_events(&["bulkWrite"])
        .into_iter();

    let first_event = command_started_events
        .next()
        .expect("no first event observed");
    let first_len = first_event.command.get_array("ops").unwrap().len();
    assert_eq!(first_len, num_models - 1);

    let second_event = command_started_events
        .next()
        .expect("no second event observed");
    let second_len = second_event.command.get_array("ops").unwrap().len();
    assert_eq!(second_len, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_concern_error_batches() {
    let mut options = get_client_options().await.clone();
    options.retry_writes = Some(false);
    if TestClient::new().await.is_sharded() {
        options.hosts.drain(1..);
    }

    let event_buffer = EventBuffer::new();
    let client = Client::test_builder()
        .options(options)
        .event_buffer(event_buffer.clone())
        .build()
        .await;

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping write_concern_error_batches: bulkWrite requires 8.0+");
        return;
    }

    let max_write_batch_size = client.server_info.max_write_batch_size.unwrap() as usize;

    let fail_point = FailPoint::new(&["bulkWrite"], FailPointMode::Times(2))
        .write_concern_error(doc! { "code": 91, "errmsg": "Replication is being shut down" });
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

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
    let mut event_buffer = EventBuffer::new();
    let client = Client::test_builder()
        .event_buffer(event_buffer.clone())
        .build()
        .await;

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping write_error_batches: bulkWrite requires 8.0+");
        return;
    }

    let max_write_batch_size = client.server_info.max_write_batch_size.unwrap() as usize;

    let document = doc! { "_id": 1 };
    let collection = client.database("db").collection("coll");
    collection.drop().await.unwrap();
    collection.insert_one(document.clone()).await.unwrap();

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

    let command_started_events = event_buffer.get_command_started_events(&["bulkWrite"]);
    assert_eq!(command_started_events.len(), 2);

    event_buffer.clear_cached_events();

    let error = client.bulk_write(models).ordered(true).await.unwrap_err();

    let ErrorKind::ClientBulkWrite(bulk_write_error) = *error.kind else {
        panic!("Expected bulk write error, got {:?}", error);
    };

    assert_eq!(bulk_write_error.write_errors.len(), 1);

    let command_started_events = event_buffer.get_command_started_events(&["bulkWrite"]);
    assert_eq!(command_started_events.len(), 1);
}

#[tokio::test]
async fn successful_cursor_iteration() {
    let event_buffer = EventBuffer::new();
    let client = Client::test_builder()
        .event_buffer(event_buffer.clone())
        .build()
        .await;

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping successful_cursor_iteration: bulkWrite requires 8.0+");
        return;
    }

    let max_bson_object_size = client.server_info.max_bson_object_size as usize;

    let collection = client.database("db").collection::<bson::Document>("coll");
    collection.drop().await.unwrap();

    let models = vec![
        WriteModel::UpdateOne {
            namespace: collection.namespace(),
            filter: doc! { "_id": "a".repeat(max_bson_object_size / 2) },
            update: doc! { "$set": { "x": 1 } }.into(),
            array_filters: None,
            collation: None,
            hint: None,
            upsert: Some(true),
        },
        WriteModel::UpdateOne {
            namespace: collection.namespace(),
            filter: doc! { "_id": "b".repeat(max_bson_object_size / 2) },
            update: doc! { "$set": { "x": 1 } }.into(),
            array_filters: None,
            collation: None,
            hint: None,
            upsert: Some(true),
        },
    ];

    let result = client
        .bulk_write(models)
        .verbose_results(true)
        .await
        .unwrap();
    assert_eq!(result.upserted_count, 2);
    assert_eq!(result.update_results.unwrap().len(), 2);

    let command_started_events = event_buffer.get_command_started_events(&["getMore"]);
    assert_eq!(command_started_events.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_cursor_iteration() {
    let mut options = get_client_options().await.clone();
    options.hosts.drain(1..);

    let event_buffer = EventBuffer::new();
    let client = Client::test_builder()
        .options(options)
        .event_buffer(event_buffer.clone())
        .build()
        .await;

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping failed_cursor_iteration: bulkWrite requires 8.0+");
        return;
    }

    let max_bson_object_size = client.server_info.max_bson_object_size as usize;

    let fail_point = FailPoint::new(&["getMore"], FailPointMode::Times(1)).error_code(8);
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    let collection = client.database("db").collection::<bson::Document>("coll");
    collection.drop().await.unwrap();

    let models = vec![
        WriteModel::UpdateOne {
            namespace: collection.namespace(),
            filter: doc! { "_id": "a".repeat(max_bson_object_size / 2) },
            update: doc! { "$set": { "x": 1 } }.into(),
            array_filters: None,
            collation: None,
            hint: None,
            upsert: Some(true),
        },
        WriteModel::UpdateOne {
            namespace: collection.namespace(),
            filter: doc! { "_id": "b".repeat(max_bson_object_size / 2) },
            update: doc! { "$set": { "x": 1 } }.into(),
            array_filters: None,
            collation: None,
            hint: None,
            upsert: Some(true),
        },
    ];

    let error = client
        .bulk_write(models)
        .verbose_results(true)
        .await
        .unwrap_err();

    let Some(ref source) = error.source else {
        panic!("Expected error to contain source");
    };
    assert_eq!(source.code(), Some(8));

    let ErrorKind::ClientBulkWrite(ClientBulkWriteError {
        partial_result: Some(partial_result),
        ..
    }) = *error.kind
    else {
        panic!(
            "Expected bulk write error with partial result, got {:?}",
            error
        );
    };
    assert_eq!(partial_result.upserted_count, 2);
    assert_eq!(partial_result.update_results.unwrap().len(), 1);

    let get_more_events = event_buffer.get_command_started_events(&["getMore"]);
    assert_eq!(get_more_events.len(), 1);

    let kill_cursors_events = event_buffer.get_command_started_events(&["killCursors"]);
    assert_eq!(kill_cursors_events.len(), 1);
}

#[tokio::test]
async fn cursor_iteration_in_a_transaction() {
    let event_buffer = EventBuffer::new();
    let client = Client::test_builder()
        .event_buffer(event_buffer.clone())
        .build()
        .await;

    if client.server_version_lt(8, 0) || client.is_standalone() {
        log_uncaptured(
            "skipping cursor_iteration_in_a_transaction: bulkWrite requires 8.0+, transactions \
             require a non-standalone topology",
        );
        return;
    }

    let max_bson_object_size = client.server_info.max_bson_object_size as usize;

    let collection = client.database("db").collection::<bson::Document>("coll");
    collection.drop().await.unwrap();

    let mut session = client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();

    let models = vec![
        WriteModel::UpdateOne {
            namespace: collection.namespace(),
            filter: doc! { "_id": "a".repeat(max_bson_object_size / 2) },
            update: doc! { "$set": { "x": 1 } }.into(),
            array_filters: None,
            collation: None,
            hint: None,
            upsert: Some(true),
        },
        WriteModel::UpdateOne {
            namespace: collection.namespace(),
            filter: doc! { "_id": "b".repeat(max_bson_object_size / 2) },
            update: doc! { "$set": { "x": 1 } }.into(),
            array_filters: None,
            collation: None,
            hint: None,
            upsert: Some(true),
        },
    ];

    let result = client
        .bulk_write(models)
        .verbose_results(true)
        .session(&mut session)
        .await
        .unwrap();
    assert_eq!(result.upserted_count, 2);
    assert_eq!(result.update_results.unwrap().len(), 2);

    let command_started_events = event_buffer.get_command_started_events(&["getMore"]);
    assert_eq!(command_started_events.len(), 1);
}
