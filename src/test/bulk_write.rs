use std::collections::HashMap;

use crate::{
    bson::{doc, Document},
    error::{bulk_write::PartialBulkWriteResult, BulkWriteError, ErrorKind},
    options::{InsertOneModel, UpdateOneModel},
    results::UpdateResult,
    test::{
        get_client_options,
        log_uncaptured,
        util::fail_point::{FailPoint, FailPointMode},
    },
    Client,
    Namespace,
};

use super::TestClient;

impl PartialBulkWriteResult {
    fn inserted_count(&self) -> i64 {
        match self {
            Self::Summary(summary_result) => summary_result.inserted_count,
            Self::Verbose(verbose_result) => verbose_result.summary.inserted_count,
        }
    }

    fn upserted_count(&self) -> i64 {
        match self {
            Self::Summary(summary_result) => summary_result.upserted_count,
            Self::Verbose(verbose_result) => verbose_result.summary.upserted_count,
        }
    }

    fn update_results(&self) -> Option<&HashMap<usize, UpdateResult>> {
        match self {
            Self::Summary(_) => None,
            Self::Verbose(verbose_result) => Some(&verbose_result.update_results),
        }
    }
}

// CRUD prose test 3
#[tokio::test]
async fn max_write_batch_size_batching() {
    let client = Client::test_builder().monitor_events().build().await;

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping max_write_batch_size_batching: bulkWrite requires 8.0+");
        return;
    }

    let max_write_batch_size = client.server_info.max_write_batch_size.unwrap() as usize;

    let model = InsertOneModel::builder()
        .namespace(Namespace::new("db", "coll"))
        .document(doc! { "a": "b" })
        .build();
    let models = vec![model; max_write_batch_size + 1];

    let result = client.bulk_write(models).await.unwrap();
    assert_eq!(result.inserted_count as usize, max_write_batch_size + 1);

    let mut command_started_events = client
        .events
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

// CRUD prose test 4
#[tokio::test]
async fn max_message_size_bytes_batching() {
    let client = Client::test_builder().monitor_events().build().await;

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping max_message_size_bytes_batching: bulkWrite requires 8.0+");
        return;
    }

    let max_bson_object_size = client.server_info.max_bson_object_size as usize;
    let max_message_size_bytes = client.server_info.max_message_size_bytes as usize;

    let document = doc! { "a": "b".repeat(max_bson_object_size - 500) };
    let model = InsertOneModel::builder()
        .namespace(Namespace::new("db", "coll"))
        .document(document)
        .build();
    let num_models = max_message_size_bytes / max_bson_object_size + 1;
    let models = vec![model; num_models];

    let result = client.bulk_write(models).await.unwrap();
    assert_eq!(result.inserted_count as usize, num_models);

    let mut command_started_events = client
        .events
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

// CRUD prose test 5
#[tokio::test(flavor = "multi_thread")]
async fn write_concern_error_batches() {
    let mut options = get_client_options().await.clone();
    options.retry_writes = Some(false);
    if TestClient::new().await.is_sharded() {
        options.hosts.drain(1..);
    }
    let client = Client::test_builder()
        .options(options)
        .monitor_events()
        .build()
        .await;

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping write_concern_error_batches: bulkWrite requires 8.0+");
        return;
    }

    let max_write_batch_size = client.server_info.max_write_batch_size.unwrap() as usize;

    let fail_point = FailPoint::fail_command(&["bulkWrite"], FailPointMode::Times(2))
        .write_concern_error(doc! { "code": 91, "errmsg": "Replication is being shut down" });
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    let models = vec![
        InsertOneModel::builder()
            .namespace(Namespace::new("db", "coll"))
            .document(doc! { "a": "b" })
            .build();
        max_write_batch_size + 1
    ];
    let error = client.bulk_write(models).ordered(false).await.unwrap_err();

    let ErrorKind::BulkWrite(bulk_write_error) = *error.kind else {
        panic!("Expected bulk write error, got {:?}", error);
    };

    assert_eq!(bulk_write_error.write_concern_errors.len(), 2);

    let partial_result = bulk_write_error.partial_result.unwrap();
    assert_eq!(
        partial_result.inserted_count() as usize,
        max_write_batch_size + 1
    );

    let command_started_events = client.events.get_command_started_events(&["bulkWrite"]);
    assert_eq!(command_started_events.len(), 2);
}

// CRUD prose test 6
#[tokio::test]
async fn write_error_batches() {
    let mut client = Client::test_builder().monitor_events().build().await;

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
        InsertOneModel::builder()
            .namespace(collection.namespace())
            .document(document)
            .build();
        max_write_batch_size + 1
    ];

    let error = client
        .bulk_write(models.clone())
        .ordered(false)
        .await
        .unwrap_err();

    let ErrorKind::BulkWrite(bulk_write_error) = *error.kind else {
        panic!("Expected bulk write error, got {:?}", error);
    };

    assert_eq!(
        bulk_write_error.write_errors.len(),
        max_write_batch_size + 1
    );

    let command_started_events = client.events.get_command_started_events(&["bulkWrite"]);
    assert_eq!(command_started_events.len(), 2);

    client.events.clear_cached_events();

    let error = client.bulk_write(models).ordered(true).await.unwrap_err();

    let ErrorKind::BulkWrite(bulk_write_error) = *error.kind else {
        panic!("Expected bulk write error, got {:?}", error);
    };

    assert_eq!(bulk_write_error.write_errors.len(), 1);

    let command_started_events = client.events.get_command_started_events(&["bulkWrite"]);
    assert_eq!(command_started_events.len(), 1);
}

// CRUD prose test 7
#[tokio::test]
async fn successful_cursor_iteration() {
    let client = Client::test_builder().monitor_events().build().await;

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping successful_cursor_iteration: bulkWrite requires 8.0+");
        return;
    }

    let max_bson_object_size = client.server_info.max_bson_object_size as usize;

    let collection = client.database("db").collection::<Document>("coll");
    collection.drop().await.unwrap();

    let models = vec![
        UpdateOneModel::builder()
            .namespace(collection.namespace())
            .filter(doc! { "_id": "a".repeat(max_bson_object_size / 2) })
            .update(doc! { "$set": { "x": 1 } })
            .upsert(true)
            .build(),
        UpdateOneModel::builder()
            .namespace(collection.namespace())
            .filter(doc! { "_id": "b".repeat(max_bson_object_size / 2) })
            .update(doc! { "$set": { "x": 1 } })
            .upsert(true)
            .build(),
    ];

    let result = client.bulk_write(models).verbose_results().await.unwrap();
    assert_eq!(result.summary.upserted_count, 2);
    assert_eq!(result.update_results.len(), 2);

    let command_started_events = client.events.get_command_started_events(&["getMore"]);
    assert_eq!(command_started_events.len(), 1);
}

// CRUD prose test 8
#[tokio::test]
async fn cursor_iteration_in_a_transaction() {
    let client = Client::test_builder().monitor_events().build().await;

    if client.server_version_lt(8, 0) || client.is_standalone() {
        log_uncaptured(
            "skipping cursor_iteration_in_a_transaction: bulkWrite requires 8.0+, transactions \
             require a non-standalone topology",
        );
        return;
    }

    let max_bson_object_size = client.server_info.max_bson_object_size as usize;

    let collection = client.database("db").collection::<Document>("coll");
    collection.drop().await.unwrap();

    let mut session = client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();

    let models = vec![
        UpdateOneModel::builder()
            .namespace(collection.namespace())
            .filter(doc! { "_id": "a".repeat(max_bson_object_size / 2) })
            .update(doc! { "$set": { "x": 1 } })
            .upsert(true)
            .build(),
        UpdateOneModel::builder()
            .namespace(collection.namespace())
            .filter(doc! { "_id": "b".repeat(max_bson_object_size / 2) })
            .update(doc! { "$set": { "x": 1 } })
            .upsert(true)
            .build(),
    ];

    let result = client
        .bulk_write(models)
        .verbose_results()
        .session(&mut session)
        .await
        .unwrap();
    assert_eq!(result.summary.upserted_count, 2);
    assert_eq!(result.update_results.len(), 2);

    let command_started_events = client.events.get_command_started_events(&["getMore"]);
    assert_eq!(command_started_events.len(), 1);
}

// CRUD prose test 9
#[tokio::test(flavor = "multi_thread")]
async fn failed_cursor_iteration() {
    let mut options = get_client_options().await.clone();
    if TestClient::new().await.is_sharded() {
        options.hosts.drain(1..);
    }
    let client = Client::test_builder()
        .options(options)
        .monitor_events()
        .build()
        .await;

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping failed_cursor_iteration: bulkWrite requires 8.0+");
        return;
    }

    let max_bson_object_size = client.server_info.max_bson_object_size as usize;

    let fail_point = FailPoint::fail_command(&["getMore"], FailPointMode::Times(1)).error_code(8);
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    let collection = client.database("db").collection::<Document>("coll");
    collection.drop().await.unwrap();

    let models = vec![
        UpdateOneModel::builder()
            .namespace(collection.namespace())
            .filter(doc! { "_id": "a".repeat(max_bson_object_size / 2) })
            .update(doc! { "$set": { "x": 1 } })
            .upsert(true)
            .build(),
        UpdateOneModel::builder()
            .namespace(collection.namespace())
            .filter(doc! { "_id": "b".repeat(max_bson_object_size / 2) })
            .update(doc! { "$set": { "x": 1 } })
            .upsert(true)
            .build(),
    ];

    let error = client
        .bulk_write(models)
        .verbose_results()
        .await
        .unwrap_err();

    let Some(ref source) = error.source else {
        panic!("Expected error to contain source");
    };
    assert_eq!(source.code(), Some(8));

    let ErrorKind::BulkWrite(BulkWriteError {
        partial_result: Some(partial_result),
        ..
    }) = *error.kind
    else {
        panic!(
            "Expected bulk write error with partial result, got {:?}",
            error
        );
    };
    assert_eq!(partial_result.upserted_count(), 2);
    assert_eq!(partial_result.update_results().unwrap().len(), 1);

    let get_more_events = client.events.get_command_started_events(&["getMore"]);
    assert_eq!(get_more_events.len(), 1);

    let kill_cursors_events = client.events.get_command_started_events(&["killCursors"]);
    assert_eq!(kill_cursors_events.len(), 1);
}

// CRUD prose test 10 not implemented. The driver does not support unacknowledged writes.

// CRUD prose test 11
#[tokio::test]
async fn namespace_batch_splitting() {
    let first_namespace = Namespace::new("db", "coll");

    let mut client = Client::test_builder().monitor_events().build().await;
    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping namespace_batch_splitting: bulkWrite requires 8.0+");
        return;
    }

    let max_message_size_bytes = client.server_info.max_message_size_bytes as usize;
    let max_bson_object_size = client.server_info.max_bson_object_size as usize;

    let ops_bytes = max_message_size_bytes - 1122;
    let num_models = ops_bytes / max_bson_object_size;

    let model = InsertOneModel::builder()
        .namespace(first_namespace.clone())
        .document(doc! { "a": "b".repeat(max_bson_object_size - 57) })
        .build();
    let mut models = vec![model; num_models];

    let remainder_bytes = ops_bytes % max_bson_object_size;
    if remainder_bytes >= 217 {
        models.push(
            InsertOneModel::builder()
                .namespace(first_namespace.clone())
                .document(doc! { "a": "b".repeat(remainder_bytes - 57) })
                .build(),
        );
    }

    // Case 1: no batch-splitting required

    let mut first_models = models.clone();
    first_models.push(
        InsertOneModel::builder()
            .namespace(first_namespace.clone())
            .document(doc! { "a": "b" })
            .build(),
    );
    let num_models = first_models.len();

    let result = client.bulk_write(first_models).await.unwrap();
    assert_eq!(result.inserted_count as usize, num_models);

    let command_started_events = client.events.get_command_started_events(&["bulkWrite"]);
    assert_eq!(command_started_events.len(), 1);

    let event = &command_started_events[0];

    let ops = event.command.get_array("ops").unwrap();
    assert_eq!(ops.len(), num_models);

    let ns_info = event.command.get_array("nsInfo").unwrap();
    assert_eq!(ns_info.len(), 1);
    let namespace = ns_info[0].as_document().unwrap().get_str("ns").unwrap();
    assert_eq!(namespace, &first_namespace.to_string());

    // Case 2: batch-splitting required

    client.events.clear_cached_events();

    let second_namespace = Namespace::new("db", "c".repeat(200));

    let mut second_models = models.clone();
    second_models.push(
        InsertOneModel::builder()
            .namespace(second_namespace.clone())
            .document(doc! { "a": "b" })
            .build(),
    );
    let num_models = second_models.len();

    let result = client.bulk_write(second_models).await.unwrap();
    assert_eq!(result.inserted_count as usize, num_models);

    let command_started_events = client.events.get_command_started_events(&["bulkWrite"]);
    assert_eq!(command_started_events.len(), 2);

    let first_event = &command_started_events[0];

    let first_ops = first_event.command.get_array("ops").unwrap();
    assert_eq!(first_ops.len(), num_models - 1);

    let first_ns_info = first_event.command.get_array("nsInfo").unwrap();
    assert_eq!(first_ns_info.len(), 1);
    let actual_first_namespace = first_ns_info[0]
        .as_document()
        .unwrap()
        .get_str("ns")
        .unwrap();
    assert_eq!(actual_first_namespace, &first_namespace.to_string());

    let second_event = &command_started_events[1];

    let second_ops = second_event.command.get_array("ops").unwrap();
    assert_eq!(second_ops.len(), 1);

    let second_ns_info = second_event.command.get_array("nsInfo").unwrap();
    assert_eq!(second_ns_info.len(), 1);
    let actual_second_namespace = second_ns_info[0]
        .as_document()
        .unwrap()
        .get_str("ns")
        .unwrap();
    assert_eq!(actual_second_namespace, &second_namespace.to_string());
}

// CRUD prose test 12
#[tokio::test]
async fn too_large_client_error() {
    let client = Client::test_builder().monitor_events().build().await;
    let max_message_size_bytes = client.server_info.max_message_size_bytes as usize;

    if client.server_version_lt(8, 0) {
        log_uncaptured("skipping too_large_client_error: bulkWrite requires 8.0+");
        return;
    }

    // Case 1: document too large
    let model = InsertOneModel::builder()
        .namespace(Namespace::new("db", "coll"))
        .document(doc! { "a": "b".repeat(max_message_size_bytes) })
        .build();

    let error = client.bulk_write(vec![model]).await.unwrap_err();
    assert!(!error.is_server_error());

    // Case 2: namespace too large
    let model = InsertOneModel::builder()
        .namespace(Namespace::new("db", "c".repeat(max_message_size_bytes)))
        .document(doc! { "a": "b" })
        .build();

    let error = client.bulk_write(vec![model]).await.unwrap_err();
    assert!(!error.is_server_error());
}

// CRUD prose test 13
#[cfg(feature = "in-use-encryption-unstable")]
#[tokio::test]
async fn encryption_error() {
    use crate::{
        client::csfle::options::{AutoEncryptionOptions, KmsProviders},
        mongocrypt::ctx::KmsProvider,
    };

    let kms_providers = KmsProviders::new(vec![(
        KmsProvider::aws(),
        doc! { "accessKeyId": "foo", "secretAccessKey": "bar" },
        None,
    )])
    .unwrap();
    let encrypted_options = AutoEncryptionOptions::new(Namespace::new("db", "coll"), kms_providers);
    let encrypted_client = Client::test_builder()
        .encrypted_options(encrypted_options)
        .build()
        .await;

    let model = InsertOneModel::builder()
        .namespace(Namespace::new("db", "coll"))
        .document(doc! { "a": "b" })
        .build();
    let error = encrypted_client.bulk_write(vec![model]).await.unwrap_err();

    let ErrorKind::Encryption(encryption_error) = *error.kind else {
        panic!("expected encryption error, got {:?}", error);
    };

    assert_eq!(
        encryption_error.message,
        Some("bulkWrite does not currently support automatic encryption".to_string())
    );
}

#[tokio::test]
async fn unsupported_server_client_error() {
    let client = Client::test_builder().build().await;

    if client.server_version_gte(8, 0) {
        return;
    }

    let error = client
        .bulk_write(vec![InsertOneModel::builder()
            .namespace(Namespace::new("db", "coll"))
            .document(doc! { "a": "b" })
            .build()])
        .await
        .unwrap_err();
    assert!(matches!(*error.kind, ErrorKind::IncompatibleServer { .. }));
}
