use std::time::Duration;

use crate::{
    bson::{doc, Document},
    bson_util,
    change_stream::ChangeStreamTarget,
    cmap::{CommandResponse, StreamDescription},
    concern::{ReadConcern, ReadConcernLevel},
    operation::{Operation, Watch},
    options::{
        ChangeStreamOptions,
        FullDocumentType,
        ReadPreference,
        SelectionCriteria,
        StreamAddress,
    },
    Namespace,
};

fn build_test(
    target: impl Into<ChangeStreamTarget>,
    pipeline: Vec<Document>,
    options: Option<ChangeStreamOptions>,
    mut expected_body: Document,
) {
    let watch = Watch::new(target.into(), pipeline, options).unwrap();

    let mut cmd = watch.build(&StreamDescription::new_testing()).unwrap();

    assert_eq!(cmd.name.as_str(), "aggregate");

    bson_util::sort_document(&mut expected_body);
    bson_util::sort_document(&mut cmd.body);

    assert_eq!(cmd.body, expected_body);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let pipeline = vec![doc! { "$match": { "x": 3 } }];
    let options = ChangeStreamOptions::builder()
        .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary))
        .read_concern(ReadConcern::from(ReadConcernLevel::Available))
        .max_await_time(Duration::from_millis(5))
        .batch_size(10)
        .full_document(FullDocumentType::UpdateLookup)
        .build();

    let expected_body = doc! {
        "aggregate": 1,
        "cursor": { "batchSize": 10 },
        "pipeline": [
            { "$changeStream": { "fullDocument": "updateLookup" } },
            { "$match": { "x": 3 } },
        ],
        "readConcern": { "level": "available" },
    };

    build_test(
        ChangeStreamTarget::Database("test_db".to_string()),
        pipeline,
        Some(options),
        expected_body,
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_collection() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let pipeline = vec![doc! { "$match": { "x": 3 } }];
    let options = ChangeStreamOptions::builder()
        .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary))
        .read_concern(ReadConcern::from(ReadConcernLevel::Available))
        .full_document(FullDocumentType::UpdateLookup)
        .build();

    let expected_body = doc! {
        "aggregate": "test_coll",
        "cursor": {},
        "pipeline": [
            { "$changeStream": { "fullDocument": "updateLookup" } },
            { "$match": { "x": 3 } }
        ],
        "readConcern": { "level": "available" },
    };

    build_test(
        ChangeStreamTarget::Collection(ns),
        pipeline,
        Some(options),
        expected_body,
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_cluster() {
    let pipeline = vec![doc! { "$match": { "x": 3 } }];
    let options = ChangeStreamOptions::builder()
        .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary))
        .read_concern(ReadConcern::from(ReadConcernLevel::Available))
        .full_document(FullDocumentType::UpdateLookup)
        .build();

    let expected_body = doc! {
        "aggregate": 1,
        "cursor": {},
        "pipeline": [
            { "$changeStream": { "fullDocument": "updateLookup", "allChangesForCluster": true } },
            { "$match": { "x": 3 } }
        ],
        "readConcern": { "level": "available" },
    };

    build_test(
        ChangeStreamTarget::Cluster("test_db".to_string()),
        pipeline,
        Some(options),
        expected_body,
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_empty_pipeline() {
    let pipeline = Vec::new();
    let expected_body = doc! {
        "aggregate": 1,
        "cursor": {},
        "pipeline": [ { "$changeStream": {} } ]
    };

    build_test(
        ChangeStreamTarget::Database("test_db".to_string()),
        pipeline,
        None,
        expected_body,
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let address = StreamAddress {
        hostname: "localhost".to_string(),
        port: None,
    };

    let pipeline = vec![doc! { "$match": { "x": 3 } }];
    let watch = Watch::new(
        ChangeStreamTarget::Database("test_db".to_string()),
        pipeline.clone(),
        None,
    )
    .unwrap();

    let first_batch = vec![doc! { "_id": 1 }, doc! { "_id": 2 }];

    let response = doc! {
        "aggregate": 1,
        "cursor": {
            "id": 123,
            "ns": format!("{}.{}", ns.db, ns.coll),
            "firstBatch": bson_util::to_bson_array(&first_batch),
        },
        "pipeline": [
            { "$changeStream": {} },
            { "$match": { "x": 3 } }
        ]
    };

    let result = watch.handle_response(CommandResponse::with_document_and_address(
        address.clone(),
        response.clone(),
    ));
    assert!(result.is_ok());

    let change_stream_spec = result.unwrap();
    assert_eq!(change_stream_spec.pipeline, pipeline);

    let cursor_spec = change_stream_spec.cursor_spec;
    assert_eq!(cursor_spec.address(), &address);
    assert_eq!(cursor_spec.id(), 123);
    assert_eq!(cursor_spec.batch_size(), None);

    assert_eq!(
        cursor_spec
            .initial_buffer
            .into_iter()
            .collect::<Vec<Document>>(),
        first_batch
    );

    let options = ChangeStreamOptions::builder()
        .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary))
        .max_await_time(Duration::from_millis(5))
        .batch_size(10)
        .full_document(FullDocumentType::UpdateLookup)
        .build();

    let watch = Watch::new(
        ChangeStreamTarget::Database("test_db".to_string()),
        Vec::new(),
        Some(options),
    )
    .unwrap();
    let result = watch.handle_response(CommandResponse::with_document_and_address(
        address.clone(),
        response,
    ));
    assert!(result.is_ok());

    let change_streams_spec = result.unwrap();
    assert_eq!(change_streams_spec.pipeline, Vec::new());

    let cursor_spec = change_streams_spec.cursor_spec;
    assert_eq!(cursor_spec.address(), &address);
    assert_eq!(cursor_spec.id(), 123);
    assert_eq!(cursor_spec.batch_size(), Some(10));
    assert_eq!(cursor_spec.max_time(), Some(Duration::from_millis(5)));
    assert_eq!(
        cursor_spec
            .initial_buffer
            .into_iter()
            .collect::<Vec<Document>>(),
        first_batch
    );
}
