use std::time::Duration;

use super::AggregateTarget;
use crate::{
    bson::{doc, Document},
    bson_util,
    cmap::{CommandResponse, StreamDescription},
    concern::{ReadConcern, ReadConcernLevel},
    error::{ErrorKind, WriteFailure},
    operation::{test, Aggregate, Operation},
    options::{AggregateOptions, Hint, StreamAddress},
    Namespace,
};

fn build_test(
    target: impl Into<AggregateTarget>,
    pipeline: Vec<Document>,
    options: Option<AggregateOptions>,
    mut expected_body: Document,
) {
    let target = target.into();

    let aggregate = Aggregate::new(target.clone(), pipeline, options);

    let mut cmd = aggregate.build(&StreamDescription::new_testing()).unwrap();

    assert_eq!(cmd.name.as_str(), "aggregate");
    assert_eq!(cmd.target_db.as_str(), target.db_name());

    bson_util::sort_document(&mut expected_body);
    bson_util::sort_document(&mut cmd.body);

    assert_eq!(cmd.body, expected_body);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let pipeline = vec![doc! { "$match": { "x": 3 }}];

    let options = AggregateOptions::builder()
        .hint(Hint::Keys(doc! { "x": 1, "y": 2 }))
        .bypass_document_validation(true)
        .read_concern(ReadConcern::from(ReadConcernLevel::Available))
        .build();

    let expected_body = doc! {
        "aggregate": "test_coll",
        "pipeline": bson_util::to_bson_array(&pipeline),
        "cursor": {},
        "hint": {
            "x": 1,
            "y": 2,
        },
        "bypassDocumentValidation": true,
        "readConcern": {
            "level": "available"
        },
    };

    build_test(ns, pipeline, Some(options), expected_body);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_batch_size() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let pipeline = Vec::new();

    let mut expected_body = doc! {
        "aggregate": "test_coll",
        "pipeline": [],
        "cursor": {},
    };

    build_test(ns.clone(), pipeline.clone(), None, expected_body.clone());

    build_test(
        ns.clone(),
        pipeline.clone(),
        Some(AggregateOptions::default()),
        expected_body.clone(),
    );

    let batch_size_options = AggregateOptions::builder().batch_size(5).build();
    expected_body.insert("cursor", doc! { "batchSize": 5 });
    build_test(
        ns.clone(),
        pipeline,
        Some(batch_size_options.clone()),
        expected_body.clone(),
    );

    let out_pipeline = vec![doc! { "$out": "cat" }];
    expected_body.insert("cursor", Document::new());
    expected_body.insert("pipeline", bson_util::to_bson_array(&out_pipeline));
    build_test(
        ns.clone(),
        out_pipeline,
        Some(batch_size_options.clone()),
        expected_body.clone(),
    );

    let merge_pipeline = vec![doc! {
        "$merge": {
            "into": "out",
        }
    }];
    expected_body.insert("pipeline", bson_util::to_bson_array(&merge_pipeline));
    build_test(ns, merge_pipeline, Some(batch_size_options), expected_body);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_target() {
    let pipeline = Vec::new();

    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let expected_body = doc! {
        "aggregate": "test_coll",
        "pipeline": [],
        "cursor": {},
    };
    build_test(ns.clone(), pipeline.clone(), None, expected_body);

    let expected_body = doc! {
        "aggregate": 1,
        "pipeline": [],
        "cursor": {}
    };
    build_test(ns.db, pipeline, None, expected_body);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_max_await_time() {
    let options = AggregateOptions::builder()
        .max_await_time(Duration::from_millis(5))
        .max_time(Duration::from_millis(10))
        .build();

    let body = doc! {
        "aggregate": 1,
        "cursor": {},
        "maxTimeMS": 10i32,
        "pipeline": []
    };

    build_test("".to_string(), Vec::new(), Some(options), body);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn op_selection_criteria() {
    test::op_selection_criteria(|selection_criteria| {
        let options = AggregateOptions {
            selection_criteria,
            ..Default::default()
        };
        Aggregate::new("".to_string(), Vec::new(), Some(options))
    });
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

    let aggregate = Aggregate::new(ns.clone(), Vec::new(), None);

    let first_batch = vec![doc! {"_id": 1}, doc! {"_id": 2}];

    let response = doc! {
        "cursor": {
            "id": 123,
            "ns": format!("{}.{}", ns.db, ns.coll),
            "firstBatch": bson_util::to_bson_array(&first_batch),
        },
        "ok": 1.0
    };

    let result = aggregate.handle_response(
        CommandResponse::with_document_and_address(address.clone(), response.clone()),
        &Default::default(),
    );
    assert!(result.is_ok());

    let cursor_spec = result.unwrap();
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

    let aggregate = Aggregate::new(
        ns,
        Vec::new(),
        Some(
            AggregateOptions::builder()
                .batch_size(123)
                .max_await_time(Duration::from_millis(5))
                .build(),
        ),
    );
    let result = aggregate.handle_response(
        CommandResponse::with_document_and_address(address.clone(), response),
        &Default::default(),
    );
    assert!(result.is_ok());

    let cursor_spec = result.unwrap();
    assert_eq!(cursor_spec.address(), &address);
    assert_eq!(cursor_spec.id(), 123);
    assert_eq!(cursor_spec.batch_size(), Some(123));
    assert_eq!(cursor_spec.max_time(), Some(Duration::from_millis(5)));
    assert_eq!(
        cursor_spec
            .initial_buffer
            .into_iter()
            .collect::<Vec<Document>>(),
        first_batch
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_max_await_time() {
    let response = CommandResponse::with_document_and_address(
        StreamAddress::default(),
        doc! {
            "cursor": {
                "id": 123,
                "ns": "a.b",
                "firstBatch": []
            },
            "ok": 1.0
        },
    );

    let aggregate = Aggregate::empty();

    let spec = aggregate
        .handle_response(response.clone(), &Default::default())
        .expect("handle should succeed");
    assert!(spec.max_time().is_none());

    let max_await = Duration::from_millis(123);
    let options = AggregateOptions::builder()
        .max_await_time(max_await)
        .build();
    let aggregate = Aggregate::new(Namespace::empty(), Vec::new(), Some(options));
    let spec = aggregate
        .handle_response(response, &Default::default())
        .expect("handle_should_succeed");
    assert_eq!(spec.max_time(), Some(max_await));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_write_concern_error() {
    let response = CommandResponse::with_document(doc! {
        "cursor" : {
            "firstBatch" : [ ],
            "id" : 0_i64,
            "ns" : "test.test"
        },
        "writeConcernError" : {
            "code" : 64,
            "codeName" : "WriteConcernFailed",
            "errmsg" : "waiting for replication timed out",
            "errInfo" : {
                "wtimeout" : true
            }
        },
        "ok" : 1,
    });

    let aggregate = Aggregate::new(
        Namespace::empty(),
        vec![doc! { "$merge": { "into": "a" } }],
        None,
    );

    let error = aggregate
        .handle_response(response, &Default::default())
        .expect_err("should get wc error");
    match error.kind {
        ErrorKind::WriteError(WriteFailure::WriteConcernError(_)) => {}
        ref e => panic!("should have gotten WriteConcernError, got {:?} instead", e),
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_invalid_response() {
    let aggregate = Aggregate::empty();

    let garbled = doc! { "asdfasf": "ASdfasdf" };
    assert!(aggregate
        .handle_response(CommandResponse::with_document(garbled), &Default::default())
        .is_err());

    let missing_cursor_field = doc! {
        "cursor": {
            "ns": "test.test",
            "firstBatch": [],
        }
    };
    assert!(aggregate
        .handle_response(
            CommandResponse::with_document(missing_cursor_field),
            &Default::default()
        )
        .is_err());
}
