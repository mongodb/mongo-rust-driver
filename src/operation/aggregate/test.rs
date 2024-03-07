use std::time::Duration;

use crate::{
    bson::doc,
    error::{ErrorKind, WriteFailure},
    operation::{
        aggregate::Aggregate,
        test::{self, handle_response_test},
    },
    options::AggregateOptions,
    Namespace,
};

#[test]
fn op_selection_criteria() {
    test::op_selection_criteria(|selection_criteria| {
        let options = AggregateOptions {
            selection_criteria,
            ..Default::default()
        };
        Aggregate::new("".to_string(), Vec::new(), Some(options))
    });
}

#[test]
fn handle_max_await_time() {
    let response = doc! {
        "ok": 1,
        "cursor": {
            "id": 123,
            "ns": "a.b",
            "firstBatch": []
        }
    };

    let aggregate = Aggregate::empty();
    let spec = handle_response_test(&aggregate, response.clone()).unwrap();
    assert!(spec.max_time().is_none());

    let max_await = Duration::from_millis(123);
    let options = AggregateOptions::builder()
        .max_await_time(max_await)
        .build();
    let aggregate = Aggregate::new(Namespace::empty(), Vec::new(), Some(options));
    let spec = handle_response_test(&aggregate, response).unwrap();
    assert_eq!(spec.max_time(), Some(max_await));
}

#[test]
fn handle_write_concern_error() {
    let response = doc! {
        "ok": 1.0,
        "cursor": {
            "id": 0,
            "ns": "test.test",
            "firstBatch": [],
        },
        "writeConcernError": {
            "code": 64,
            "codeName": "WriteConcernFailed",
            "errmsg": "Waiting for replication timed out",
            "errInfo": {
                "wtimeout": true
            }
        }
    };

    let aggregate = Aggregate::new(
        Namespace::empty(),
        vec![doc! { "$merge": { "into": "a" } }],
        None,
    );

    let error = handle_response_test(&aggregate, response).unwrap_err();
    match *error.kind {
        ErrorKind::Write(WriteFailure::WriteConcernError(_)) => {}
        ref e => panic!("should have gotten WriteConcernError, got {:?} instead", e),
    }
}

#[test]
fn handle_invalid_response() {
    let aggregate = Aggregate::empty();

    let garbled = doc! { "asdfasf": "ASdfasdf" };
    handle_response_test(&aggregate, garbled).unwrap_err();

    let missing_cursor_field = doc! {
        "ok": 1.0,
        "cursor": {
            "ns": "test.test",
            "firstBatch": [],
        }
    };
    handle_response_test(&aggregate, missing_cursor_field).unwrap_err();
}
