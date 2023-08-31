use pretty_assertions::assert_eq;

use crate::{
    bson::{doc, Bson},
    error::{ErrorKind, WriteConcernError, WriteError, WriteFailure},
    operation::{test::handle_response_test, Update},
};

#[test]
fn handle_success() {
    let op = Update::empty();

    let ok_response = doc! {
        "ok": 1.0,
        "n": 3,
        "nModified": 1,
        "upserted": [
            { "index": 0, "_id": 1 }
        ]
    };

    let update_result = handle_response_test(&op, ok_response).unwrap();
    assert_eq!(update_result.matched_count, 0);
    assert_eq!(update_result.modified_count, 1);
    assert_eq!(update_result.upserted_id, Some(Bson::Int32(1)));
}

#[test]
fn handle_success_no_upsert() {
    let op = Update::empty();

    let ok_response = doc! {
        "ok": 1.0,
        "n": 5,
        "nModified": 2
    };

    let update_result = handle_response_test(&op, ok_response).unwrap();
    assert_eq!(update_result.matched_count, 5);
    assert_eq!(update_result.modified_count, 2);
    assert_eq!(update_result.upserted_id, None);
}

#[test]
fn handle_write_failure() {
    let op = Update::empty();

    let write_error_response = doc! {
        "ok": 1.0,
        "n": 12,
        "nModified": 0,
        "writeErrors": [
            {
                "index": 0,
                "code": 1234,
                "errmsg": "my error string"
            }
        ]
    };

    let write_error = handle_response_test(&op, write_error_response).unwrap_err();
    match *write_error.kind {
        ErrorKind::Write(WriteFailure::WriteError(ref error)) => {
            let expected_err = WriteError {
                code: 1234,
                code_name: None,
                message: "my error string".to_string(),
                details: None,
            };
            assert_eq!(error, &expected_err);
        }
        ref e => panic!("expected write error, got {:?}", e),
    };
}

#[test]
fn handle_write_concern_failure() {
    let op = Update::empty();

    let wc_error_response = doc! {
        "ok": 1.0,
        "n": 0,
        "nModified": 0,
        "writeConcernError": {
            "code": 456,
            "codeName": "wcError",
            "errmsg": "some message",
            "errInfo": {
                "writeConcern": {
                    "w": 2,
                    "wtimeout": 0,
                    "provenance": "clientSupplied"
                }
            }
        }
    };

    let wc_error = handle_response_test(&op, wc_error_response).unwrap_err();
    match *wc_error.kind {
        ErrorKind::Write(WriteFailure::WriteConcernError(ref wc_error)) => {
            let expected_wc_err = WriteConcernError {
                code: 456,
                code_name: "wcError".to_string(),
                message: "some message".to_string(),
                details: Some(doc! { "writeConcern": {
                    "w": 2,
                    "wtimeout": 0,
                    "provenance": "clientSupplied"
                } }),
                labels: vec![],
            };
            assert_eq!(wc_error, &expected_wc_err);
        }
        ref e => panic!("expected write concern error, got {:?}", e),
    }
}
