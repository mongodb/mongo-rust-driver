use pretty_assertions::assert_eq;

use bson::{bson, doc, Bson};

use crate::{
    bson_util,
    cmap::{CommandResponse, StreamDescription},
    concern::{Acknowledgment, WriteConcern},
    error::{ErrorKind, WriteConcernError, WriteError, WriteFailure},
    operation::{Operation, Update},
    options::{UpdateModifications, UpdateOptions},
    Namespace,
};

#[test]
fn build() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let filter = doc! { "x": { "$gt": 1 } };
    let update = UpdateModifications::Document(doc! { "x": { "$inc": 1 } });
    let wc = WriteConcern {
        w: Some(Acknowledgment::Majority),
        ..Default::default()
    };
    let options = UpdateOptions {
        upsert: Some(false),
        bypass_document_validation: Some(true),
        write_concern: Some(wc),
        ..Default::default()
    };

    let op = Update::new(ns, filter.clone(), update.clone(), false, Some(options));

    let description = StreamDescription::new_testing();
    let mut cmd = op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "update");
    assert_eq!(cmd.target_db.as_str(), "test_db");
    assert_eq!(cmd.read_pref.as_ref(), None);

    let mut expected_body = doc! {
        "update": "test_coll",
        "updates": [
            {
                "q": filter,
                "u": update.to_bson(),
                "upsert": false,
            }
        ],
        "writeConcern": {
            "w": "majority"
        },
        "bypassDocumentValidation": true,
        "ordered": true,
    };

    bson_util::sort_document(&mut cmd.body);
    bson_util::sort_document(&mut expected_body);

    assert_eq!(cmd.body, expected_body);
}

#[test]
fn build_many() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let filter = doc! { "x": { "$gt": 1 } };
    let update = UpdateModifications::Document(doc! { "x": { "$inc": 1 } });

    let op = Update::new(ns, filter.clone(), update.clone(), true, None);

    let description = StreamDescription::new_testing();
    let mut cmd = op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "update");
    assert_eq!(cmd.target_db.as_str(), "test_db");
    assert_eq!(cmd.read_pref.as_ref(), None);

    let mut expected_body = doc! {
        "update": "test_coll",
        "updates": [
            {
                "q": filter,
                "u": update.to_bson(),
                "multi": true,
            }
        ],
        "ordered": true,
    };

    bson_util::sort_document(&mut cmd.body);
    bson_util::sort_document(&mut expected_body);

    assert_eq!(cmd.body, expected_body);
}

#[test]
fn handle_success() {
    let op = Update::empty();

    let ok_response = CommandResponse::with_document(doc! {
        "ok": 1.0,
        "n": 3,
        "nModified": 1,
        "upserted": [
            { "index": 0, "_id": 1 }
        ]
    });

    let ok_result = op.handle_response(ok_response);
    assert!(ok_result.is_ok());

    let update_result = ok_result.unwrap();
    assert_eq!(update_result.matched_count, 0);
    assert_eq!(update_result.modified_count, 1);
    assert_eq!(update_result.upserted_id, Some(Bson::I32(1)));
}

#[test]
fn handle_success_no_upsert() {
    let op = Update::empty();

    let ok_response = CommandResponse::with_document(doc! {
        "ok": 1.0,
        "n": 5,
        "nModified": 2
    });

    let ok_result = op.handle_response(ok_response);
    assert!(ok_result.is_ok());

    let update_result = ok_result.unwrap();
    assert_eq!(update_result.matched_count, 5);
    assert_eq!(update_result.modified_count, 2);
    assert_eq!(update_result.upserted_id, None);
}

#[test]
fn handle_invalid_response() {
    let op = Update::empty();

    let invalid_response = CommandResponse::with_document(doc! { "ok": 1.0, "asdfadsf": 123123 });
    assert!(op.handle_response(invalid_response).is_err());
}

#[test]
fn handle_write_failure() {
    let op = Update::empty();

    let write_error_response = CommandResponse::with_document(doc! {
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
    });
    let write_error_result = op.handle_response(write_error_response);
    assert!(write_error_result.is_err());
    match *write_error_result.unwrap_err().kind {
        ErrorKind::WriteError(WriteFailure::WriteError(ref error)) => {
            let expected_err = WriteError {
                code: 1234,
                code_name: None,
                message: "my error string".to_string(),
            };
            assert_eq!(error, &expected_err);
        }
        ref e => panic!("expected write error, got {:?}", e),
    };
}

#[test]
fn handle_write_concern_failure() {
    let op = Update::empty();

    let wc_error_response = CommandResponse::with_document(doc! {
        "ok": 1.0,
        "n": 0,
        "nModified": 0,
        "writeConcernError": {
            "code": 456,
            "codeName": "wcError",
            "errmsg": "some message"
        }
    });

    let wc_error_result = op.handle_response(wc_error_response);
    assert!(wc_error_result.is_err());

    match *wc_error_result.unwrap_err().kind {
        ErrorKind::WriteError(WriteFailure::WriteConcernError(ref wc_error)) => {
            let expected_wc_err = WriteConcernError {
                code: 456,
                code_name: "wcError".to_string(),
                message: "some message".to_string(),
            };
            assert_eq!(wc_error, &expected_wc_err);
        }
        ref e => panic!("expected write concern error, got {:?}", e),
    }
}
