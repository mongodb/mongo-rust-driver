use pretty_assertions::assert_eq;

use crate::{
    bson::{doc, Bson},
    bson_util,
    cmap::StreamDescription,
    coll::options::Hint,
    concern::{Acknowledgment, WriteConcern},
    error::{ErrorKind, WriteConcernError, WriteError, WriteFailure},
    operation::{test::handle_response_test, Operation, Update},
    options::{UpdateModifications, UpdateOptions},
    Namespace,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
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

    let mut op = Update::new(ns, filter.clone(), update.clone(), false, Some(options));

    let description = StreamDescription::new_testing();
    let mut cmd = op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "update");
    assert_eq!(cmd.target_db.as_str(), "test_db");

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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_hint() {
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
        hint: Some(Hint::Keys(doc! { "x": 1, "y": -1 })),
        ..Default::default()
    };

    let mut op = Update::new(ns, filter.clone(), update.clone(), false, Some(options));

    let description = StreamDescription::new_testing();
    let mut cmd = op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "update");
    assert_eq!(cmd.target_db.as_str(), "test_db");

    let mut expected_body = doc! {
        "update": "test_coll",
        "updates": [
            {
                "q": filter,
                "u": update.to_bson(),
                "upsert": false,
                "hint": {
                    "x": 1,
                    "y": -1,
                },
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_no_write_concern() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let filter = doc! { "x": { "$gt": 1 } };
    let update = UpdateModifications::Document(doc! { "x": { "$inc": 1 } });
    let wc = WriteConcern {
        ..Default::default()
    };
    let options = UpdateOptions {
        upsert: Some(false),
        bypass_document_validation: Some(true),
        write_concern: Some(wc),
        ..Default::default()
    };

    let mut op = Update::new(ns, filter.clone(), update.clone(), false, Some(options));

    let description = StreamDescription::new_testing();
    let mut cmd = op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "update");
    assert_eq!(cmd.target_db.as_str(), "test_db");

    let mut expected_body = doc! {
        "update": "test_coll",
        "updates": [
            {
                "q": filter,
                "u": update.to_bson(),
                "upsert": false,
            }
        ],
        "bypassDocumentValidation": true,
        "ordered": true,
    };

    bson_util::sort_document(&mut cmd.body);
    bson_util::sort_document(&mut expected_body);

    assert_eq!(cmd.body, expected_body);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_many() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let filter = doc! { "x": { "$gt": 1 } };
    let update = UpdateModifications::Document(doc! { "x": { "$inc": 1 } });

    let mut op = Update::new(ns, filter.clone(), update.clone(), true, None);

    let description = StreamDescription::new_testing();
    let mut cmd = op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "update");
    assert_eq!(cmd.target_db.as_str(), "test_db");

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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success_no_upsert() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_write_failure() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_write_concern_failure() {
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
            };
            assert_eq!(wc_error, &expected_wc_err);
        }
        ref e => panic!("expected write concern error, got {:?}", e),
    }
}
