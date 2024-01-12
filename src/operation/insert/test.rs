use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use crate::{
    bson::{doc, Document},
    cmap::StreamDescription,
    concern::WriteConcern,
    error::{BulkWriteError, ErrorKind, WriteConcernError},
    operation::{test::handle_response_test, Insert, Operation},
    options::InsertManyOptions,
    Namespace,
};

struct TestFixtures {
    op: Insert<'static, Document>,
    documents: Vec<Document>,
}

/// Get an Insert operation and the documents/options used to construct it.
fn fixtures(opts: Option<InsertManyOptions>) -> TestFixtures {
    lazy_static! {
        static ref DOCUMENTS: Vec<Document> = vec![
            Document::new(),
            doc! {"_id": 1234, "a": 1},
            doc! {"a": 123, "b": "hello world" },
        ];
    }

    let options = opts.unwrap_or(InsertManyOptions {
        ordered: Some(true),
        write_concern: Some(WriteConcern::builder().journal(true).build()),
        ..Default::default()
    });

    let op = Insert::new(
        Namespace {
            db: "test_db".to_string(),
            coll: "test_coll".to_string(),
        },
        DOCUMENTS.iter().collect(),
        Some(options.clone()),
        false,
    );

    TestFixtures {
        op,
        documents: DOCUMENTS.clone(),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Documents<D> {
    documents: Vec<D>,
}

#[test]
fn handle_success() {
    let mut fixtures = fixtures(None);

    // populate _id for documents that don't provide it
    fixtures
        .op
        .build(&StreamDescription::new_testing())
        .unwrap();
    let response = handle_response_test(&fixtures.op, doc! { "ok": 1.0, "n": 3 }).unwrap();
    let inserted_ids = response.inserted_ids;
    assert_eq!(inserted_ids.len(), 3);
    assert_eq!(
        inserted_ids.get(&1).unwrap(),
        fixtures.documents[1].get("_id").unwrap()
    );
}

#[test]
fn handle_invalid_response() {
    let fixtures = fixtures(None);
    handle_response_test(&fixtures.op, doc! { "ok": 1.0, "asdfadsf": 123123 }).unwrap_err();
}

#[test]
fn handle_write_failure() {
    let mut fixtures = fixtures(None);

    // generate _id for operations missing it.
    let _ = fixtures
        .op
        .build(&StreamDescription::new_testing())
        .unwrap();

    let write_error_response = doc! {
        "ok": 1.0,
        "n": 1,
        "writeErrors": [
            {
                "index": 1,
                "code": 11000,
                "errmsg": "duplicate key",
                "errInfo": {
                    "test key": "test value",
                }
            }
        ],
        "writeConcernError": {
            "code": 123,
            "codeName": "woohoo",
            "errmsg": "error message",
            "errInfo": {
                "writeConcern": {
                    "w": 2,
                    "wtimeout": 0,
                    "provenance": "clientSupplied"
                }
            }
        }
    };

    let write_error_response =
        handle_response_test(&fixtures.op, write_error_response).unwrap_err();
    match *write_error_response.kind {
        ErrorKind::BulkWrite(bwe) => {
            let write_errors = bwe.write_errors.expect("write errors should be present");
            assert_eq!(write_errors.len(), 1);
            let expected_err = BulkWriteError {
                index: 1,
                code: 11000,
                code_name: None,
                message: "duplicate key".to_string(),
                details: Some(doc! { "test key": "test value" }),
            };
            assert_eq!(write_errors.first().unwrap(), &expected_err);

            let write_concern_error = bwe
                .write_concern_error
                .expect("write concern error should be present");
            let expected_wc_err = WriteConcernError {
                code: 123,
                code_name: "woohoo".to_string(),
                message: "error message".to_string(),
                details: Some(doc! { "writeConcern": {
                    "w": 2,
                    "wtimeout": 0,
                    "provenance": "clientSupplied"
                } }),
                labels: vec![],
            };
            assert_eq!(write_concern_error, expected_wc_err);

            assert_eq!(bwe.inserted_ids.len(), 1);
        }
        e => panic!("expected bulk write error, got {:?}", e),
    };
}
