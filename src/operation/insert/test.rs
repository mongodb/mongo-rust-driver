use crate::{
    bson::{doc, Bson, Document},
    cmap::{CommandResponse, StreamDescription},
    concern::WriteConcern,
    error::{BulkWriteError, ErrorKind, WriteConcernError},
    operation::{Insert, Operation},
    options::InsertManyOptions,
    Namespace,
};

struct TestFixtures {
    op: Insert,
    documents: Vec<Document>,
    options: InsertManyOptions,
}

/// Get an Insert operation and the documents/options used to construct it.
fn fixtures() -> TestFixtures {
    let documents = vec![
        Document::new(),
        doc! {"_id": 1234, "a": 1},
        doc! {"a": 123, "b": "hello world" },
    ];

    let options = InsertManyOptions {
        ordered: Some(true),
        write_concern: Some(WriteConcern::builder().journal(true).build()),
        ..Default::default()
    };

    let op = Insert::new(
        Namespace {
            db: "test_db".to_string(),
            coll: "test_coll".to_string(),
        },
        documents.clone(),
        Some(options.clone()),
    );

    TestFixtures {
        op,
        documents,
        options,
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let fixtures = fixtures();

    let description = StreamDescription::new_testing();
    let cmd = fixtures.op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "insert");
    assert_eq!(cmd.target_db.as_str(), "test_db");

    assert_eq!(
        cmd.body.get("insert").unwrap(),
        &Bson::String("test_coll".to_string())
    );

    let mut cmd_docs: Vec<Document> = cmd
        .body
        .get("documents")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b.as_document().unwrap().clone())
        .collect();
    assert_eq!(cmd_docs.len(), fixtures.documents.len());

    for (original_doc, cmd_doc) in fixtures.documents.iter().zip(cmd_docs.iter_mut()) {
        assert!(cmd_doc.get("_id").is_some());
        if original_doc.get("_id").is_some() {
            assert_eq!(original_doc, cmd_doc);
        } else {
            cmd_doc.remove("_id");
            assert_eq!(original_doc, cmd_doc);
        };
    }

    assert_eq!(
        cmd.body.get("ordered"),
        fixtures.options.ordered.map(Bson::Boolean).as_ref()
    );
    assert_eq!(
        cmd.body.get("bypassDocumentValidation"),
        fixtures
            .options
            .bypass_document_validation
            .map(Bson::Boolean)
            .as_ref()
    );
    assert_eq!(
        cmd.body.get("writeConcern"),
        fixtures
            .options
            .write_concern
            .as_ref()
            .map(|wc| bson::to_bson(wc).unwrap())
            .as_ref()
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_ordered() {
    let insert = Insert::new(Namespace::empty(), Vec::new(), None);
    let cmd = insert
        .build(&StreamDescription::new_testing())
        .expect("should succeed");
    assert_eq!(cmd.body.get("ordered"), Some(&Bson::Boolean(true)));

    let insert = Insert::new(
        Namespace::empty(),
        Vec::new(),
        Some(InsertManyOptions::builder().ordered(false).build()),
    );
    let cmd = insert
        .build(&StreamDescription::new_testing())
        .expect("should succeed");
    assert_eq!(cmd.body.get("ordered"), Some(&Bson::Boolean(false)));

    let insert = Insert::new(
        Namespace::empty(),
        Vec::new(),
        Some(InsertManyOptions::builder().ordered(true).build()),
    );
    let cmd = insert
        .build(&StreamDescription::new_testing())
        .expect("should succeed");
    assert_eq!(cmd.body.get("ordered"), Some(&Bson::Boolean(true)));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let fixtures = fixtures();

    let ok_response = CommandResponse::with_document(doc! { "ok": 1.0, "n": 3 });
    let ok_result = fixtures
        .op
        .handle_response(ok_response, &Default::default());
    assert!(ok_result.is_ok());

    let inserted_ids = ok_result.unwrap().inserted_ids;
    assert_eq!(inserted_ids.len(), 3); // populate _id for documents that don't provide it
    assert_eq!(
        inserted_ids.get(&1).unwrap(),
        fixtures.documents[1].get("_id").unwrap()
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_invalid_response() {
    let fixtures = fixtures();

    let invalid_response = CommandResponse::with_document(doc! { "ok": 1.0, "asdfadsf": 123123 });
    assert!(fixtures
        .op
        .handle_response(invalid_response, &Default::default())
        .is_err());
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_write_failure() {
    let fixtures = fixtures();

    let write_error_response = CommandResponse::with_document(doc! {
        "ok": 1.0,
        "n": 1,
        "writeErrors": [
            {
                "index": 1,
                "code": 11000,
                "errmsg": "duplicate key"
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
    });

    let write_error_result = fixtures
        .op
        .handle_response(write_error_response, &Default::default());
    assert!(write_error_result.is_err());

    match write_error_result.unwrap_err().kind {
        ErrorKind::BulkWriteError(ref error) => {
            let write_errors = error.write_errors.clone().unwrap();
            assert_eq!(write_errors.len(), 1);
            let expected_err = BulkWriteError {
                index: 1,
                code: 11000,
                code_name: None,
                message: "duplicate key".to_string(),
            };
            assert_eq!(write_errors.first().unwrap(), &expected_err);

            let write_concern_error = error.write_concern_error.clone().unwrap();
            let expected_wc_err = WriteConcernError {
                code: 123,
                code_name: "woohoo".to_string(),
                message: "error message".to_string(),
                details: Some(doc! { "writeConcern": {
                    "w": 2,
                    "wtimeout": 0,
                    "provenance": "clientSupplied"
                } }),
                labels: Vec::new(),
            };
            assert_eq!(write_concern_error, expected_wc_err);
        }
        ref e => panic!("expected bulk write error, got {:?}", e),
    };
}
