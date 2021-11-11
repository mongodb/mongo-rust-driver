use bson::{
    oid::ObjectId,
    spec::BinarySubtype,
    Binary,
    DateTime,
    JavaScriptCodeWithScope,
    Regex,
    Timestamp,
};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use crate::{
    bson::{doc, Bson, Document},
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
    options: InsertManyOptions,
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
    );

    TestFixtures {
        op,
        documents: DOCUMENTS.clone(),
        options,
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let mut fixtures = fixtures(None);

    let description = StreamDescription::new_testing();
    let cmd = fixtures.op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "insert");
    assert_eq!(cmd.target_db.as_str(), "test_db");

    assert_eq!(cmd.body.insert, "test_coll".to_string());

    let mut cmd_docs: Vec<Document> = cmd
        .body
        .documents
        .documents
        .iter()
        .map(|b| Document::from_reader(b.as_slice()).unwrap())
        .collect();
    assert_eq!(cmd_docs.len(), fixtures.documents.len());

    for (original_doc, cmd_doc) in fixtures.documents.iter().zip(cmd_docs.iter_mut()) {
        assert!(cmd_doc.get("_id").is_some());
        if original_doc.get("_id").is_none() {
            cmd_doc.remove("_id");
        }
        assert_eq!(original_doc, cmd_doc);
    }

    let serialized = fixtures.op.serialize_command(cmd).unwrap();
    let cmd_doc = Document::from_reader(serialized.as_slice()).unwrap();

    assert_eq!(
        cmd_doc.get("ordered"),
        fixtures.options.ordered.map(Bson::Boolean).as_ref()
    );
    assert_eq!(
        cmd_doc.get("bypassDocumentValidation"),
        fixtures
            .options
            .bypass_document_validation
            .map(Bson::Boolean)
            .as_ref()
    );
    assert_eq!(
        cmd_doc.get("writeConcern"),
        fixtures
            .options
            .write_concern
            .as_ref()
            .map(|wc| bson::to_bson(wc).unwrap())
            .as_ref()
    );
}

#[test]
fn build_no_write_concern() {
    let options = InsertManyOptions {
        ordered: Some(true),
        write_concern: Some(WriteConcern::builder().build()),
        ..Default::default()
    };
    let mut fixtures = fixtures(Some(options));
    let cmd = fixtures
        .op
        .build(&StreamDescription::new_testing())
        .unwrap();

    let serialized = fixtures.op.serialize_command(cmd).unwrap();
    let cmd_doc = Document::from_reader(serialized.as_slice()).unwrap();

    assert_eq!(
        cmd_doc.get("ordered"),
        fixtures.options.ordered.map(Bson::Boolean).as_ref()
    );
    assert_eq!(
        cmd_doc.get("bypassDocumentValidation"),
        fixtures
            .options
            .bypass_document_validation
            .map(Bson::Boolean)
            .as_ref()
    );
    assert_eq!(cmd_doc.get("writeConcern"), None);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_ordered() {
    let docs = vec![Document::new()];
    let mut insert = Insert::new(Namespace::empty(), docs.iter().collect(), None);
    let cmd = insert
        .build(&StreamDescription::new_testing())
        .expect("should succeed");
    let serialized = insert.serialize_command(cmd).unwrap();
    let cmd_doc = Document::from_reader(serialized.as_slice()).unwrap();
    assert_eq!(cmd_doc.get("ordered"), Some(&Bson::Boolean(true)));

    let mut insert = Insert::new(
        Namespace::empty(),
        docs.iter().collect(),
        Some(InsertManyOptions::builder().ordered(false).build()),
    );
    let cmd = insert
        .build(&StreamDescription::new_testing())
        .expect("should succeed");
    let serialized = insert.serialize_command(cmd).unwrap();
    let cmd_doc = Document::from_reader(serialized.as_slice()).unwrap();
    assert_eq!(cmd_doc.get("ordered"), Some(&Bson::Boolean(false)));

    let mut insert = Insert::new(
        Namespace::empty(),
        docs.iter().collect(),
        Some(InsertManyOptions::builder().ordered(true).build()),
    );
    let cmd = insert
        .build(&StreamDescription::new_testing())
        .expect("should succeed");
    let serialized = insert.serialize_command(cmd).unwrap();
    let cmd_doc = Document::from_reader(serialized.as_slice()).unwrap();
    assert_eq!(cmd_doc.get("ordered"), Some(&Bson::Boolean(true)));

    let mut insert = Insert::new(
        Namespace::empty(),
        docs.iter().collect(),
        Some(InsertManyOptions::builder().build()),
    );
    let cmd = insert
        .build(&StreamDescription::new_testing())
        .expect("should succeed");
    let serialized = insert.serialize_command(cmd).unwrap();
    let cmd_doc = Document::from_reader(serialized.as_slice()).unwrap();
    assert_eq!(cmd_doc.get("ordered"), Some(&Bson::Boolean(true)));
}

#[derive(Debug, Serialize, Deserialize)]
struct Documents<D> {
    documents: Vec<D>,
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn generate_ids() {
    let docs = vec![doc! { "x": 1 }, doc! { "_id": 1_i32, "x": 2 }];

    let mut insert = Insert::new(Namespace::empty(), docs.iter().collect(), None);
    let cmd = insert.build(&StreamDescription::new_testing()).unwrap();
    let serialized = insert.serialize_command(cmd).unwrap();

    #[derive(Debug, Serialize, Deserialize)]
    struct D {
        x: i32,

        #[serde(rename = "_id")]
        id: Bson,
    }

    let docs: Documents<D> = bson::from_slice(serialized.as_slice()).unwrap();

    assert_eq!(docs.documents.len(), 2);
    let docs = docs.documents;

    docs[0].id.as_object_id().unwrap();
    assert_eq!(docs[0].x, 1);

    assert_eq!(docs[1].id, Bson::Int32(1));
    assert_eq!(docs[1].x, 2);

    // ensure the _id was prepended to the document
    let docs: Documents<Document> = bson::from_slice(serialized.as_slice()).unwrap();
    assert_eq!(docs.documents[0].iter().next().unwrap().0, "_id")
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn serialize_all_types() {
    let binary = Binary {
        bytes: vec![36, 36, 36],
        subtype: BinarySubtype::Generic,
    };
    let date = DateTime::now();
    let regex = Regex {
        pattern: "hello".to_string(),
        options: "x".to_string(),
    };
    let timestamp = Timestamp {
        time: 123,
        increment: 456,
    };
    let code = Bson::JavaScriptCode("console.log(1)".to_string());
    let code_w_scope = JavaScriptCodeWithScope {
        code: "console.log(a)".to_string(),
        scope: doc! { "a": 1 },
    };
    let oid = ObjectId::new();
    let subdoc = doc! { "k": true, "b": { "hello": "world" } };

    let decimal = {
        let bytes = hex::decode("18000000136400D0070000000000000000000000003A3000").unwrap();
        let d = Document::from_reader(bytes.as_slice()).unwrap();
        d.get("d").unwrap().clone()
    };

    let docs = vec![doc! {
        "x": 1_i32,
        "y": 2_i64,
        "s": "oke",
        "array": [ true, "oke", { "12": 24 } ],
        "bson": 1234.5,
        "oid": oid,
        "null": Bson::Null,
        "subdoc": subdoc,
        "b": true,
        "d": 12.5,
        "binary": binary,
        "date": date,
        "regex": regex,
        "ts": timestamp,
        "i": { "a": 300, "b": 12345 },
        "undefined": Bson::Undefined,
        "code": code,
        "code_w_scope": code_w_scope,
        "decimal": decimal,
        "symbol": Bson::Symbol("ok".to_string()),
        "min_key": Bson::MinKey,
        "max_key": Bson::MaxKey,
        "_id": ObjectId::new(),
    }];

    let mut insert = Insert::new(Namespace::empty(), docs.iter().collect(), None);
    let cmd = insert.build(&StreamDescription::new_testing()).unwrap();
    let serialized = insert.serialize_command(cmd).unwrap();
    let cmd: Documents<Document> = bson::from_slice(serialized.as_slice()).unwrap();

    assert_eq!(cmd.documents, docs);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_invalid_response() {
    let fixtures = fixtures(None);
    handle_response_test(&fixtures.op, doc! { "ok": 1.0, "asdfadsf": 123123 }).unwrap_err();
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_write_failure() {
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
            };
            assert_eq!(write_concern_error, expected_wc_err);

            assert_eq!(bwe.inserted_ids.len(), 1);
        }
        e => panic!("expected bulk write error, got {:?}", e),
    };
}
