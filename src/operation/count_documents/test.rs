use crate::{
    bson::doc,
    bson_util,
    cmap::StreamDescription,
    coll::Namespace,
    concern::ReadConcern,
    operation::{
        test::{self, handle_response_test},
        Operation,
    },
    options::{CountOptions, Hint},
};

use super::CountDocuments;

#[test]
fn build() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let mut count_op = CountDocuments::new(ns, Some(doc! { "x": 1 }), None).unwrap();
    let mut count_command = count_op
        .build(&StreamDescription::new_testing())
        .expect("error on build");

    let mut expected_body = doc! {
        "aggregate": "test_coll",
        "pipeline": [
            { "$match": { "x": 1 } },
            { "$group": { "_id": 1, "n": { "$sum": 1 } } },
        ],
        "cursor": { }
    };

    bson_util::sort_document(&mut expected_body);
    bson_util::sort_document(&mut count_command.body);

    assert_eq!(count_command.body, expected_body);
    assert_eq!(count_command.target_db, "test_db");
}

#[test]
fn build_with_options() {
    let skip = 2;
    let limit = 5;
    let options = CountOptions::builder()
        .skip(skip)
        .limit(limit)
        .hint(Hint::Name("_id_1".to_string()))
        .read_concern(ReadConcern::available())
        .build();
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let mut count_op = CountDocuments::new(ns, None, Some(options)).unwrap();
    let count_command = count_op
        .build(&StreamDescription::new_testing())
        .expect("error on build");
    assert_eq!(count_command.target_db, "test_db");

    let mut expected_body = doc! {
        "aggregate": "test_coll",
        "$db": "test_db",
        "pipeline": [
            { "$match": {} },
            { "$skip": skip as i64 },
            { "$limit": limit as i64 },
            { "$group": { "_id": 1, "n": { "$sum": 1 } } },
        ],
        "hint": "_id_1",
        "cursor": { },
        "readConcern": { "level": "available" },
    };

    bson_util::sort_document(&mut expected_body);
    let serialized_command = count_op.serialize_command(count_command).unwrap();
    let mut cmd_doc = bson::from_slice(&serialized_command).unwrap();
    bson_util::sort_document(&mut cmd_doc);

    assert_eq!(cmd_doc, expected_body);
}

#[test]
fn op_selection_criteria() {
    test::op_selection_criteria(|selection_criteria| {
        let options = CountOptions {
            selection_criteria,
            ..Default::default()
        };
        CountDocuments::new(Namespace::empty(), None, Some(options)).unwrap()
    });
}

#[test]
fn handle_success() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let count_op = CountDocuments::new(ns, None, None).unwrap();

    let n = 26;
    let response = doc! {
        "ok": 1.0,
        "cursor": {
            "id": 0,
            "ns": "test_db.test_coll",
            "firstBatch": [ { "_id": 1, "n": n as i32 } ],
        }
    };

    let actual_values = handle_response_test(&count_op, response).unwrap();
    assert_eq!(actual_values, n);
}
