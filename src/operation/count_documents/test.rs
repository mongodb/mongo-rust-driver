#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]

use crate::{
    bson::doc,
    bson_util,
    cmap::StreamDescription,
    coll::Namespace,
    operation::{test::handle_response_test, Operation},
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
