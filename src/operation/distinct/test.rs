use std::time::Duration;

use bson::{bson, doc, Bson};

use crate::{
    cmap::{CommandResponse, StreamDescription},
    coll::{options::DistinctOptions, Namespace},
    error::ErrorKind,
    operation::Operation,
    operation::{test, Distinct},
};

#[test]
fn build() {
    let field_name = "field_name".to_string();
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let distinct_op = Distinct::new(ns, field_name.clone(), None, None);
    let distinct_command = distinct_op
        .build(&StreamDescription::new_testing())
        .expect("error on build");
    assert_eq!(
        distinct_command.body,
        doc! {
            "distinct": "test_coll",
            "key": field_name.clone()
        }
    );
    assert_eq!(distinct_command.target_db, "test_db");
    assert_eq!(distinct_command.read_pref, None);
}

#[test]
fn build_with_query() {
    let field_name = "field_name".to_string();
    let query = doc! {"something" : "something else"};
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let distinct_op = Distinct::new(ns, field_name.clone(), Some(query.clone()), None);
    let distinct_command = distinct_op
        .build(&StreamDescription::new_testing())
        .expect("error on build");
    assert_eq!(
        distinct_command.body,
        doc! {
            "distinct": "test_coll",
            "key": field_name.clone(),
            "query": Bson::Document(query.clone())
        }
    );
    assert_eq!(distinct_command.target_db, "test_db");
    assert_eq!(distinct_command.read_pref, None);
}

#[test]
fn build_with_options() {
    let field_name = "field_name".to_string();
    let max_time = Duration::new(2 as u64, 0);
    let options: DistinctOptions = DistinctOptions::builder().max_time(max_time).build();
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let distinct_op = Distinct::new(ns, field_name.clone(), None, Some(options));
    let distinct_command = distinct_op
        .build(&StreamDescription::new_testing())
        .expect("error on build");

    assert_eq!(
        distinct_command.body,
        doc! {
            "distinct": "test_coll",
            "key": field_name.clone(),
            "maxTimeMS": max_time.as_millis() as i64
        }
    );
    assert_eq!(distinct_command.target_db, "test_db");
    assert_eq!(distinct_command.read_pref, None);
}

#[test]
fn handle_success() {
    let distinct_op = Distinct::empty();

    let expected_values: Vec<Bson> =
        vec![Bson::String("A".to_string()), Bson::String("B".to_string())];

    let response = CommandResponse::with_document(doc! {
       "values" : expected_values.clone(),
       "ok" : 1
    });

    let actual_values = distinct_op
        .handle_response(response)
        .expect("supposed to succeed");

    assert_eq!(actual_values, expected_values);
}

#[test]
fn handle_response_with_empty_values() {
    let distinct_op = Distinct::empty();

    let response = CommandResponse::with_document(doc! {
       "values" : [],
       "ok" : 1
    });

    let expected_values: Vec<Bson> = Vec::new();

    let actual_values = distinct_op
        .handle_response(response)
        .expect("supposed to succeed");

    assert_eq!(actual_values, expected_values);
}

#[test]
fn handle_response_no_values() {
    let distinct_op = Distinct::empty();

    let response = CommandResponse::with_document(doc! {
       "ok" : 1
    });

    let result = distinct_op.handle_response(response);
    match result.as_ref().map_err(|e| e.as_ref()) {
        Err(ErrorKind::ResponseError { .. }) => {}
        other => panic!("expected response error, but got {:?}", other),
    }
}

#[test]
fn handle_response_no_ok() {
    let distinct_op = Distinct::empty();

    let response = CommandResponse::with_document(doc! {
       "values" : [ "A", "B" ],
    });

    let result = distinct_op.handle_response(response);
    match result.as_ref().map_err(|e| e.as_ref()) {
        Err(ErrorKind::ResponseError { .. }) => {}
        other => panic!("expected response error, but got {:?}", other),
    }
}

#[test]
fn handle_command_error() {
    test::handle_command_error(Distinct::empty());
}
