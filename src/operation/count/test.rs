#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]

use crate::{
    bson::doc,
    cmap::StreamDescription,
    coll::{options::EstimatedDocumentCountOptions, Namespace},
    operation::{
        test::{self, handle_response_test},
        Count,
        Operation,
    },
};

#[test]
fn build() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let mut count_op = Count::new(ns, None);
    let count_command = count_op
        .build(&StreamDescription::new_testing())
        .expect("error on build");
    assert_eq!(
        count_command.body,
        doc! {
            "count": "test_coll",
        }
    );
    assert_eq!(count_command.target_db, "test_db");
}

#[test]
fn op_selection_criteria() {
    test::op_selection_criteria(|selection_criteria| {
        let options = EstimatedDocumentCountOptions {
            selection_criteria,
            ..Default::default()
        };
        Count::new(Namespace::empty(), Some(options))
    });
}

#[test]
fn handle_success() {
    let count_op = Count::empty();

    let n = 26;
    let response = doc! { "ok": 1.0, "n": n as i32 };

    let actual_values = handle_response_test(&count_op, response).unwrap();
    assert_eq!(actual_values, n);
}

#[test]
fn handle_response_no_n() {
    let count_op = Count::empty();
    handle_response_test(&count_op, doc! { "ok": 1.0 }).unwrap_err();
}
