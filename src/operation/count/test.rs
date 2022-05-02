use std::time::Duration;

use crate::{
    bson::doc,
    bson_util,
    cmap::StreamDescription,
    coll::{options::EstimatedDocumentCountOptions, Namespace},
    concern::ReadConcern,
    operation::{
        test::{self, handle_response_test},
        Count,
        Operation,
    },
    options::ReadConcernLevel,
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
fn build_with_options() {
    let read_concern: ReadConcern = ReadConcernLevel::Local.into();
    let max_time = Duration::from_millis(2_u64);
    let options: EstimatedDocumentCountOptions = EstimatedDocumentCountOptions::builder()
        .max_time(max_time)
        .read_concern(read_concern.clone())
        .build();
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let mut count_op = Count::new(ns, Some(options));
    let count_command = count_op
        .build(&StreamDescription::new_testing())
        .expect("error on build");

    assert_eq!(count_command.target_db, "test_db");

    let mut expected_body = doc! {
        "count": "test_coll",
        "$db": "test_db",
        "maxTimeMS": max_time.as_millis() as i32,
        "readConcern": doc! {"level": read_concern.level.as_str().to_string() },
    };

    let cmd_bytes = count_op.serialize_command(count_command).unwrap();
    let mut cmd_doc = bson::from_slice(&cmd_bytes).unwrap();

    bson_util::sort_document(&mut cmd_doc);
    bson_util::sort_document(&mut expected_body);

    assert_eq!(cmd_doc, expected_body);
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
