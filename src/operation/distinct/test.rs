use std::time::Duration;

use crate::{
    bson::{doc, Bson},
    cmap::{CommandResponse, StreamDescription},
    coll::{options::DistinctOptions, Namespace},
    error::ErrorKind,
    operation::{test, Distinct, Operation},
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
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
            "key": field_name
        }
    );
    assert_eq!(distinct_command.target_db, "test_db");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_with_query() {
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
            "key": field_name,
            "query": Bson::Document(query)
        }
    );
    assert_eq!(distinct_command.target_db, "test_db");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_with_options() {
    let field_name = "field_name".to_string();
    let max_time = Duration::new(2_u64, 0);
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
            "key": field_name,
            "maxTimeMS": max_time.as_millis() as i32
        }
    );
    assert_eq!(distinct_command.target_db, "test_db");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn op_selection_criteria() {
    test::op_selection_criteria(|selection_criteria| {
        let options = DistinctOptions {
            selection_criteria,
            ..Default::default()
        };
        Distinct::new(Namespace::empty(), String::new(), None, Some(options))
    });
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let distinct_op = Distinct::empty();

    let expected_values: Vec<Bson> =
        vec![Bson::String("A".to_string()), Bson::String("B".to_string())];

    let response = CommandResponse::with_document(doc! {
       "values" : expected_values.clone(),
       "ok" : 1
    });

    let actual_values = distinct_op
        .handle_response(response, &Default::default())
        .expect("supposed to succeed");

    assert_eq!(actual_values, expected_values);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_response_with_empty_values() {
    let distinct_op = Distinct::empty();

    let response = CommandResponse::with_document(doc! {
       "values" : [],
       "ok" : 1
    });

    let expected_values: Vec<Bson> = Vec::new();

    let actual_values = distinct_op
        .handle_response(response, &Default::default())
        .expect("supposed to succeed");

    assert_eq!(actual_values, expected_values);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_response_no_values() {
    let distinct_op = Distinct::empty();

    let response = CommandResponse::with_document(doc! {
       "ok" : 1
    });

    let result = distinct_op.handle_response(response, &Default::default());
    match result.as_ref().map_err(|e| &e.kind) {
        Err(ErrorKind::ResponseError { .. }) => {}
        other => panic!("expected response error, but got {:?}", other),
    }
}
