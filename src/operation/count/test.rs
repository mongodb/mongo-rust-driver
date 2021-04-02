use std::time::Duration;

use crate::{
    bson::doc,
    cmap::{CommandResponse, StreamDescription},
    coll::{options::EstimatedDocumentCountOptions, Namespace},
    concern::ReadConcern,
    error::ErrorKind,
    operation::{test, Count, Operation},
    options::ReadConcernLevel,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let count_op = Count::new(ns, None);
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_with_options() {
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
    let count_op = Count::new(ns, Some(options));
    let count_command = count_op
        .build(&StreamDescription::new_testing())
        .expect("error on build");

    assert_eq!(
        count_command.body,
        doc! {
            "count": "test_coll",
            "maxTimeMS": max_time.as_millis() as i32,
            "readConcern": doc!{"level": read_concern.level.as_str().to_string()}
        }
    );
    assert_eq!(count_command.target_db, "test_db");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn op_selection_criteria() {
    test::op_selection_criteria(|selection_criteria| {
        let options = EstimatedDocumentCountOptions {
            selection_criteria,
            ..Default::default()
        };
        Count::new(Namespace::empty(), Some(options))
    });
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let count_op = Count::empty();

    let n = 26;
    let response = CommandResponse::with_document(doc! { "n" : n, "ok" : 1 });

    let actual_values = count_op
        .handle_response(response, &Default::default())
        .expect("supposed to succeed");

    assert_eq!(actual_values, n);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_response_no_n() {
    let count_op = Count::empty();

    let response = CommandResponse::with_document(doc! { "ok" : 1 });

    let result = count_op.handle_response(response, &Default::default());
    match result.as_ref().map_err(|e| &e.kind) {
        Err(ErrorKind::ResponseError { .. }) => {}
        other => panic!("expected response error, but got {:?}", other),
    }
}
