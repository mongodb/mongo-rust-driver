use bson::{bson, doc, Bson};
use function_name::named;
use mongodb::{
    concern::ReadConcern,
    options::{
        AggregateOptions, CountOptions, DistinctOptions, EstimatedDocumentCountOptions, FindOptions,
    },
};

use super::run_operation_with_events;

#[test]
#[named]
fn test_count_with_read_concern() {
    let events = run_operation_with_events(function_name!(), "count_documents", |collection| {
        collection
            .count_documents(
                None,
                Some(
                    CountOptions::builder()
                        .read_concern(ReadConcern::Local)
                        .build(),
                ),
            )
            .unwrap();
    });
    let read_concern = events[0].command.get("readConcern").unwrap();
    assert_eq!(read_concern, &Bson::Document(doc! {"level" : "local"}));
}

#[test]
#[named]
fn test_count_without_read_concern() {
    let events = run_operation_with_events(function_name!(), "count_documents", |collection| {
        collection.count_documents(None, None).unwrap();
    });
    assert!(!events[0].command.contains_key("readConcern"));
}

#[test]
#[named]
fn test_estimated_count_documents_with_read_concern() {
    let events =
        run_operation_with_events(function_name!(), "estimated_document_count", |collection| {
            collection
                .estimated_document_count(Some(
                    EstimatedDocumentCountOptions::builder()
                        .read_concern(ReadConcern::Local)
                        .build(),
                ))
                .unwrap();
        });

    let read_concern = events[0].command.get("readConcern").unwrap();
    assert_eq!(read_concern, &Bson::Document(doc! {"level" : "local"}));
}

#[test]
#[named]
fn test_estimated_count_documents_without_read_concern() {
    let events =
        run_operation_with_events(function_name!(), "estimated_document_count", |collection| {
            collection.estimated_document_count(None).unwrap();
        });
    assert!(!events[0].command.contains_key("readConcern"));
}

#[test]
#[named]
fn test_aggregate_with_read_concern() {
    let events = run_operation_with_events(function_name!(), "aggregate", |collection| {
        collection
            .aggregate(
                None,
                Some(
                    AggregateOptions::builder()
                        .read_concern(ReadConcern::Local)
                        .build(),
                ),
            )
            .unwrap();
    });

    let read_concern = events[0].command.get("readConcern").unwrap();
    assert_eq!(read_concern, &Bson::Document(doc! {"level" : "local"}));
}

#[test]
#[named]
fn test_aggregate_without_read_concern() {
    let events = run_operation_with_events(function_name!(), "aggregate", |collection| {
        collection.aggregate(None, None).unwrap();
    });
    assert!(!events[0].command.contains_key("readConcern"));
}

#[test]
#[named]
fn test_distinct_with_read_concern() {
    let events = run_operation_with_events(function_name!(), "distinct", |collection| {
        collection
            .distinct(
                "",
                None,
                Some(
                    DistinctOptions::builder()
                        .read_concern(ReadConcern::Local)
                        .build(),
                ),
            )
            .unwrap();
    });
    let read_concern = events[0].command.get("readConcern").unwrap();
    assert_eq!(read_concern, &Bson::Document(doc! {"level" : "local"}));
}

#[test]
#[named]
fn test_distinct_without_read_concern() {
    let events = run_operation_with_events(function_name!(), "distinct", |collection| {
        collection.distinct("", None, None).unwrap();
    });
    assert!(!events[0].command.contains_key("readConcern"));
}

#[test]
#[named]
fn test_find_with_read_concern() {
    let events = run_operation_with_events(function_name!(), "find", |collection| {
        collection
            .find(
                None,
                Some(
                    FindOptions::builder()
                        .read_concern(ReadConcern::Local)
                        .build(),
                ),
            )
            .unwrap();
    });

    assert!(!events[0].command.contains_key("readConcern"));
}

#[test]
#[named]
fn test_find_without_read_concern() {
    let events = run_operation_with_events(function_name!(), "find", |collection| {
        collection.find(None, None).unwrap();
    });

    assert!(!events[0].command.contains_key("readConcern"));
}
