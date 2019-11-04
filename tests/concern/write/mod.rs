use bson::{bson, doc, Bson};
use function_name::named;
use mongodb::{
    concern::{Acknowledgment, WriteConcern},
    options::{
        AggregateOptions, DeleteOptions, FindOneAndDeleteOptions, FindOneAndReplaceOptions,
        FindOneAndUpdateOptions, InsertManyOptions, InsertOneOptions, ReplaceOptions,
        UpdateOptions,
    },
};

use super::run_operation_with_events;

#[test]
#[named]
fn test_aggregate_with_write_concern() {
    let events = run_operation_with_events(function_name!(), "aggregate", |collection| {
        collection
            .aggregate(
                None,
                Some(
                    AggregateOptions::builder()
                        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
                        .build(),
                ),
            )
            .unwrap();
    });
    let write_concern = events[0].command.get("writeConcern").unwrap();
    assert_eq!(write_concern, &Bson::Document(doc! {"w" : "majority"}));
}

#[test]
#[named]
fn test_aggregate_without_write_concern() {
    let events = run_operation_with_events(function_name!(), "aggregate", |collection| {
        collection.aggregate(None, None).unwrap();
    });
    assert!(!events[0].command.contains_key("writeConcern"));
}

#[test]
#[named]
fn test_delete_many_with_write_concern() {
    let events = run_operation_with_events(function_name!(), "delete_many", |collection| {
        collection
            .delete_many(
                doc! {},
                Some(
                    DeleteOptions::builder()
                        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
                        .build(),
                ),
            )
            .unwrap();
    });
    let write_concern = events[0].command.get("writeConcern").unwrap();
    assert_eq!(write_concern, &Bson::Document(doc! {"w" : "majority"}));
}

#[test]
#[named]
fn test_delete_many_without_write_concern() {
    let events = run_operation_with_events(function_name!(), "delete_many", |collection| {
        collection.delete_many(doc! {}, None).unwrap();
    });
    assert!(!events[0].command.contains_key("writeConcern"));
}

#[test]
#[named]
fn test_delete_one_with_write_concern() {
    let events = run_operation_with_events(function_name!(), "delete_one", |collection| {
        collection
            .delete_one(
                doc! {},
                Some(
                    DeleteOptions::builder()
                        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
                        .build(),
                ),
            )
            .unwrap();
    });
    let write_concern = events[0].command.get("writeConcern").unwrap();
    assert_eq!(write_concern, &Bson::Document(doc! {"w" : "majority"}));
}

#[test]
#[named]
fn test_delete_one_without_write_concern() {
    let events = run_operation_with_events(function_name!(), "delete_one", |collection| {
        collection.delete_one(doc! {}, None).unwrap();
    });
    assert!(!events[0].command.contains_key("writeConcern"));
}

#[test]
#[named]
fn test_find_one_and_delete_with_write_concern() {
    let events = run_operation_with_events(function_name!(), "find_one_and_delete", |collection| {
        collection
            .find_one_and_delete(
                doc! {},
                Some(
                    FindOneAndDeleteOptions::builder()
                        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
                        .build(),
                ),
            )
            .unwrap();
    });
    let write_concern = events[0].command.get("writeConcern").unwrap();
    assert_eq!(write_concern, &Bson::Document(doc! {"w" : "majority"}));
}

#[test]
#[named]
fn test_find_one_and_delete_without_write_concern() {
    let events = run_operation_with_events(function_name!(), "find_one_and_delete", |collection| {
        collection.find_one_and_delete(doc! {}, None).unwrap();
    });
    assert!(!events[0].command.contains_key("writeConcern"));
}

#[test]
#[named]
fn test_find_one_and_replace_with_write_concern() {
    let events =
        run_operation_with_events(function_name!(), "find_one_and_replace", |collection| {
            collection
                .find_one_and_replace(
                    doc! {},
                    doc! {},
                    Some(
                        FindOneAndReplaceOptions::builder()
                            .write_concern(
                                WriteConcern::builder().w(Acknowledgment::Majority).build(),
                            )
                            .build(),
                    ),
                )
                .unwrap();
        });
    let write_concern = events[0].command.get("writeConcern").unwrap();
    assert_eq!(write_concern, &Bson::Document(doc! {"w" : "majority"}));
}

#[test]
#[named]
fn test_find_one_and_replace_without_write_concern() {
    let events =
        run_operation_with_events(function_name!(), "find_one_and_replace", |collection| {
            collection
                .find_one_and_replace(doc! {}, doc! {}, None)
                .unwrap();
        });
    assert!(!events[0].command.contains_key("writeConcern"));
}

#[test]
#[named]
fn test_find_one_and_update_with_write_concern() {
    let events = run_operation_with_events(function_name!(), "find_one_and_update", |collection| {
        collection
            .find_one_and_update(
                doc! {},
                doc! {},
                Some(
                    FindOneAndUpdateOptions::builder()
                        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
                        .build(),
                ),
            )
            .unwrap();
    });
    let write_concern = events[0].command.get("writeConcern").unwrap();
    assert_eq!(write_concern, &Bson::Document(doc! {"w" : "majority"}));
}

#[test]
#[named]
fn test_find_one_and_update_without_write_concern() {
    let events = run_operation_with_events(function_name!(), "find_one_and_update", |collection| {
        collection
            .find_one_and_update(doc! {}, doc! {}, None)
            .unwrap();
    });
    assert!(!events[0].command.contains_key("writeConcern"));
}

#[test]
#[named]
fn test_insert_many_with_write_concern() {
    let events = run_operation_with_events(function_name!(), "insert_many", |collection| {
        collection
            .insert_many(
                None,
                Some(
                    InsertManyOptions::builder()
                        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
                        .build(),
                ),
            )
            .unwrap();
    });
    let write_concern = events[0].command.get("writeConcern").unwrap();
    assert_eq!(write_concern, &Bson::Document(doc! {"w" : "majority"}));
}

#[test]
#[named]
fn test_insert_many_without_write_concern() {
    let events = run_operation_with_events(function_name!(), "insert_many", |collection| {
        collection.insert_many(None, None).unwrap();
    });
    assert!(!events[0].command.contains_key("writeConcern"));
}

#[test]
#[named]
fn test_insert_one_with_write_concern() {
    let events = run_operation_with_events(function_name!(), "insert_one", |collection| {
        collection
            .insert_one(
                doc! {},
                Some(
                    InsertOneOptions::builder()
                        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
                        .build(),
                ),
            )
            .unwrap();
    });
    let write_concern = events[0].command.get("writeConcern").unwrap();
    assert_eq!(write_concern, &Bson::Document(doc! {"w" : "majority"}));
}

#[test]
#[named]
fn test_insert_one_without_write_concern() {
    let events = run_operation_with_events(function_name!(), "insert_one", |collection| {
        collection.insert_one(doc! {}, None).unwrap();
    });
    assert!(!events[0].command.contains_key("writeConcern"));
}

#[test]
#[named]
fn test_replace_one_with_write_concern() {
    let events = run_operation_with_events(function_name!(), "replace_one", |collection| {
        collection
            .replace_one(
                doc! {},
                doc! {},
                Some(
                    ReplaceOptions::builder()
                        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
                        .build(),
                ),
            )
            .unwrap();
    });
    let write_concern = events[0].command.get("writeConcern").unwrap();
    assert_eq!(write_concern, &Bson::Document(doc! {"w" : "majority"}));
}

#[test]
#[named]
fn test_replace_one_without_write_concern() {
    let events = run_operation_with_events(function_name!(), "replace_one", |collection| {
        collection.replace_one(doc! {}, doc! {}, None).unwrap();
    });
    assert!(!events[0].command.contains_key("writeConcern"));
}

#[test]
#[named]
fn test_update_many_with_write_concern() {
    let events = run_operation_with_events(function_name!(), "update_many", |collection| {
        collection
            .update_many(
                doc! {},
                doc! {},
                Some(
                    UpdateOptions::builder()
                        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
                        .build(),
                ),
            )
            .unwrap();
    });
    let write_concern = events[0].command.get("writeConcern").unwrap();
    assert_eq!(write_concern, &Bson::Document(doc! {"w" : "majority"}));
}

#[test]
#[named]
fn test_update_many_without_write_concern() {
    let events = run_operation_with_events(function_name!(), "update_many", |collection| {
        collection.update_many(doc! {}, doc! {}, None).unwrap();
    });
    assert!(!events[0].command.contains_key("writeConcern"));
}

#[test]
#[named]
fn test_update_one_with_write_concern() {
    let events = run_operation_with_events(function_name!(), "update_one", |collection| {
        collection
            .update_one(
                doc! {},
                doc! {},
                Some(
                    UpdateOptions::builder()
                        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
                        .build(),
                ),
            )
            .unwrap();
    });
    let write_concern = events[0].command.get("writeConcern").unwrap();
    assert_eq!(write_concern, &Bson::Document(doc! {"w" : "majority"}));
}

#[test]
#[named]
fn test_update_one_without_write_concern() {
    let events = run_operation_with_events(function_name!(), "update_one", |collection| {
        collection.update_one(doc! {}, doc! {}, None).unwrap();
    });
    assert!(!events[0].command.contains_key("writeConcern"));
}
