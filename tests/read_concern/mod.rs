use bson::{bson, doc, Bson};
use mongodb::{
    concern::ReadConcern,
    options::{AggregateOptions, DistinctOptions, EstimatedDocumentCountOptions, FindOptions},
};

use crate::util::EventClient;

#[test]
fn test_count_with_read_concern() {
    let client = EventClient::new();
    let collection = client.database("test_db").collection("test_col");
    collection
        .estimated_document_count(Some(
            EstimatedDocumentCountOptions::builder()
                .read_concern(ReadConcern::Local)
                .build(),
        ))
        .unwrap();
    let events: Vec<_> = client
        .events
        .write()
        .unwrap()
        .drain(..)
        .filter(|event| event.command_name == "count")
        .collect();
    assert_eq!(events.len(), 1);
    let read_concern = events[0].command.get("readConcern").unwrap();
    assert_eq!(read_concern, &Bson::Document(doc! {"level" : "local"}));
}

#[test]
fn test_count_without_read_concern() {
    let client = EventClient::new();
    let collection = client.database("test_db").collection("test_col");
    collection.estimated_document_count(None).unwrap();
    let events: Vec<_> = client
        .events
        .write()
        .unwrap()
        .drain(..)
        .filter(|event| event.command_name == "count")
        .collect();
    assert_eq!(events.len(), 1);
    assert!(!events[0].command.contains_key("readConcern"));
}

#[test]
fn test_aggregate_with_read_concern() {
    let client = EventClient::new();
    let collection = client.database("test_db").collection("test_col");
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
    let events: Vec<_> = client
        .events
        .write()
        .unwrap()
        .drain(..)
        .filter(|event| event.command_name == "aggregate")
        .collect();
    assert_eq!(events.len(), 1);
    let read_concern = events[0].command.get("readConcern").unwrap();
    assert_eq!(read_concern, &Bson::Document(doc! {"level" : "local"}));
}

#[test]
fn test_aggregate_without_read_concern() {
    let client = EventClient::new();
    let collection = client.database("test_db").collection("test_col");
    collection.aggregate(None, None).unwrap();
    let events: Vec<_> = client
        .events
        .write()
        .unwrap()
        .drain(..)
        .filter(|event| event.command_name == "aggregate")
        .collect();
    assert_eq!(events.len(), 1);
    assert!(!events[0].command.contains_key("readConcern"));
}

#[test]
fn test_distinct_with_read_concern() {
    let client = EventClient::new();
    let collection = client.database("test_db").collection("test_col");
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
    let events: Vec<_> = client
        .events
        .write()
        .unwrap()
        .drain(..)
        .filter(|event| event.command_name == "distinct")
        .collect();
    assert_eq!(events.len(), 1);
    let read_concern = events[0].command.get("readConcern").unwrap();
    assert_eq!(read_concern, &Bson::Document(doc! {"level" : "local"}));
}

#[test]
fn test_distinct_without_read_concern() {
    let client = EventClient::new();
    let collection = client.database("test_db").collection("test_col");
    collection.distinct("", None, None).unwrap();
    let events: Vec<_> = client
        .events
        .write()
        .unwrap()
        .drain(..)
        .filter(|event| event.command_name == "distinct")
        .collect();
    assert_eq!(events.len(), 1);
    assert!(!events[0].command.contains_key("readConcern"));
}

#[test]
fn test_find_with_read_concern() {
    let client = EventClient::new();
    let collection = client.database("test_db").collection("test_col");
    collection.find(None, None).unwrap();
    let events: Vec<_> = client
        .events
        .write()
        .unwrap()
        .drain(..)
        .filter(|event| event.command_name == "aggregate")
        .collect();
    assert_eq!(events.len(), 1);
    assert!(!events[0].command.contains_key("readConcern"));
}

#[test]
fn test_find_without_read_concern() {
    let client = EventClient::new();
    let collection = client.database("test_db").collection("test_col");
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
    let events: Vec<_> = client
        .events
        .write()
        .unwrap()
        .drain(..)
        .filter(|event| event.command_name == "aggregate")
        .collect();
    assert_eq!(events.len(), 1);
    assert!(!events[0].command.contains_key("readConcern"));
}
