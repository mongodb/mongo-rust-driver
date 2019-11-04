use bson::{bson, doc, Bson};
use function_name::named;
use mongodb::{
    concern::ReadConcern,
    options::{
        AggregateOptions, CountOptions, DistinctOptions, EstimatedDocumentCountOptions, FindOptions,
    },
};

use crate::util::EventClient;

#[test]
#[named]
fn test_count_with_read_concern() {
    let client = EventClient::new();
    let collection = client
        .database(function_name!())
        .collection(function_name!());
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
    let events: Vec<_> = client
        .events
        .write()
        .unwrap()
        .drain(..)
        .filter(|event| event.command_name == "count_documents")
        .collect();
    assert_eq!(events.len(), 1);
    let read_concern = events[0].command.get("readConcern").unwrap();
    assert_eq!(read_concern, &Bson::Document(doc! {"level" : "local"}));
}

#[test]
#[named]
fn test_count_without_read_concern() {
    let client = EventClient::new();
    let collection = client
        .database(function_name!())
        .collection(function_name!());
    collection.count_documents(None, None).unwrap();
    let events: Vec<_> = client
        .events
        .write()
        .unwrap()
        .drain(..)
        .filter(|event| event.command_name == "count_documents")
        .collect();
    assert_eq!(events.len(), 1);
    assert!(!events[0].command.contains_key("readConcern"));
}

#[test]
#[named]
fn test_estimated_count_documents_with_read_concern() {
    let client = EventClient::new();
    let collection = client
        .database(function_name!())
        .collection(function_name!());
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
        .filter(|event| event.command_name == "estimated_document_count")
        .collect();
    assert_eq!(events.len(), 1);
    let read_concern = events[0].command.get("readConcern").unwrap();
    assert_eq!(read_concern, &Bson::Document(doc! {"level" : "local"}));
}

#[test]
#[named]
fn test_estimated_count_documents_without_read_concern() {
    let client = EventClient::new();
    let collection = client
        .database(function_name!())
        .collection(function_name!());
    collection.estimated_document_count(None).unwrap();
    let events: Vec<_> = client
        .events
        .write()
        .unwrap()
        .drain(..)
        .filter(|event| event.command_name == "estimated_document_count")
        .collect();
    assert_eq!(events.len(), 1);
    assert!(!events[0].command.contains_key("readConcern"));
}

#[test]
#[named]
fn test_aggregate_with_read_concern() {
    let client = EventClient::new();
    let collection = client
        .database(function_name!())
        .collection(function_name!());
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
#[named]
fn test_aggregate_without_read_concern() {
    let client = EventClient::new();
    let collection = client
        .database(function_name!())
        .collection(function_name!());
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
#[named]
fn test_distinct_with_read_concern() {
    let client = EventClient::new();
    let collection = client
        .database(function_name!())
        .collection(function_name!());
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
#[named]
fn test_distinct_without_read_concern() {
    let client = EventClient::new();
    let collection = client
        .database(function_name!())
        .collection(function_name!());
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
#[named]
fn test_find_with_read_concern() {
    let client = EventClient::new();
    let collection = client
        .database(function_name!())
        .collection(function_name!());
    collection.find(None, None).unwrap();
    let events: Vec<_> = client
        .events
        .write()
        .unwrap()
        .drain(..)
        .filter(|event| event.command_name == "find")
        .collect();
    assert_eq!(events.len(), 1);
    assert!(!events[0].command.contains_key("readConcern"));
}

#[test]
#[named]
fn test_find_without_read_concern() {
    let client = EventClient::new();
    let collection = client
        .database(function_name!())
        .collection(function_name!());
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
        .filter(|event| event.command_name == "find")
        .collect();
    assert_eq!(events.len(), 1);
    assert!(!events[0].command.contains_key("readConcern"));
}
