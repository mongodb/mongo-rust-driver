use bson::{doc, Bson};

use crate::hello::HelloCommandResponse;

#[test]
fn parse_connection_id() {
    let mut parsed: HelloCommandResponse = bson::from_document(doc! {
        "connectionId": Bson::Int32(42),
        "maxBsonObjectSize": 0,
        "maxMessageSizeBytes": 0,
    })
    .unwrap();
    assert_eq!(parsed.connection_id, Some(42));

    parsed = bson::from_document(doc! {
        "connectionId": Bson::Int64(13),
        "maxBsonObjectSize": 0,
        "maxMessageSizeBytes": 0,
    })
    .unwrap();
    assert_eq!(parsed.connection_id, Some(13));

    parsed = bson::from_document(doc! {
        "connectionId": Bson::Double(1066.0),
        "maxBsonObjectSize": 0,
        "maxMessageSizeBytes": 0,
    })
    .unwrap();
    assert_eq!(parsed.connection_id, Some(1066));

    parsed = bson::from_document(doc! {
        "maxBsonObjectSize": 0,
        "maxMessageSizeBytes": 0,
    })
    .unwrap();
    assert_eq!(parsed.connection_id, None);
}
