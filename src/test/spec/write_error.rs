use crate::{
    bson::{doc, Document},
    error::{ErrorKind, WriteFailure},
    test::{log_uncaptured, EventClient},
    Collection,
};

#[tokio::test]
async fn details() {
    let client = EventClient::new().await;

    if client.server_version_lt(5, 0) {
        // SERVER-58399
        log_uncaptured("skipping write_error_details test due to server version");
        return;
    }

    let db = client.database("write_error_details");
    db.drop().await.unwrap();
    db.create_collection("test")
        .validator(doc! {
            "x": { "$type": "string" }
        })
        .await
        .unwrap();
    let coll: Collection<Document> = db.collection("test");
    let err = coll.insert_one(doc! { "x": 1 }).await.unwrap_err();
    let write_err = match *err.kind {
        ErrorKind::Write(WriteFailure::WriteError(e)) => e,
        _ => panic!("expected WriteError, got {:?}", err.kind),
    };
    let mut events = client.events.clone();
    let (_, event) = events.get_successful_command_execution("insert");
    assert_eq!(write_err.code, 121 /* DocumentValidationFailure */);
    assert_eq!(
        &write_err.details.unwrap(),
        event
            .reply
            .get_array("writeErrors")
            .unwrap()
            .first()
            .unwrap()
            .as_document()
            .unwrap()
            .get_document("errInfo")
            .unwrap()
    );
}
