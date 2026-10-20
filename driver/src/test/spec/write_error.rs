use crate::{
    bson::{doc, Bson, Document},
    error::{ErrorKind, WriteFailure},
    Client,
    Collection,
};

#[tokio::test]
async fn details() {
    let client = Client::for_test().monitor_events().await;

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
    let (_, event) = client.events.get_successful_command_execution("insert");
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

#[tokio::test]
async fn details_ext_json() {
    let client = Client::for_test().await;

    let db = client.database("db");
    db.create_collection("details_ext_json")
        .validator(doc! { "x": { "$type": "string" } })
        .await
        .unwrap();

    let coll = db.collection("details_ext_json");
    let error = coll
        .insert_one(doc! { "x": { "$numberLong": Bson::Null } })
        .await
        .unwrap_err();

    let ErrorKind::Write(WriteFailure::WriteError(write_error)) = *error.kind else {
        panic!("expected write error, got {error:?}");
    };
    let details = write_error.details.unwrap();
    let considered_value = details["details"]["consideredValue"].as_document().unwrap();
    assert_eq!(considered_value, &doc! { "$numberLong": Bson::Null });
}
