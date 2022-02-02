use crate::{
    bson::{doc, Document},
    error::{ErrorKind, WriteFailure},
    options::CreateCollectionOptions,
    test::{EventClient, LOCK},
    Collection,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn details() {
    let _guard = LOCK.run_concurrently().await;
    let client = EventClient::new().await;

    if client.server_version_lt(5, 0) {
        // SERVER-58399
        println!("skipping write_error_details test due to server version");
        return;
    }

    let db = client.database("write_error_details");
    db.drop(None).await.unwrap();
    db.create_collection(
        "test",
        CreateCollectionOptions::builder()
            .validator(doc! {
                "x": { "$type": "string" }
            })
            .build(),
    )
    .await
    .unwrap();
    let coll: Collection<Document> = db.collection("test");
    let err = coll.insert_one(doc! { "x": 1 }, None).await.unwrap_err();
    let write_err = match *err.kind {
        ErrorKind::Write(WriteFailure::WriteError(e)) => e,
        _ => panic!("expected WriteError, got {:?}", err.kind),
    };
    let (_, event) = client.get_successful_command_execution("insert");
    assert_eq!(write_err.code, 121 /* DocumentValidationFailure */);
    assert_eq!(
        &write_err.details.unwrap(),
        event
            .reply
            .get_array("writeErrors")
            .unwrap()
            .get(0)
            .unwrap()
            .as_document()
            .unwrap()
            .get_document("errInfo")
            .unwrap()
    );
}
