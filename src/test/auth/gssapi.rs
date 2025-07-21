use crate::{
    bson::{doc, Document},
    Client,
};

#[tokio::test]
async fn auth_gssapi() {
    let host = std::env::var("SASL_HOST").expect("SASL_HOST not set");
    let user_principal = std::env::var("PRINCIPAL")
        .expect("PRINCIPAL not set")
        .replace("@", "%40");
    let gssapi_db = std::env::var("GSSAPI_DB").expect("GSSAPI_DB not set");

    let uri =
        format!("mongodb://{user_principal}@{host}/?authSource=%24external&authMechanism=GSSAPI");
    let client = Client::with_uri_str(uri)
        .await
        .expect("failed to create MongoDB Client");

    let coll = client.database(&gssapi_db).collection::<Document>("test");
    let doc = coll.find_one(doc! {}).await;
    match doc {
        Ok(Some(doc)) => {
            assert!(
                doc.get_bool("kerberos").unwrap(),
                "expected 'kerberos' field to exist and be 'true'"
            );
            assert_eq!(
                doc.get_str("authenticated").unwrap(),
                "yeah",
                "unexpected 'authenticated' value"
            );
        }
        Ok(None) => panic!("expected `find_one` to return a document, but it did not"),
        Err(e) => panic!("expected `find_one` to return a document, but it failed: {e:?}"),
    }
}
