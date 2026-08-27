use crate::{
    bson::{doc, Document},
    test::get_var,
    Client,
};

#[tokio::test]
async fn plain_auth() {
    let username = get_var("SASL_USER");
    let password = get_var("SASL_PASS");
    let host = get_var("SASL_HOST");
    let uri = format!(
        "mongodb://{username}:{password}@{host}/?authMechanism=PLAIN&authSource=%24external"
    );

    let client = Client::with_uri_str(uri).await.unwrap();
    let doc = client
        .database("ldap")
        .collection::<Document>("test")
        .find_one(doc! {})
        .await
        .unwrap()
        .unwrap();
    assert!(doc.get_bool("ldap").unwrap());
}
