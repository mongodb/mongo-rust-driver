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
        "mongodb://{username}:{password}@{host}/?authMechanism=PLAIN&authSource=%24external&\
         serverSelectionTimeoutMS=2000"
    );

    let client = Client::with_uri_str(uri).await.unwrap();
    client
        .database("db")
        .collection::<Document>("coll")
        .find_one(doc! { "x": 1 })
        .await
        .unwrap();
}
