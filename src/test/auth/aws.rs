use crate::{
    bson::{doc, Document},
    Client,
};

#[tokio::test]
async fn auth_aws() {
    let client = Client::for_test().await;
    let coll = client.database("aws").collection::<Document>("somecoll");

    coll.find_one(doc! {}).await.unwrap();
}
