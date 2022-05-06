use bson::Document;

use super::TestClient;

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn auth_aws() {
    let client = TestClient::new().await;
    let coll = client.database("aws").collection::<Document>("somecoll");

    coll.find_one(None, None).await.unwrap();
}
