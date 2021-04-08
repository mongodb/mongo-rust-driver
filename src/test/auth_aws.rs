use bson::Document;
use tokio::sync::RwLockReadGuard;

use super::{TestClient, LOCK};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn auth_aws() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    let coll = client.database("aws").collection::<Document>("somecoll");

    coll.find_one(None, None).await.unwrap();
}
