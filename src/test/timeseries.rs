use bson::doc;
use futures::TryStreamExt;

use crate::{
    db::options::{CreateCollectionOptions, TimeseriesOptions},
    test::log_uncaptured,
    Client,
};

use super::LOCK;

type Result<T> = anyhow::Result<T>;

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn list_collections_timeseries() -> Result<()> {
    let _guard = LOCK.run_exclusively();

    let client = Client::test_builder().build().await;
    if client.server_version_lt(5, 0) {
        log_uncaptured("Skipping list_collections_timeseries: timeseries require server >= 5.0");
        return Ok(());
    }
    let db = client.database("list_collections_timeseries");
    db.drop(None).await?;
    db.create_collection(
        "test",
        CreateCollectionOptions::builder()
            .timeseries(
                TimeseriesOptions::builder()
                    .time_field("timestamp".to_string())
                    .meta_field(None)
                    .granularity(None)
                    .build(),
            )
            .build(),
    )
    .await?;
    let results: Vec<_> = db
        .list_collections(doc! { "name": "test" }, None)
        .await?
        .try_collect()
        .await?;
    assert_eq!(results.len(), 1);

    Ok(())
}
