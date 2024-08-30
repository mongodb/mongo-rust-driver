use bson::doc;
use futures::TryStreamExt;

use crate::{db::options::TimeseriesOptions, test::log_uncaptured, Client};

type Result<T> = anyhow::Result<T>;

#[tokio::test]
async fn list_collections_timeseries() -> Result<()> {
    let client = Client::test_builder().await;
    if client.server_version_lt(5, 0) {
        log_uncaptured("Skipping list_collections_timeseries: timeseries require server >= 5.0");
        return Ok(());
    }
    let db = client.database("list_collections_timeseries");
    db.drop().await?;
    db.create_collection("test")
        .timeseries(
            TimeseriesOptions::builder()
                .time_field("timestamp".to_string())
                .meta_field(None)
                .granularity(None)
                .build(),
        )
        .await?;
    let results: Vec<_> = db
        .list_collections()
        .filter(doc! { "name": "test" })
        .await?
        .try_collect()
        .await?;
    assert_eq!(results.len(), 1);

    Ok(())
}
