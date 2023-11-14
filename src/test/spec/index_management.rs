use std::time::{Instant, Duration};

use bson::{oid::ObjectId, Document, doc};
use futures_util::TryStreamExt;

use crate::{test::spec::unified_runner::run_unified_tests, Client, SearchIndexModel};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    run_unified_tests(&["index-management"]).await;
}

/// Search Index Case 1: Driver can successfully create and list search indexes
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn search_index_create_list() {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(60 * 5);

    let client = Client::test_builder().build().await;
    let db = client.database("search_index_test");
    let coll_name = ObjectId::new().to_hex();
    db.create_collection(&coll_name, None).await.unwrap();
    let coll0 = db.collection::<Document>(&coll_name);

    let name = coll0.create_search_index(
        SearchIndexModel::builder()
            .name(String::from("test-search-index"))
            .definition(doc! { "mappings": { "dynamic": false } })
            .build(),
        None,
    ).await.unwrap();
    assert_eq!(name, "test-search-index");

    let found = 'outer: loop {
        let mut cursor = coll0.list_search_indexes(None, None, None).await.unwrap();
        while let Some(d) = cursor.try_next().await.unwrap() {
            if d.get_str("name") == Ok("test-search-index") && d.get_bool("queryable") == Ok(true) {
                break 'outer d;
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        if Instant::now() > deadline {
            panic!("timed out");
        }
    };

    assert_eq!(found.get_document("latestDefinition"), Ok(&doc! { "mappings": { "dynamic": false } }));
}