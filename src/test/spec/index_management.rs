use std::{
    env,
    time::{Duration, Instant},
};

use bson::{doc, oid::ObjectId, Document};
use futures_util::TryStreamExt;

use crate::{
    search_index::SearchIndexType,
    test::{log_uncaptured, spec::unified_runner::run_unified_tests},
    Client,
    Collection,
    SearchIndexModel,
};

#[tokio::test]
async fn run() {
    if env::var("INDEX_MANAGEMENT_TEST_UNIFIED").is_err() {
        log_uncaptured(
            "Skipping index management unified tests: INDEX_MANAGEMENT_TEST_UNIFIED not set",
        );
        return;
    }
    run_unified_tests(&["index-management"]).await;
}

/// Search Index Case 1: Driver can successfully create and list search indexes
#[tokio::test]
async fn search_index_create_list() {
    if env::var("INDEX_MANAGEMENT_TEST_PROSE").is_err() {
        log_uncaptured("Skipping index management prose test: INDEX_MANAGEMENT_TEST_PROSE not set");
        return;
    }
    let start = Instant::now();
    let deadline = start + Duration::from_secs(60 * 5);

    let client = Client::test_builder().build().await;
    let db = client.database("search_index_test");
    let coll_name = ObjectId::new().to_hex();
    db.create_collection(&coll_name).await.unwrap();
    let coll0 = db.collection::<Document>(&coll_name);

    let name = coll0
        .create_search_index(
            SearchIndexModel::builder()
                .name(String::from("test-search-index"))
                .definition(doc! { "mappings": { "dynamic": false } })
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(name, "test-search-index");

    let found = 'outer: loop {
        let mut cursor = coll0.list_search_indexes().await.unwrap();
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

    assert_eq!(
        found.get_document("latestDefinition"),
        Ok(&doc! { "mappings": { "dynamic": false } })
    );
}

/// Search Index Case 2: Driver can successfully create multiple indexes in batch
#[tokio::test]
async fn search_index_create_multiple() {
    if env::var("INDEX_MANAGEMENT_TEST_PROSE").is_err() {
        log_uncaptured("Skipping index management prose test: INDEX_MANAGEMENT_TEST_PROSE not set");
        return;
    }
    let start = Instant::now();
    let deadline = start + Duration::from_secs(60 * 5);

    let client = Client::test_builder().build().await;
    let db = client.database("search_index_test");
    let coll_name = ObjectId::new().to_hex();
    db.create_collection(&coll_name).await.unwrap();
    let coll0 = db.collection::<Document>(&coll_name);

    let names = coll0
        .create_search_indexes([
            SearchIndexModel::builder()
                .name(String::from("test-search-index-1"))
                .definition(doc! { "mappings": { "dynamic": false } })
                .build(),
            SearchIndexModel::builder()
                .name(String::from("test-search-index-2"))
                .definition(doc! { "mappings": { "dynamic": false } })
                .build(),
        ])
        .await
        .unwrap();
    assert_eq!(names, ["test-search-index-1", "test-search-index-2"]);

    let mut index1 = None;
    let mut index2 = None;
    loop {
        let mut cursor = coll0.list_search_indexes().await.unwrap();
        while let Some(d) = cursor.try_next().await.unwrap() {
            if d.get_str("name") == Ok("test-search-index-1") && d.get_bool("queryable") == Ok(true)
            {
                index1 = Some(d);
            } else if d.get_str("name") == Ok("test-search-index-2")
                && d.get_bool("queryable") == Ok(true)
            {
                index2 = Some(d);
            }
        }
        if index1.is_some() && index2.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        if Instant::now() > deadline {
            panic!("timed out");
        }
    }

    assert_eq!(
        index1.unwrap().get_document("latestDefinition"),
        Ok(&doc! { "mappings": { "dynamic": false } })
    );
    assert_eq!(
        index2.unwrap().get_document("latestDefinition"),
        Ok(&doc! { "mappings": { "dynamic": false } })
    );
}

/// Search Index Case 3: Driver can successfully drop search indexes
#[tokio::test]
async fn search_index_drop() {
    if env::var("INDEX_MANAGEMENT_TEST_PROSE").is_err() {
        log_uncaptured("Skipping index management prose test: INDEX_MANAGEMENT_TEST_PROSE not set");
        return;
    }
    let start = Instant::now();
    let deadline = start + Duration::from_secs(60 * 5);

    let client = Client::test_builder().build().await;
    let db = client.database("search_index_test");
    let coll_name = ObjectId::new().to_hex();
    db.create_collection(&coll_name).await.unwrap();
    let coll0 = db.collection::<Document>(&coll_name);

    let name = coll0
        .create_search_index(
            SearchIndexModel::builder()
                .name(String::from("test-search-index"))
                .definition(doc! { "mappings": { "dynamic": false } })
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(name, "test-search-index");

    'outer: loop {
        let mut cursor = coll0.list_search_indexes().await.unwrap();
        while let Some(d) = cursor.try_next().await.unwrap() {
            if d.get_str("name") == Ok("test-search-index") && d.get_bool("queryable") == Ok(true) {
                break 'outer;
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        if Instant::now() > deadline {
            panic!("search index creation timed out");
        }
    }

    coll0.drop_search_index("test-search-index").await.unwrap();

    loop {
        let cursor = coll0.list_search_indexes().await.unwrap();
        if !cursor.has_next() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        if Instant::now() > deadline {
            panic!("search index drop timed out");
        }
    }
}

/// Search Index Case 4: Driver can update a search index
#[tokio::test]
async fn search_index_update() {
    if env::var("INDEX_MANAGEMENT_TEST_PROSE").is_err() {
        log_uncaptured("Skipping index management prose test: INDEX_MANAGEMENT_TEST_PROSE not set");
        return;
    }
    let start = Instant::now();
    let deadline = start + Duration::from_secs(60 * 5);

    let client = Client::test_builder().build().await;
    let db = client.database("search_index_test");
    let coll_name = ObjectId::new().to_hex();
    db.create_collection(&coll_name).await.unwrap();
    let coll0 = db.collection::<Document>(&coll_name);

    let name = coll0
        .create_search_index(
            SearchIndexModel::builder()
                .name(String::from("test-search-index"))
                .definition(doc! { "mappings": { "dynamic": false } })
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(name, "test-search-index");

    'outer: loop {
        let mut cursor = coll0.list_search_indexes().await.unwrap();
        while let Some(d) = cursor.try_next().await.unwrap() {
            if d.get_str("name") == Ok("test-search-index") && d.get_bool("queryable") == Ok(true) {
                break 'outer;
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        if Instant::now() > deadline {
            panic!("search index creation timed out");
        }
    }

    coll0
        .update_search_index(
            "test-search-index",
            doc! { "mappings": { "dynamic": true } },
        )
        .await
        .unwrap();

    let found = 'find: loop {
        let mut cursor = coll0.list_search_indexes().await.unwrap();
        while let Some(d) = cursor.try_next().await.unwrap() {
            if d.get_str("name") == Ok("test-search-index")
                && d.get_bool("queryable") == Ok(true)
                && d.get_str("status") == Ok("READY")
            {
                break 'find d;
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        if Instant::now() > deadline {
            panic!("search index update timed out");
        }
    };

    assert_eq!(
        found.get_document("latestDefinition"),
        Ok(&doc! { "mappings": { "dynamic": true } })
    );
}

/// Search Index Case 5: dropSearchIndex suppresses namespace not found errors
#[tokio::test]
async fn search_index_drop_not_found() {
    if env::var("INDEX_MANAGEMENT_TEST_PROSE").is_err() {
        log_uncaptured("Skipping index management prose test: INDEX_MANAGEMENT_TEST_PROSE not set");
        return;
    }
    let client = Client::test_builder().build().await;
    let coll_name = ObjectId::new().to_hex();
    let coll0 = client
        .database("search_index_test")
        .collection::<Document>(&coll_name);

    coll0.drop_search_index("test-search-index").await.unwrap();
}

async fn wait_for_index(coll: &Collection<Document>, name: &str) -> Document {
    let deadline = Instant::now() + Duration::from_secs(60 * 5);
    while Instant::now() < deadline {
        let mut cursor = coll.list_search_indexes().name(name).await.unwrap();
        while let Some(def) = cursor.try_next().await.unwrap() {
            if def.get_str("name") == Ok(name) && def.get_bool("queryable") == Ok(true) {
                return def;
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    panic!("search index creation timed out");
}

// SearchIndex Case 7: Driver can successfully handle search index types when creating indexes
#[tokio::test]
async fn search_index_create_with_type() {
    if env::var("INDEX_MANAGEMENT_TEST_PROSE").is_err() {
        log_uncaptured("Skipping index management prose test: INDEX_MANAGEMENT_TEST_PROSE not set");
        return;
    }
    let client = Client::test_builder().build().await;
    let coll_name = ObjectId::new().to_hex();
    let db = client.database("search_index_test");
    db.create_collection(&coll_name).await.unwrap();
    let coll0 = db.collection::<Document>(&coll_name);

    let name = coll0
        .create_search_index(
            SearchIndexModel::builder()
                .name(String::from("test-search-index-case7-implicit"))
                .definition(doc! { "mappings": { "dynamic": false } })
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(name, "test-search-index-case7-implicit");
    let index1 = wait_for_index(&coll0, &name).await;
    assert_eq!(index1.get_str("type"), Ok("search"));

    let name = coll0
        .create_search_index(
            SearchIndexModel::builder()
                .name(String::from("test-search-index-case7-explicit"))
                .index_type(SearchIndexType::Search)
                .definition(doc! { "mappings": { "dynamic": false } })
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(name, "test-search-index-case7-explicit");
    let index2 = wait_for_index(&coll0, &name).await;
    assert_eq!(index2.get_str("type"), Ok("search"));

    let name = coll0
        .create_search_index(
            SearchIndexModel::builder()
                .name(String::from("test-search-index-case7-vector"))
                .index_type(SearchIndexType::VectorSearch)
                .definition(doc! {
                    "fields": [{
                        "type": "vector",
                        "path": "plot_embedding",
                        "numDimensions": 1536,
                        "similarity": "euclidean",
                    }]
                })
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(name, "test-search-index-case7-vector");
    let index3 = wait_for_index(&coll0, &name).await;
    assert_eq!(index3.get_str("type"), Ok("vectorSearch"));
}

// SearchIndex Case 8: Driver requires explicit type to create a vector search index
#[tokio::test]
async fn search_index_requires_explicit_vector() {
    if env::var("INDEX_MANAGEMENT_TEST_PROSE").is_err() {
        log_uncaptured("Skipping index management prose test: INDEX_MANAGEMENT_TEST_PROSE not set");
        return;
    }
    let client = Client::test_builder().build().await;
    let coll_name = ObjectId::new().to_hex();
    let db = client.database("search_index_test");
    db.create_collection(&coll_name).await.unwrap();
    let coll0 = db.collection::<Document>(&coll_name);

    let result = coll0
        .create_search_index(
            SearchIndexModel::builder()
                .name(String::from("test-search-index-case8-error"))
                .definition(doc! {
                    "fields": [{
                        "type": "vector",
                        "path": "plot_embedding",
                        "numDimensions": 1536,
                        "similarity": "euclidean",
                    }]
                })
                .build(),
        )
        .await;
    assert!(
        result
            .as_ref()
            .is_err_and(|e| e.to_string().contains("Attribute mappings missing")),
        "invalid result: {:?}",
        result
    );
}
