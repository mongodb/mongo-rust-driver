use std::time::Duration;

use futures_util::TryStreamExt;

use crate::{
    bson::{doc, oid::ObjectId, Document},
    error::{Error, Result},
    search_index::SearchIndexType,
    Client,
    Collection,
    SearchIndexModel,
};

async fn wait_for_match<T>(
    coll: &Collection<Document>,
    filter: impl Fn(Vec<Document>) -> Option<T>,
) -> Result<T> {
    const ITERATIONS_ALLOWED: usize = 60;
    let mut iterations = 0;
    loop {
        let result = async {
            coll.list_search_indexes()
                .await?
                .try_collect::<Vec<_>>()
                .await
        }
        .await;
        let found = match result {
            Ok(indexes) => filter(indexes),
            Err(e) if e.code() == Some(125) => None,
            Err(e) => return Err(e),
        };
        if let Some(found) = found {
            return Ok(found);
        } else if iterations > ITERATIONS_ALLOWED {
            return Err(Error::internal("timed out waiting for list search indexes"));
        } else {
            iterations += 1;
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
    }
}

async fn wait_for_search_index(
    coll: &Collection<Document>,
    filter: impl Fn(&Document) -> bool,
) -> Result<Document> {
    wait_for_match(coll, |indexes| indexes.into_iter().find(&filter)).await
}

async fn wait_for_search_indexes(
    coll: &Collection<Document>,
    condition: impl Fn(&Vec<Document>) -> bool,
) -> Result<Vec<Document>> {
    wait_for_match(coll, |indexes| condition(&indexes).then_some(indexes)).await
}

fn name_matches(index: &Document, name: &str) -> bool {
    index.get_str("name").is_ok_and(|n| n == name) && index.get_bool("queryable").unwrap_or(false)
}

/// Search Index Case 1: Driver can successfully create and list search indexes
#[tokio::test]
async fn search_index_create_list() {
    let client = Client::for_test().await;
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

    let index = wait_for_search_index(&coll0, |index| name_matches(index, &name))
        .await
        .unwrap();
    let dynamic = index["latestDefinition"]["mappings"]["dynamic"]
        .as_bool()
        .unwrap();
    assert!(!dynamic);
}

/// Search Index Case 2: Driver can successfully create multiple indexes in batch
#[tokio::test]
async fn search_index_create_multiple() {
    let client = Client::for_test().await;
    let db = client.database("search_index_test");
    let coll_name = ObjectId::new().to_hex();
    db.create_collection(&coll_name).await.unwrap();
    let coll0 = db.collection::<Document>(&coll_name);

    let names = ["test-search-index-1", "test-search-index-2"];

    let created_names = coll0
        .create_search_indexes([
            SearchIndexModel::builder()
                .name(names[0].to_string())
                .definition(doc! { "mappings": { "dynamic": false } })
                .build(),
            SearchIndexModel::builder()
                .name(names[1].to_string())
                .definition(doc! { "mappings": { "dynamic": false } })
                .build(),
        ])
        .await
        .unwrap();
    assert_eq!(created_names, names);

    let indexes = wait_for_search_indexes(&coll0, |indexes| {
        for name in names {
            if !indexes.iter().any(|index| name_matches(&index, name)) {
                return false;
            }
        }
        true
    })
    .await
    .unwrap();
    for index in indexes {
        let dynamic = index["latestDefinition"]["mappings"]["dynamic"]
            .as_bool()
            .unwrap();
        assert!(!dynamic);
    }
}

/// Search Index Case 3: Driver can successfully drop search indexes
#[tokio::test]
async fn search_index_drop() {
    let client = Client::for_test().await;
    let db = client.database("search_index_test");
    let coll_name = ObjectId::new().to_hex();
    db.create_collection(&coll_name).await.unwrap();
    let coll0 = db.collection::<Document>(&coll_name);

    let name = "test-search-index";

    let created_name = coll0
        .create_search_index(
            SearchIndexModel::builder()
                .name(name.to_string())
                .definition(doc! { "mappings": { "dynamic": false } })
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(created_name, name);

    let _ = wait_for_search_index(&coll0, |index| name_matches(index, name))
        .await
        .unwrap();

    coll0.drop_search_index("test-search-index").await.unwrap();

    let _ = wait_for_search_indexes(&coll0, Vec::is_empty)
        .await
        .unwrap();
}

/// Search Index Case 4: Driver can update a search index
#[tokio::test]
async fn search_index_update() {
    let client = Client::for_test().await;
    let db = client.database("search_index_test");
    let coll_name = ObjectId::new().to_hex();
    db.create_collection(&coll_name).await.unwrap();
    let coll0 = db.collection::<Document>(&coll_name);

    let name = "test-search-index";

    let created_name = coll0
        .create_search_index(
            SearchIndexModel::builder()
                .name(name.to_string())
                .definition(doc! { "mappings": { "dynamic": false } })
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(created_name, name);

    let _ = wait_for_search_index(&coll0, |index| name_matches(index, name))
        .await
        .unwrap();

    coll0
        .update_search_index(
            "test-search-index",
            doc! { "mappings": { "dynamic": true } },
        )
        .await
        .unwrap();

    let index = wait_for_search_index(&coll0, |index| {
        name_matches(index, name) && index.get_str("status").is_ok_and(|s| s == "READY")
    })
    .await
    .unwrap();
    let dynamic = index["latestDefinition"]["mappings"]["dynamic"]
        .as_bool()
        .unwrap();
    assert!(dynamic);
}

/// Search Index Case 5: dropSearchIndex suppresses namespace not found errors
#[tokio::test]
async fn search_index_drop_not_found() {
    let client = Client::for_test().await;
    let coll_name = ObjectId::new().to_hex();
    let coll0 = client
        .database("search_index_test")
        .collection::<Document>(&coll_name);

    coll0.drop_search_index("test-search-index").await.unwrap();
}

// SearchIndex Case 7: Driver can successfully handle search index types when creating indexes
#[tokio::test]
async fn search_index_create_with_type() {
    let client = Client::for_test().await;
    let coll_name = ObjectId::new().to_hex();
    let db = client.database("search_index_test");
    db.create_collection(&coll_name).await.unwrap();
    let coll0 = db.collection::<Document>(&coll_name);

    let name_implicit = "test-search-index-case7-implicit";
    let name_explicit = "test-search-index-case7-explicit";
    let name_vector = "test-search-index-case7-vector";

    let created_name = coll0
        .create_search_index(
            SearchIndexModel::builder()
                .name(name_implicit.to_string())
                .definition(doc! { "mappings": { "dynamic": false } })
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(created_name, name_implicit);

    let index = wait_for_search_index(&coll0, |index| name_matches(index, name_implicit))
        .await
        .unwrap();
    assert_eq!(index.get_str("type").unwrap(), "search");

    let created_name = coll0
        .create_search_index(
            SearchIndexModel::builder()
                .name(name_explicit.to_string())
                .index_type(SearchIndexType::Search)
                .definition(doc! { "mappings": { "dynamic": false } })
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(created_name, name_explicit);

    let index = wait_for_search_index(&coll0, |index| name_matches(index, name_explicit))
        .await
        .unwrap();
    assert_eq!(index.get_str("type").unwrap(), "search");

    let created_name = coll0
        .create_search_index(
            SearchIndexModel::builder()
                .name(name_vector.to_string())
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
    assert_eq!(created_name, name_vector);

    let index = wait_for_search_index(&coll0, |index| name_matches(index, name_vector))
        .await
        .unwrap();
    assert_eq!(index.get_str("type").unwrap(), "vectorSearch");
}

// SearchIndex Case 8: Driver requires explicit type to create a vector search index
#[tokio::test]
async fn search_index_requires_explicit_vector() {
    let client = Client::for_test().await;
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
        "invalid result: {result:?}"
    );
}
