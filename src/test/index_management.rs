use futures::stream::TryStreamExt;

use crate::{
    bson::doc,
    error::ErrorKind,
    options::{CommitQuorum, IndexOptions},
    test::log_uncaptured,
    Client,
    IndexModel,
};

// Test that creating indexes works as expected.
#[tokio::test]
#[function_name::named]
async fn index_management_creates() {
    let client = Client::for_test().await;
    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    // Test creating a single index with driver-generated name.
    let result = coll
        .create_index(IndexModel::builder().keys(doc! { "a": 1, "b": -1 }).build())
        .await
        .expect("Test failed to create index");

    assert_eq!(result.index_name, "a_1_b_-1".to_string());

    // Test creating several indexes, with both specified and unspecified names.
    let result = coll
        .create_indexes(vec![
            IndexModel::builder().keys(doc! { "c": 1 }).build(),
            IndexModel::builder()
                .keys(doc! { "d": 1 })
                .options(
                    IndexOptions::builder()
                        .name("customname".to_string())
                        .build(),
                )
                .build(),
        ])
        .await
        .expect("Test failed to create indexes");

    assert_eq!(
        result.index_names,
        vec!["c_1".to_string(), "customname".to_string()]
    );

    // Pull all index names from db to verify the _id_ index.
    let names = coll
        .list_index_names()
        .await
        .expect("Test failed to list index names");
    assert_eq!(names, vec!["_id_", "a_1_b_-1", "c_1", "customname"]);
}

// Test that creating a duplicate index works as expected.
#[tokio::test]
#[function_name::named]
async fn index_management_handles_duplicates() {
    let client = Client::for_test().await;
    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    let result = coll
        .create_index(IndexModel::builder().keys(doc! { "a": 1 }).build())
        .await
        .expect("Test failed to create index");

    assert_eq!(result.index_name, "a_1".to_string());

    // Insert duplicate.
    let result = coll
        .create_index(IndexModel::builder().keys(doc! { "a": 1 }).build())
        .await
        .expect("Test failed to create index");

    assert_eq!(result.index_name, "a_1".to_string());

    // Test partial duplication.
    let result = coll
        .create_indexes(vec![
            IndexModel::builder().keys(doc! { "a": 1 }).build(), // Duplicate
            IndexModel::builder().keys(doc! { "b": 1 }).build(), // Not duplicate
        ])
        .await
        .expect("Test failed to create indexes");

    assert_eq!(
        result.index_names,
        vec!["a_1".to_string(), "b_1".to_string()]
    );
}

// Test that listing indexes works as expected.
#[tokio::test]
#[function_name::named]
async fn index_management_lists() {
    let client = Client::for_test().await;
    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    let insert_data = vec![
        IndexModel::builder().keys(doc! { "a": 1 }).build(),
        IndexModel::builder().keys(doc! { "b": 1, "c": 1 }).build(),
        IndexModel::builder()
            .keys(doc! { "d": 1 })
            .options(IndexOptions::builder().unique(Some(true)).build())
            .build(),
    ];

    coll.create_indexes(insert_data.clone())
        .await
        .expect("Test failed to create indexes");

    let expected_names = vec![
        "_id_".to_string(),
        "a_1".to_string(),
        "b_1_c_1".to_string(),
        "d_1".to_string(),
    ];

    let mut indexes = coll
        .list_indexes()
        .await
        .expect("Test failed to list indexes");

    let id = indexes.try_next().await.unwrap().unwrap();
    assert_eq!(id.get_name().unwrap(), expected_names[0]);
    assert!(!id.is_unique());

    let a = indexes.try_next().await.unwrap().unwrap();
    assert_eq!(a.get_name().unwrap(), expected_names[1]);
    assert!(!a.is_unique());

    let b_c = indexes.try_next().await.unwrap().unwrap();
    assert_eq!(b_c.get_name().unwrap(), expected_names[2]);
    assert!(!b_c.is_unique());

    // Unique index.
    let d = indexes.try_next().await.unwrap().unwrap();
    assert_eq!(d.get_name().unwrap(), expected_names[3]);
    assert!(d.is_unique());

    assert!(indexes.try_next().await.unwrap().is_none());

    let names = coll
        .list_index_names()
        .await
        .expect("Test failed to list index names");

    assert_eq!(names, expected_names);
}

// Test that dropping indexes works as expected.
#[tokio::test]
#[function_name::named]
async fn index_management_drops() {
    let client = Client::for_test().await;
    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    let result = coll
        .create_indexes(vec![
            IndexModel::builder().keys(doc! { "a": 1 }).build(),
            IndexModel::builder().keys(doc! { "b": 1 }).build(),
            IndexModel::builder().keys(doc! { "c": 1 }).build(),
        ])
        .await
        .expect("Test failed to create multiple indexes");

    assert_eq!(
        result.index_names,
        vec!["a_1".to_string(), "b_1".to_string(), "c_1".to_string()]
    );

    // Test dropping single index.
    coll.drop_index("a_1")
        .await
        .expect("Test failed to drop index");
    let names = coll
        .list_index_names()
        .await
        .expect("Test failed to list index names");
    assert_eq!(names, vec!["_id_", "b_1", "c_1"]);

    // Test dropping several indexes.
    coll.drop_indexes()
        .await
        .expect("Test failed to drop indexes");
    let names = coll
        .list_index_names()
        .await
        .expect("Test failed to list index names");
    assert_eq!(names, vec!["_id_"]);
}

// Test that index management commands execute the expected database commands.
#[tokio::test]
#[function_name::named]
async fn index_management_executes_commands() {
    let client = Client::for_test().monitor_events().await;
    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    // Collection::create_index and Collection::create_indexes execute createIndexes.
    assert_eq!(
        client
            .events
            .get_command_started_events(&["createIndexes"])
            .len(),
        0
    );
    coll.create_index(IndexModel::builder().keys(doc! { "a": 1 }).build())
        .await
        .expect("Create Index op failed");
    assert_eq!(
        client
            .events
            .get_command_started_events(&["createIndexes"])
            .len(),
        1
    );
    coll.create_indexes(vec![
        IndexModel::builder().keys(doc! { "b": 1 }).build(),
        IndexModel::builder().keys(doc! { "c": 1 }).build(),
    ])
    .await
    .expect("Create Indexes op failed");
    assert_eq!(
        client
            .events
            .get_command_started_events(&["createIndexes"])
            .len(),
        2
    );

    // Collection::list_indexes and Collection::list_index_names execute listIndexes.
    assert_eq!(
        client
            .events
            .get_command_started_events(&["listIndexes"])
            .len(),
        0
    );
    coll.list_indexes().await.expect("List index op failed");
    assert_eq!(
        client
            .events
            .get_command_started_events(&["listIndexes"])
            .len(),
        1
    );
    coll.list_index_names().await.expect("List index op failed");
    assert_eq!(
        client
            .events
            .get_command_started_events(&["listIndexes"])
            .len(),
        2
    );

    // Collection::drop_index and Collection::drop_indexes execute dropIndexes.
    assert_eq!(
        client
            .events
            .get_command_started_events(&["dropIndexes"])
            .len(),
        0
    );
    coll.drop_index("a_1").await.expect("Drop index op failed");
    assert_eq!(
        client
            .events
            .get_command_started_events(&["dropIndexes"])
            .len(),
        1
    );
    coll.drop_indexes().await.expect("Drop indexes op failed");
    assert_eq!(
        client
            .events
            .get_command_started_events(&["dropIndexes"])
            .len(),
        2
    );
}

#[tokio::test]
#[function_name::named]
async fn commit_quorum_error() {
    let client = Client::for_test().await;
    if client.is_standalone() {
        log_uncaptured("skipping commit_quorum_error due to standalone topology");
        return;
    }

    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    let model = IndexModel::builder().keys(doc! { "x": 1 }).build();
    let result = coll
        .create_index(model)
        .commit_quorum(CommitQuorum::Majority)
        .await;

    if client.server_version_lt(4, 4) {
        let err = result.unwrap_err();
        assert!(matches!(*err.kind, ErrorKind::InvalidArgument { .. }));
    } else {
        assert!(result.is_ok());
    }
}
