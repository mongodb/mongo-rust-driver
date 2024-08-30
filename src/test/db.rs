use std::cmp::Ord;

use futures::stream::TryStreamExt;
use serde::Deserialize;

use crate::{
    action::Action,
    bson::{doc, Document},
    error::Result,
    options::{
        ClusteredIndex,
        Collation,
        CreateCollectionOptions,
        IndexOptionDefaults,
        ValidationAction,
        ValidationLevel,
    },
    results::{CollectionSpecification, CollectionType},
    Client,
    Cursor,
    Database,
};

use super::log_uncaptured;

async fn get_coll_info(db: &Database, filter: Option<Document>) -> Vec<CollectionSpecification> {
    let mut colls: Vec<CollectionSpecification> = db
        .list_collections()
        .optional(filter, |b, f| b.filter(f))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    colls.sort_by(|c1, c2| c1.name.cmp(&c2.name));

    colls
}

#[tokio::test]
#[function_name::named]
async fn list_collections() {
    let client = Client::for_test().await;
    let db = client.database(function_name!());
    db.drop().await.unwrap();

    let colls: Result<Vec<_>> = db.list_collections().await.unwrap().try_collect().await;
    assert_eq!(colls.unwrap().len(), 0);

    let coll_names = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for coll_name in coll_names {
        db.collection(coll_name)
            .insert_one(doc! { "x": 1 })
            .await
            .unwrap();
    }

    let colls = get_coll_info(&db, None).await;
    assert_eq!(colls.len(), coll_names.len());

    for (i, coll) in colls.into_iter().enumerate() {
        assert_eq!(&coll.name, &coll_names[i]);
        assert_eq!(&coll.collection_type, &CollectionType::Collection);
        assert!(!coll.info.read_only);
    }
}

#[tokio::test]
#[function_name::named]
async fn list_collections_filter() {
    let client = Client::for_test().await;
    let db = client.database(function_name!());
    db.drop().await.unwrap();

    let colls: Result<Vec<_>> = db.list_collections().await.unwrap().try_collect().await;
    assert_eq!(colls.unwrap().len(), 0);

    let coll_names = &["bar", "baz", "foo"];
    for coll_name in coll_names {
        db.collection(coll_name)
            .insert_one(doc! { "x": 1 })
            .await
            .unwrap();
    }

    let filter = doc! {
        "name": {
            "$lt": "c"
        }
    };
    let coll_names = &coll_names[..coll_names.len() - 1];

    let colls = get_coll_info(&db, Some(filter)).await;
    assert_eq!(colls.len(), coll_names.len());

    for (i, coll) in colls.into_iter().enumerate() {
        assert_eq!(&coll.name, &coll_names[i]);
        assert_eq!(&coll.collection_type, &CollectionType::Collection);
        assert!(!coll.info.read_only);
    }
}

#[tokio::test]
#[function_name::named]
async fn list_collection_names() {
    let client = Client::for_test().await;
    let db = client.database(function_name!());
    db.drop().await.unwrap();

    assert!(db.list_collection_names().await.unwrap().is_empty());

    let expected_colls = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for coll in expected_colls {
        db.collection(coll)
            .insert_one(doc! { "x": 1 })
            .await
            .unwrap();
    }

    let mut actual_colls = db.list_collection_names().await.unwrap();
    actual_colls.sort();

    assert_eq!(&actual_colls, expected_colls);
}

#[tokio::test]
#[function_name::named]
async fn collection_management() {
    let client = Client::for_test().await;
    let db = client.database(function_name!());
    db.drop().await.unwrap();

    assert!(db.list_collection_names().await.unwrap().is_empty());

    db.create_collection(&format!("{}{}", function_name!(), 1))
        .await
        .unwrap();

    let collation = Collation::builder().locale("en_US".to_string()).build();
    let options = CreateCollectionOptions::builder()
        .capped(true)
        .size(512)
        .max(50)
        .storage_engine(doc! { "wiredTiger": {} })
        .validator(doc! { "x" : { "$gt": 0 } })
        .validation_level(ValidationLevel::Moderate)
        .validation_action(ValidationAction::Error)
        .collation(collation.clone())
        .index_option_defaults(
            IndexOptionDefaults::builder()
                .storage_engine(doc! { "wiredTiger": {} })
                .build(),
        )
        .build();

    db.create_collection(&format!("{}{}", function_name!(), 2))
        .with_options(options.clone())
        .await
        .unwrap();

    let view_options = CreateCollectionOptions::builder()
        .view_on(format!("{}{}", function_name!(), 2))
        .pipeline(vec![doc! { "$match": {} }])
        .build();
    db.create_collection(&format!("{}{}", function_name!(), 3))
        .with_options(view_options.clone())
        .await
        .unwrap();

    let mut colls = get_coll_info(
        &db,
        Some(doc! { "name": { "$regex": "^collection_management" } }),
    )
    .await;
    assert_eq!(colls.len(), 3);

    assert_eq!(colls[0].name, format!("{}1", function_name!()));
    assert_eq!(colls[0].collection_type, CollectionType::Collection);
    assert_eq!(
        bson::to_document(&colls[0].options).expect("serialization should succeed"),
        doc! {}
    );
    assert!(!colls[0].info.read_only);

    let coll2 = colls.remove(1);
    assert_eq!(coll2.name, format!("{}2", function_name!()));
    assert_eq!(coll2.collection_type, CollectionType::Collection);
    assert_eq!(coll2.options.capped, options.capped);
    assert_eq!(coll2.options.size, options.size);
    assert_eq!(coll2.options.validator, options.validator);
    assert_eq!(coll2.options.validation_action, options.validation_action);
    assert_eq!(coll2.options.validation_level, options.validation_level);
    assert_eq!(coll2.options.collation.unwrap().locale, collation.locale);
    assert_eq!(coll2.options.storage_engine, options.storage_engine);
    assert_eq!(
        coll2.options.index_option_defaults,
        options.index_option_defaults
    );
    assert!(!coll2.info.read_only);
    assert!(coll2.id_index.is_some());

    let coll3 = colls.remove(1);
    assert_eq!(coll3.name, format!("{}3", function_name!()));
    assert_eq!(coll3.collection_type, CollectionType::View);
    assert_eq!(coll3.options.view_on, view_options.view_on);
    assert_eq!(coll3.options.pipeline, view_options.pipeline);
    assert!(coll3.info.read_only);
    assert!(coll3.id_index.is_none());
}

#[tokio::test]
async fn db_aggregate() {
    let client = Client::for_test().await;

    if client.server_version_lt(4, 0) {
        log_uncaptured("skipping db_aggregate due to server version < 4.0");
        return;
    }

    let db = client.database("admin");

    let pipeline = vec![
        doc! {
          "$currentOp": {
            "allUsers": false,
            "idleConnections": false
          }
        },
        doc! {
          "$match": {
            "command.aggregate": {
              "$eq": 1
            }
          }
        },
        doc! {
          "$project": {
            "command": 1
          }
        },
        doc! {
          "$project": {
            "command.lsid": 0
          }
        },
    ];

    db.aggregate(pipeline)
        .await
        .expect("aggregate should succeed");
}

#[tokio::test]
async fn db_aggregate_disk_use() {
    let client = Client::for_test().await;

    if client.server_version_lt(4, 0) {
        log_uncaptured("skipping db_aggregate_disk_use due to server version < 4.0");
        return;
    }

    let db = client.database("admin");

    let pipeline = vec![
        doc! {
          "$currentOp": {
            "allUsers": true,
            "idleConnections": true
          }
        },
        doc! {
          "$match": {
            "command.aggregate": {
              "$eq": 1
            }
          }
        },
        doc! {
          "$project": {
            "command": 1
          }
        },
        doc! {
          "$project": {
            "command.lsid": 0
          }
        },
    ];

    db.aggregate(pipeline)
        .allow_disk_use(true)
        .await
        .expect("aggregate with disk use should succeed");
}

#[tokio::test]
#[function_name::named]
async fn create_index_options_defaults() {
    let defaults = IndexOptionDefaults {
        storage_engine: doc! { "wiredTiger": doc! {} },
    };
    index_option_defaults_test(Some(defaults), function_name!()).await;
}

#[tokio::test]
#[function_name::named]
async fn create_index_options_defaults_not_specified() {
    index_option_defaults_test(None, function_name!()).await;
}

async fn index_option_defaults_test(defaults: Option<IndexOptionDefaults>, name: &str) {
    let client = Client::for_test().monitor_events().await;
    let db = client.database(name);

    db.create_collection(name)
        .optional(defaults.clone(), |b, d| b.index_option_defaults(d))
        .await
        .unwrap();
    db.drop().await.unwrap();

    let events = client.events.get_command_started_events(&["create"]);
    assert_eq!(events.len(), 1);

    let event_defaults = match events[0].command.get_document("indexOptionDefaults") {
        Ok(defaults) => Some(IndexOptionDefaults {
            storage_engine: defaults.get_document("storageEngine").unwrap().clone(),
        }),
        Err(_) => None,
    };
    assert_eq!(event_defaults, defaults);
}

#[test]
fn deserialize_clustered_index_option_from_bool() {
    let options_doc = doc! { "clusteredIndex": true };
    let options: CreateCollectionOptions = bson::from_document(options_doc).unwrap();
    let clustered_index = options
        .clustered_index
        .expect("deserialized options should include clustered_index");
    assert_eq!(clustered_index, ClusteredIndex::default());
}

#[tokio::test]
async fn clustered_index_list_collections() {
    let client = Client::for_test().await;
    let database = client.database("db");

    if client.server_version_lt(5, 3) {
        return;
    }

    database
        .create_collection("clustered_index_collection")
        .clustered_index(ClusteredIndex::default())
        .await
        .unwrap();

    let collections = database
        .list_collections()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let clustered_index_collection = collections
        .iter()
        .find(|specification| specification.name == "clustered_index_collection")
        .unwrap();
    assert!(clustered_index_collection.options.clustered_index.is_some());
}

#[tokio::test]
async fn aggregate_with_generics() {
    #[derive(Deserialize)]
    struct A {
        str: String,
    }

    let client = Client::for_test().await;
    let database = client.database("aggregate_with_generics");

    if client.server_version_lt(5, 1) {
        log_uncaptured(
            "skipping aggregate_with_generics: $documents agg stage only available on 5.1+",
        );
        return;
    }

    // The cursor returned will contain these documents
    let pipeline = vec![doc! { "$documents": [ { "str": "hi" } ] }];

    // Assert at compile-time that the default cursor returned is a Cursor<Document>
    let _: Cursor<Document> = database.aggregate(pipeline.clone()).await.unwrap();

    // Assert that data is properly deserialized when using with_type
    let mut cursor = database
        .aggregate(pipeline.clone())
        .with_type::<A>()
        .await
        .unwrap();
    assert!(cursor.advance().await.unwrap());
    assert_eq!(&cursor.deserialize_current().unwrap().str, "hi");

    // Assert that `with_type` can be used with an explicit session.
    let mut session = client.start_session().await.unwrap();
    let _ = database
        .aggregate(pipeline.clone())
        .session(&mut session)
        .with_type::<A>()
        .await
        .unwrap();
    let _ = database
        .aggregate(pipeline.clone())
        .with_type::<A>()
        .session(&mut session)
        .await
        .unwrap();
}
