use std::cmp::Ord;

use approx::assert_ulps_eq;
use futures::stream::TryStreamExt;
use serde::Deserialize;
use tokio::sync::RwLockReadGuard;

use crate::{
    bson::{doc, Bson, Document},
    error::Result,
    options::{
        AggregateOptions,
        Collation,
        CreateCollectionOptions,
        IndexOptionDefaults,
        ValidationAction,
        ValidationLevel,
    },
    results::{CollectionSpecification, CollectionType},
    test::{
        util::{EventClient, TestClient},
        LOCK,
    },
    Database,
};

#[derive(Deserialize)]
struct IsMasterReply {
    ismaster: bool,
    ok: f64,
}

async fn get_coll_info(db: &Database, filter: Option<Document>) -> Vec<CollectionSpecification> {
    let mut colls: Vec<CollectionSpecification> = db
        .list_collections(filter, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    colls.sort_by(|c1, c2| c1.name.cmp(&c2.name));

    colls
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn is_master() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    let db = client.database("test");
    let doc = db.run_command(doc! { "ismaster": 1 }, None).await.unwrap();
    let is_master_reply: IsMasterReply = bson::from_bson(Bson::Document(doc)).unwrap();

    assert!(is_master_reply.ismaster);
    assert_ulps_eq!(is_master_reply.ok, 1.0);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn list_collections() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    let db = client.database(function_name!());
    db.drop(None).await.unwrap();

    let colls: Result<Vec<_>> = db
        .list_collections(None, None)
        .await
        .unwrap()
        .try_collect()
        .await;
    assert_eq!(colls.unwrap().len(), 0);

    let coll_names = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for coll_name in coll_names {
        db.collection(coll_name)
            .insert_one(doc! { "x": 1 }, None)
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn list_collections_filter() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    let db = client.database(function_name!());
    db.drop(None).await.unwrap();

    let colls: Result<Vec<_>> = db
        .list_collections(None, None)
        .await
        .unwrap()
        .try_collect()
        .await;
    assert_eq!(colls.unwrap().len(), 0);

    let coll_names = &["bar", "baz", "foo"];
    for coll_name in coll_names {
        db.collection(coll_name)
            .insert_one(doc! { "x": 1 }, None)
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn list_collection_names() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    let db = client.database(function_name!());
    db.drop(None).await.unwrap();

    assert!(db.list_collection_names(None).await.unwrap().is_empty());

    let expected_colls = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for coll in expected_colls {
        db.collection(coll)
            .insert_one(doc! { "x": 1 }, None)
            .await
            .unwrap();
    }

    let mut actual_colls = db.list_collection_names(None).await.unwrap();
    actual_colls.sort();

    assert_eq!(&actual_colls, expected_colls);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn collection_management() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    let db = client.database(function_name!());
    db.drop(None).await.unwrap();

    assert!(db.list_collection_names(None).await.unwrap().is_empty());

    db.create_collection(&format!("{}{}", function_name!(), 1), None)
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

    db.create_collection(&format!("{}{}", function_name!(), 2), options.clone())
        .await
        .unwrap();

    let view_options = CreateCollectionOptions::builder()
        .view_on(format!("{}{}", function_name!(), 2))
        .pipeline(vec![doc! { "$match": {} }])
        .build();
    db.create_collection(&format!("{}{}", function_name!(), 3), view_options.clone())
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn db_aggregate() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;

    if client.server_version_lt(4, 0) {
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

    db.aggregate(pipeline, None)
        .await
        .expect("aggregate should succeed");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn db_aggregate_disk_use() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;

    if client.server_version_lt(4, 0) {
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

    let options = AggregateOptions::builder().allow_disk_use(true).build();

    db.aggregate(pipeline, Some(options))
        .await
        .expect("aggregate with disk use should succeed");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn create_index_options_defaults() {
    let defaults = IndexOptionDefaults {
        storage_engine: doc! { "wiredTiger": doc! {} },
    };
    index_option_defaults_test(Some(defaults), function_name!()).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn create_index_options_defaults_not_specified() {
    index_option_defaults_test(None, function_name!()).await;
}

async fn index_option_defaults_test(defaults: Option<IndexOptionDefaults>, name: &str) {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    let db = client.database(name);

    let mut options = CreateCollectionOptions::builder().build();
    options.index_option_defaults = defaults.clone();
    db.create_collection(name, options).await.unwrap();
    db.drop(None).await.unwrap();

    let events = client.get_command_started_events(&["create"]);
    assert_eq!(events.len(), 1);

    let event_defaults = match events[0].command.get_document("indexOptionDefaults") {
        Ok(defaults) => Some(IndexOptionDefaults {
            storage_engine: defaults.get_document("storageEngine").unwrap().clone(),
        }),
        Err(_) => None,
    };
    assert_eq!(event_defaults, defaults);
}
