use std::cmp::Ord;

use approx::assert_ulps_eq;
use futures::stream::TryStreamExt;
use serde::Deserialize;
use tokio::sync::RwLockReadGuard;

use crate::{
    bson::{doc, Bson, Document},
    error::Result,
    options::{AggregateOptions, CreateCollectionOptions, IndexOptionDefaults},
    test::{
        util::{EventClient, TestClient},
        LOCK,
    },
    Database,
};

#[derive(Debug, Deserialize)]
struct CollectionInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub coll_type: String,
    pub options: Document,
    pub info: Info,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Info {
    pub read_only: bool,
    pub uuid: Bson,
}

#[derive(Deserialize)]
struct IsMasterReply {
    ismaster: bool,
    ok: f64,
}

async fn get_coll_info(db: &Database, filter: Option<Document>) -> Vec<CollectionInfo> {
    let mut colls: Vec<CollectionInfo> = db
        .list_collections(filter, None)
        .await
        .unwrap()
        .and_then(|doc| match bson::from_bson(Bson::Document(doc)) {
            Ok(info) => futures::future::ok(info),
            Err(e) => futures::future::err(e.into()),
        })
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
        assert_eq!(&coll.coll_type, "collection");
        assert!(!coll.info.read_only);

        match coll.info.uuid {
            Bson::Binary(..) => {}
            other => panic!("invalid BSON type for collection uuid: {:?}", other),
        }
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
        assert_eq!(&coll.coll_type, "collection");
        assert!(!coll.info.read_only);

        match coll.info.uuid {
            Bson::Binary(..) => {}
            other => panic!("invalid BSON type for collection uuid: {:?}", other),
        }
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

    let options = CreateCollectionOptions::builder()
        .capped(true)
        .size(512)
        .build();
    db.create_collection(&format!("{}{}", function_name!(), 2), Some(options))
        .await
        .unwrap();

    let colls = get_coll_info(&db, None).await;
    assert_eq!(colls.len(), 2);

    assert_eq!(colls[0].name, format!("{}1", function_name!()));
    assert_eq!(colls[0].coll_type, "collection");
    assert!(colls[0].options.is_empty());
    assert!(!colls[0].info.read_only);

    assert_eq!(colls[1].name, format!("{}2", function_name!()));
    assert_eq!(colls[1].coll_type, "collection");
    assert_eq!(colls[1].options.get("capped"), Some(&Bson::Boolean(true)));
    assert_eq!(colls[1].options.get("size"), Some(&Bson::Int32(512)));
    assert!(!colls[1].info.read_only);
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

    let options = CreateCollectionOptions::builder()
        .index_option_defaults(defaults.clone())
        .build();
    db.create_collection(name, options).await.unwrap();
    db.drop(None).await.unwrap();

    let events = client.get_command_started_events("create");
    assert_eq!(events.len(), 1);

    let event_defaults = match events[0].command.get_document("indexOptionDefaults") {
        Ok(defaults) => Some(IndexOptionDefaults {
            storage_engine: defaults.get_document("storageEngine").unwrap().clone(),
        }),
        Err(_) => None,
    };
    assert_eq!(event_defaults, defaults);
}
