use std::cmp::Ord;

use approx::assert_ulps_eq;
use bson::{bson, doc, Bson, Document};
use serde::Deserialize;

use crate::{
    options::{AggregateOptions, CreateCollectionOptions},
    test::{CLIENT, LOCK},
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

fn get_coll_info(db: &Database, filter: Option<Document>) -> Vec<CollectionInfo> {
    let colls: Result<Vec<Document>, _> = db.list_collections(filter, None).unwrap().collect();
    let mut colls: Vec<CollectionInfo> = colls
        .unwrap()
        .into_iter()
        .map(|doc| bson::from_bson(Bson::Document(doc)).unwrap())
        .collect();
    colls.sort_by(|c1, c2| c1.name.cmp(&c2.name));

    colls
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn is_master() {
    let _guard = LOCK.run_concurrently();

    let db = CLIENT.database("test");
    let doc = db.run_command(doc! { "ismaster": 1 }, None).unwrap();
    let is_master_reply: IsMasterReply = bson::from_bson(Bson::Document(doc)).unwrap();

    assert!(is_master_reply.ismaster);
    assert_ulps_eq!(is_master_reply.ok, 1.0);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn list_collections() {
    let _guard = LOCK.run_concurrently();

    let db = CLIENT.database(function_name!());
    db.drop(None).unwrap();

    assert_eq!(db.list_collections(None, None).unwrap().count(), 0);

    let coll_names = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for coll_name in coll_names {
        db.collection(coll_name)
            .insert_one(doc! { "x": 1 }, None)
            .unwrap();
    }

    let colls = get_coll_info(&db, None);
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
    let _guard = LOCK.run_concurrently();

    let db = CLIENT.database(function_name!());
    db.drop(None).unwrap();

    assert_eq!(db.list_collections(None, None).unwrap().count(), 0);

    let coll_names = &["bar", "baz", "foo"];
    for coll_name in coll_names {
        db.collection(coll_name)
            .insert_one(doc! { "x": 1 }, None)
            .unwrap();
    }

    let filter = doc! {
        "name": {
            "$lt": "c"
        }
    };
    let coll_names = &coll_names[..coll_names.len() - 1];

    let colls = get_coll_info(&db, Some(filter));
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
    let _guard = LOCK.run_concurrently();

    let db = CLIENT.database(function_name!());
    db.drop(None).unwrap();

    assert!(db.list_collection_names(None).unwrap().is_empty());

    let expected_colls = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for coll in expected_colls {
        db.collection(coll)
            .insert_one(doc! { "x": 1 }, None)
            .unwrap();
    }

    let mut actual_colls = db.list_collection_names(None).unwrap();
    actual_colls.sort();

    assert_eq!(&actual_colls, expected_colls);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn collection_management() {
    let _guard = LOCK.run_concurrently();

    let db = CLIENT.database(function_name!());
    db.drop(None).unwrap();

    assert!(db.list_collection_names(None).unwrap().is_empty());

    db.create_collection(&format!("{}{}", function_name!(), 1), None)
        .unwrap();

    let options = CreateCollectionOptions::builder()
        .capped(true)
        .size(512)
        .build();
    db.create_collection(&format!("{}{}", function_name!(), 2), Some(options))
        .unwrap();

    let colls = get_coll_info(&db, None);
    assert_eq!(colls.len(), 2);

    assert_eq!(colls[0].name, format!("{}1", function_name!()));
    assert_eq!(colls[0].coll_type, "collection");
    assert!(colls[0].options.is_empty());
    assert!(!colls[0].info.read_only);

    assert_eq!(colls[1].name, format!("{}2", function_name!()));
    assert_eq!(colls[1].coll_type, "collection");
    assert_eq!(colls[1].options.get("capped"), Some(&Bson::Boolean(true)));
    assert_eq!(colls[1].options.get("size"), Some(&Bson::I32(512)));
    assert!(!colls[1].info.read_only);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn db_aggregate() {
    if CLIENT.server_version_lt(4, 0) {
        return;
    }

    let _guard = LOCK.run_concurrently();

    let db = CLIENT.database("admin");

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

    let result = db.aggregate(pipeline, None);
    result.unwrap();
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn db_aggregate_disk_use() {
    if CLIENT.server_version_lt(4, 0) {
        return;
    }

    let _guard = LOCK.run_concurrently();

    let db = CLIENT.database("admin");

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

    let result = db.aggregate(pipeline, Some(options));
    result.unwrap();
}
