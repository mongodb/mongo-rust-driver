use std::cmp::Ord;

use bson::{Bson, Document};
use mongodb::{Client, Database};

use crate::MONGODB_URI;

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
    let colls: Result<Vec<Document>, _> = db.list_collections(filter).unwrap().collect();
    let mut colls: Vec<CollectionInfo> = colls
        .unwrap()
        .into_iter()
        .map(|doc| bson::from_bson(Bson::Document(doc)).unwrap())
        .collect();
    colls.sort_by(|c1, c2| c1.name.cmp(&c2.name));

    colls
}

#[test]
fn is_master() {
    let client = Client::with_uri(MONGODB_URI.as_str()).unwrap();
    let db = client.database("test");
    let doc = db.run_command(doc! { "ismaster": 1 }, None).unwrap();
    let is_master_reply: IsMasterReply = bson::from_bson(Bson::Document(doc)).unwrap();

    assert!(is_master_reply.ismaster);
    assert_ulps_eq!(is_master_reply.ok, 1.0);
}

#[test]
fn list_collections() {
    let client = Client::with_uri(MONGODB_URI.as_str()).unwrap();
    let db = client.database("list_collections");
    db.drop().unwrap();

    assert_eq!(db.list_collections(None).unwrap().count(), 0);

    let coll_names = &[
        "list_collections1",
        "list_collections2",
        "list_collections3",
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

#[test]
fn list_collections_filter() {
    let client = Client::with_uri(MONGODB_URI.as_str()).unwrap();
    let db = client.database("list_collections_filter");
    db.drop().unwrap();

    assert_eq!(db.list_collections(None).unwrap().count(), 0);

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

#[test]
fn list_collection_names() {
    let client = Client::with_uri(MONGODB_URI.as_str()).unwrap();
    let db = client.database("list_collection_names");
    db.drop().unwrap();

    assert!(db.list_collection_names(None).unwrap().is_empty());

    let expected_colls = &[
        "list_collection_names1",
        "list_collection_names2",
        "list_collection_names3",
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

#[test]
fn collection_management() {
    let client = Client::with_uri(MONGODB_URI.as_str()).unwrap();
    let db = client.database("collection_management");
    db.drop().unwrap();

    assert!(db.list_collection_names(None).unwrap().is_empty());

    db.create_collection("collection_management1", None)
        .unwrap();

    let options = doc! {
        "capped": true,
        "size": 512
    };
    db.create_collection("collection_management2", Some(options.clone()))
        .unwrap();

    let colls = get_coll_info(&db, None);
    assert_eq!(colls.len(), 2);

    assert_eq!(colls[0].name, "collection_management1");
    assert_eq!(colls[0].coll_type, "collection");
    assert!(colls[0].options.is_empty());
    assert!(!colls[0].info.read_only);

    assert_eq!(colls[1].name, "collection_management2");
    assert_eq!(colls[1].coll_type, "collection");
    assert_eq!(colls[1].options, options);
    assert!(!colls[1].info.read_only);
}
