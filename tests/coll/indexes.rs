use bson::{bson, doc, Bson, Document};
use mongodb::IndexModel;
use serde::Deserialize;

use crate::CLIENT;

#[derive(Debug, Deserialize, PartialEq)]
struct ListIndexesEntry {
    v: i64,
    key: Document,
    name: String,
    ns: Option<String>,
    unique: Option<bool>,
}

#[test]
#[function_name::named]
fn list_indexes_contains_id() {
    let coll = CLIENT.init_db_and_coll(function_name!(), function_name!());

    let indexes: Vec<_> = coll.list_indexes().unwrap().collect();
    assert!(indexes.is_empty());

    coll.insert_one(doc! { "x": 1 }, None).unwrap();

    let indexes: Result<Vec<Document>, _> = coll.list_indexes().unwrap().collect();
    let indexes = indexes.unwrap();

    assert_eq!(indexes.len(), 1);

    let entry: ListIndexesEntry =
        bson::from_bson(Bson::Document(indexes.into_iter().next().unwrap())).unwrap();

    assert_eq!(entry.key, doc! { "_id": 1 });
    assert_eq!(entry.name, "_id_");

    if let Some(ns) = entry.ns {
        assert_eq!(ns, format!("{}.{}", function_name!(), function_name!()));
    }
}

#[test]
#[function_name::named]
fn index_management() {
    let db = CLIENT.database(function_name!());
    let coll = db.collection(function_name!());

    crate::util::drop_collection(&coll);
    db.create_collection(function_name!(), None).unwrap();

    let keys = vec![
        doc! { "w": 1 },
        doc! {
            "x": -1,
            "y": 1
        },
        doc! { "z": -1 },
    ];

    let indexes = vec![
        IndexModel::builder().keys(keys[0].clone()).build(),
        IndexModel::builder().keys(keys[1].clone()).build(),
        IndexModel::builder()
            .keys(keys[2].clone())
            .options(doc! {
                "unique": true,
                "name": "ziggy"
            })
            .build(),
    ];

    let created_names = coll.create_indexes(indexes).unwrap();
    assert_eq!(created_names, vec!["w_1", "x_-1_y_1", "ziggy"]);

    let doc = doc! {
        "w": true,
        "x": 2,
        "y": 3,
        "z": ["four"]
    };
    coll.insert_one(doc, None).unwrap();

    let mut listed_indexes: Vec<ListIndexesEntry> = coll
        .list_indexes()
        .unwrap()
        .map(|doc| bson::from_bson(Bson::Document(doc.unwrap())).unwrap())
        .collect();
    listed_indexes.sort_by(|i1, i2| i1.name.cmp(&i2.name));

    assert_eq!(listed_indexes.len(), 4);

    let expected_entries: Vec<_> = (0..4)
        .map(|i| {
            let key;
            let name;

            if i == 0 {
                key = doc! { "_id": 1 };
                name = "_id_".to_string();
            } else {
                key = keys[i - 1].clone();
                name = created_names[i - 1].clone();
            };

            let unique = if i == 3 { Some(true) } else { None };

            ListIndexesEntry {
                v: listed_indexes[0].v,
                key,
                name,
                ns: listed_indexes[0]
                    .ns
                    .as_ref()
                    .and_then(|_| Some(format!("{}.{}", function_name!(), function_name!()))),
                unique,
            }
        })
        .collect();

    assert_eq!(listed_indexes, expected_entries);
}

/// This tests that creating two indexes with the same keys but different collations succeeds, and
/// that the indexes can be deleted separately.
#[test]
#[function_name::named]
fn test_create_index_collation() {
    let db = CLIENT.database(function_name!());
    let coll = db.collection(function_name!());

    crate::util::drop_collection(&coll);
    db.create_collection(function_name!(), None).unwrap();

    let keys = doc! { "x": 1 };
    let collation1 = bson::to_bson(&Collation {
        locale: "af".to_string(),
        ..Default::default()
    })
    .unwrap();

    let name = "named";
    let collation2 = bson::to_bson(&Collation {
        locale: "sq".to_string(),
        ..Default::default()
    })
    .unwrap();

    coll.create_index(IndexModel {
        keys: keys.clone(),
        options: Some(doc! { "collation": collation1}),
    })
    .unwrap();

    coll.create_index(IndexModel {
        keys: keys.clone(),
        options: Some(doc! {"name": name, "collation": collation2}),
    })
    .unwrap();

    assert!(coll.drop_index(name).is_ok());

    let listed_indexes: Vec<ListIndexesEntry> = coll
        .list_indexes()
        .unwrap()
        .map(|doc| bson::from_bson(Bson::Document(doc.unwrap())).unwrap())
        .collect();

    assert_eq!(
        listed_indexes
            .iter()
            .filter(|index| index.name == name)
            .count(),
        0
    );

    assert_eq!(
        listed_indexes
            .iter()
            .filter(|index| index.key == keys)
            .count(),
        1
    );
}
