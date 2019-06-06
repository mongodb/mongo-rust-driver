use bson::{Bson, Document};
use mongodb::options::IndexModel;

#[derive(Debug, Deserialize)]
struct ListIndexesEntry {
    v: i64,
    key: Document,
    name: String,
    ns: String,
    unique: Option<bool>,
}

#[test]
fn list_indexes_contains_id() {
    let coll = crate::get_coll("list_indexes_contains_id", "list_indexes_contains_id");
    coll.drop().unwrap();

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
    assert_eq!(
        entry.ns,
        "list_indexes_contains_id.list_indexes_contains_id"
    );
}

#[test]
fn index_management() {
    let db = crate::get_db("index_management");
    let coll = db.collection("index_management");

    coll.drop().unwrap();
    db.create_collection("index_management", None).unwrap();

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

    assert_eq!(listed_indexes[0].key, doc! { "_id": 1 });
    assert_eq!(listed_indexes[0].name, "_id_");
    assert_eq!(listed_indexes[0].ns, "index_management.index_management");
    assert!(listed_indexes[0].unique.is_none());

    assert_eq!(listed_indexes[1].key, keys[0]);
    assert_eq!(listed_indexes[1].name, created_names[0]);
    assert_eq!(listed_indexes[1].ns, "index_management.index_management");
    assert!(listed_indexes[1].unique.is_none());

    assert_eq!(listed_indexes[2].key, keys[1]);
    assert_eq!(listed_indexes[2].name, created_names[1]);
    assert_eq!(listed_indexes[2].ns, "index_management.index_management");
    assert!(listed_indexes[2].unique.is_none());

    assert_eq!(listed_indexes[3].key, keys[2]);
    assert_eq!(listed_indexes[3].name, created_names[2]);
    assert_eq!(listed_indexes[3].ns, "index_management.index_management");
    assert_eq!(listed_indexes[3].unique, Some(true));
}
