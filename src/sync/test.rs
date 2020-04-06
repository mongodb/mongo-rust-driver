use bson::{doc, Document};

use crate::{
    error::{CommandError, ErrorKind, Result},
    options::{Acknowledgment, CollectionOptions, DatabaseOptions, FindOptions, WriteConcern},
    sync::{Client, Collection},
    test::CLIENT_OPTIONS,
};

fn init_db_and_coll(client: &Client, db_name: &str, coll_name: &str) -> Collection {
    let coll = client.database(db_name).collection(coll_name);
    drop_collection(&coll);
    coll
}

pub fn drop_collection(coll: &Collection) {
    match coll.drop(None).as_ref().map_err(|e| e.as_ref()) {
        Err(ErrorKind::CommandError(CommandError { code: 26, .. })) | Ok(_) => {}
        e @ Err(_) => {
            e.unwrap();
        }
    };
}

#[test]
#[function_name::named]
fn client() {
    let options = CLIENT_OPTIONS.clone();
    let client = Client::with_options(options).expect("client creation should succeed");

    client
        .database(function_name!())
        .collection(function_name!())
        .insert_one(Document::new(), None)
        .expect("insert should succeed");

    let db_names = client
        .list_database_names(None)
        .expect("list_database_names should succeed");
    assert!(db_names.contains(&function_name!().to_string()));
}

#[test]
#[function_name::named]
fn database() {
    let options = CLIENT_OPTIONS.clone();
    let client = Client::with_options(options).expect("client creation should succeed");
    let db = client.database(function_name!());

    let coll = init_db_and_coll(&client, function_name!(), function_name!());

    coll.insert_one(doc! { "x": 1 }, None)
        .expect("insert should succeed");

    let coll_names = db
        .list_collection_names(None)
        .expect("list_database_names should succeed");
    assert!(coll_names.contains(&function_name!().to_string()));

    let admin_db = client.database("admin");
    let pipeline = vec![
        doc! { "$currentOp": {} },
        doc! { "$limit": 1 },
        doc! { "$addFields": { "dummy": 1 } },
        doc! { "$project": { "_id": 0, "dummy": 1 } },
    ];
    let cursor = admin_db
        .aggregate(pipeline, None)
        .expect("aggregate should succeed");
    let results: Vec<Document> = cursor
        .collect::<Result<Vec<Document>>>()
        .expect("cursor iteration should succeed");
    assert_eq!(results, vec![doc! { "dummy": 1 }]);

    let wc = WriteConcern {
        w: Some(Acknowledgment::Majority),
        journal: None,
        w_timeout: None,
    };
    let options = DatabaseOptions::builder().write_concern(wc.clone()).build();
    let db = client.database_with_options(function_name!(), options);
    assert!(db.write_concern().is_some());
    assert_eq!(db.write_concern().unwrap(), &wc);
}

#[test]
#[function_name::named]
fn collection() {
    let options = CLIENT_OPTIONS.clone();
    let client = Client::with_options(options).expect("client creation should succeed");
    let coll = init_db_and_coll(&client, function_name!(), function_name!());

    coll.insert_one(doc! { "x": 1 }, None)
        .expect("insert should succeed");

    let find_options = FindOptions::builder().projection(doc! { "_id": 0 }).build();
    let cursor = coll
        .find(doc! { "x": 1 }, find_options)
        .expect("find should succeed");
    let results = cursor
        .collect::<Result<Vec<Document>>>()
        .expect("cursor iteration should succeed");
    assert_eq!(results, vec![doc! { "x": 1 }]);

    let pipeline = vec![
        doc! { "$match": { "x": 1 } },
        doc! { "$project": { "_id" : 0 } },
    ];
    let cursor = coll
        .aggregate(pipeline, None)
        .expect("aggregate should succeed");
    let results = cursor
        .collect::<Result<Vec<Document>>>()
        .expect("cursor iteration should succeed");
    assert_eq!(results, vec![doc! { "x": 1 }]);

    let wc = WriteConcern {
        w: Acknowledgment::Tag("hello".to_string()).into(),
        journal: None,
        w_timeout: None,
    };
    let db_options = DatabaseOptions::builder().write_concern(wc.clone()).build();
    let coll = client
        .database_with_options(function_name!(), db_options)
        .collection(function_name!());
    assert_eq!(coll.write_concern(), Some(&wc));

    let coll_options = CollectionOptions::builder()
        .write_concern(wc.clone())
        .build();
    let coll = client
        .database(function_name!())
        .collection_with_options(function_name!(), coll_options);
    assert_eq!(coll.write_concern(), Some(&wc));
}
