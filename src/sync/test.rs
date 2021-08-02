use std::fmt::Debug;

use pretty_assertions::assert_eq;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::RwLockReadGuard;

use crate::{
    bson::{doc, Document},
    error::Result,
    options::{
        Acknowledgment,
        ClientOptions,
        CollectionOptions,
        DatabaseOptions,
        FindOptions,
        ServerAddress,
        WriteConcern,
    },
    sync::{Client, Collection},
    test::{TestClient as AsyncTestClient, CLIENT_OPTIONS, LOCK},
    RUNTIME,
};

fn init_db_and_coll(client: &Client, db_name: &str, coll_name: &str) -> Collection<Document> {
    let coll = client.database(db_name).collection(coll_name);
    coll.drop(None).unwrap();
    coll
}

fn init_db_and_typed_coll<T>(client: &Client, db_name: &str, coll_name: &str) -> Collection<T>
where
    T: Serialize + DeserializeOwned + Unpin + Debug,
{
    let coll = client.database(db_name).collection(coll_name);
    coll.drop(None).unwrap();
    coll
}

#[test]
fn client_options() {
    let _guard: RwLockReadGuard<()> = RUNTIME.block_on(async { LOCK.run_concurrently().await });

    let mut options = ClientOptions::parse("mongodb://localhost:27017/").unwrap();

    options.original_uri.take();

    assert_eq!(
        options,
        ClientOptions::builder()
            .hosts(vec![ServerAddress::Tcp {
                host: "localhost".into(),
                port: Some(27017)
            }])
            .build()
    );
}

#[test]
#[function_name::named]
fn client() {
    let _guard: RwLockReadGuard<()> = RUNTIME.block_on(async { LOCK.run_concurrently().await });

    let options = CLIENT_OPTIONS.clone();
    let client = Client::with_options(options).expect("client creation should succeed");

    client
        .database(function_name!())
        .collection(function_name!())
        .insert_one(Document::new(), None)
        .expect("insert should succeed");

    let db_names = client
        .list_database_names(None, None)
        .expect("list_database_names should succeed");
    assert!(db_names.contains(&function_name!().to_string()));
}

#[test]
#[function_name::named]
fn database() {
    let _guard: RwLockReadGuard<()> = RUNTIME.block_on(async { LOCK.run_concurrently().await });

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
    let _guard: RwLockReadGuard<()> = RUNTIME.block_on(async { LOCK.run_concurrently().await });

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
        w: Acknowledgment::Custom("hello".to_string()).into(),
        journal: None,
        w_timeout: None,
    };
    let db_options = DatabaseOptions::builder().write_concern(wc.clone()).build();
    let coll = client
        .database_with_options(function_name!(), db_options)
        .collection::<Document>(function_name!());
    assert_eq!(coll.write_concern(), Some(&wc));

    let coll_options = CollectionOptions::builder()
        .write_concern(wc.clone())
        .build();
    let coll = client
        .database(function_name!())
        .collection_with_options::<Document>(function_name!(), coll_options);
    assert_eq!(coll.write_concern(), Some(&wc));
}

#[test]
#[function_name::named]
fn typed_collection() {
    let _guard: RwLockReadGuard<()> = RUNTIME.block_on(async { LOCK.run_concurrently().await });

    let options = CLIENT_OPTIONS.clone();
    let client = Client::with_options(options).expect("client creation should succeed");
    let coll = init_db_and_typed_coll(&client, function_name!(), function_name!());

    #[derive(Serialize, Deserialize, Debug)]
    struct MyType {
        x: i32,
        str: String,
    }
    let my_type = MyType {
        x: 1,
        str: "hello".into(),
    };

    assert!(coll.insert_one(my_type, None).is_ok());
}

#[test]
#[function_name::named]
fn transactions() {
    let _guard: RwLockReadGuard<()> = RUNTIME.block_on(async { LOCK.run_concurrently().await });

    let should_skip = RUNTIME.block_on(async {
        let test_client = AsyncTestClient::new().await;
        !test_client.supports_transactions()
    });
    if should_skip {
        return;
    }

    let options = CLIENT_OPTIONS.clone();
    let client = Client::with_options(options).expect("client creation should succeed");
    let mut session = client
        .start_session(None)
        .expect("session creation should succeed");
    let coll = init_db_and_typed_coll(&client, function_name!(), function_name!());

    client
        .database(function_name!())
        .create_collection(function_name!(), None)
        .expect("create collection should succeed");

    session
        .start_transaction(None)
        .expect("start transaction should succeed");
    coll.insert_one_with_session(doc! { "x": 1 }, None, &mut session)
        .expect("insert should succeed");
    session
        .commit_transaction()
        .expect("commit transaction should succeed");

    session
        .start_transaction(None)
        .expect("start transaction should succeed");
    coll.insert_one_with_session(doc! { "x": 1 }, None, &mut session)
        .expect("insert should succeed");
    session
        .abort_transaction()
        .expect("abort transaction should succeed");
}

#[test]
#[function_name::named]
fn collection_generic_bounds() {
    let _guard: RwLockReadGuard<()> = RUNTIME.block_on(async { LOCK.run_concurrently().await });

    #[derive(Deserialize)]
    struct Foo;

    let options = CLIENT_OPTIONS.clone();
    let client = Client::with_options(options).expect("client creation should succeed");

    // ensure this code successfully compiles
    let coll: Collection<Foo> = client
        .database(function_name!())
        .collection(function_name!());
    let _result: Result<Option<Foo>> = coll.find_one(None, None);

    #[derive(Serialize)]
    struct Bar;

    // ensure this code successfully compiles
    let coll: Collection<Bar> = client
        .database(function_name!())
        .collection(function_name!());
    let _result = coll.insert_one(Bar {}, None);
}
