use std::time::Duration;

use crate::{
    bson::{doc, Bson, Document},
    error::ErrorKind,
    options::{
        Acknowledgment,
        AggregateOptions,
        CreateCollectionOptions,
        DeleteOptions,
        DropCollectionOptions,
        FindOneAndDeleteOptions,
        FindOneAndReplaceOptions,
        FindOneAndUpdateOptions,
        FindOneOptions,
        InsertManyOptions,
        InsertOneOptions,
        ReadConcern,
        ReplaceOptions,
        TransactionOptions,
        UpdateOptions,
        WriteConcern,
    },
    test::{EventClient, TestClient},
    Collection,
};

#[test]
fn write_concern_is_acknowledged() {
    let w_1 = WriteConcern::builder()
        .w(Acknowledgment::Nodes(1))
        .journal(false)
        .build();
    assert!(w_1.is_acknowledged());

    let w_majority = WriteConcern::builder()
        .w(Acknowledgment::Majority)
        .journal(false)
        .build();
    assert!(w_majority.is_acknowledged());

    let w_0 = WriteConcern::builder()
        .w(Acknowledgment::Nodes(0))
        .journal(false)
        .build();
    assert!(!w_0.is_acknowledged());

    let w_0 = WriteConcern::builder().w(Acknowledgment::Nodes(0)).build();
    assert!(!w_0.is_acknowledged());

    let empty = WriteConcern::builder().build();
    assert!(empty.is_acknowledged());

    let empty = WriteConcern::builder().journal(false).build();
    assert!(empty.is_acknowledged());

    let empty = WriteConcern::builder().journal(true).build();
    assert!(empty.is_acknowledged());
}

#[test]
fn write_concern_deserialize() {
    let w_1 = doc! { "w": 1 };
    let wc: WriteConcern = bson::from_bson(Bson::Document(w_1)).unwrap();
    assert_eq!(
        wc,
        WriteConcern {
            w: Acknowledgment::Nodes(1).into(),
            w_timeout: None,
            journal: None
        }
    );

    let w_majority = doc! { "w": "majority" };
    let wc: WriteConcern = bson::from_bson(Bson::Document(w_majority)).unwrap();
    assert_eq!(
        wc,
        WriteConcern {
            w: Acknowledgment::Majority.into(),
            w_timeout: None,
            journal: None
        }
    );

    let w_timeout = doc! { "w": "majority", "wtimeout": 100 };
    let wc: WriteConcern = bson::from_bson(Bson::Document(w_timeout)).unwrap();
    assert_eq!(
        wc,
        WriteConcern {
            w: Acknowledgment::Majority.into(),
            w_timeout: Duration::from_millis(100).into(),
            journal: None
        }
    );

    let journal = doc! { "w": "majority", "j": true };
    let wc: WriteConcern = bson::from_bson(Bson::Document(journal)).unwrap();
    assert_eq!(
        wc,
        WriteConcern {
            w: Acknowledgment::Majority.into(),
            w_timeout: None,
            journal: true.into()
        }
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn inconsistent_write_concern_rejected() {
    let client = TestClient::new().await;
    let db = client.database(function_name!());

    let coll = db.collection(function_name!());
    let wc = WriteConcern {
        w: Acknowledgment::Nodes(0).into(),
        journal: true.into(),
        w_timeout: None,
    };
    let options = InsertOneOptions::builder().write_concern(wc).build();
    let error = coll
        .insert_one(doc! {}, options)
        .await
        .expect_err("insert should fail");
    assert!(matches!(*error.kind, ErrorKind::InvalidArgument { .. }));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn unacknowledged_write_concern_rejected() {
    let client = TestClient::new().await;
    let db = client.database(function_name!());
    let coll = db.collection(function_name!());
    let wc = WriteConcern {
        w: Acknowledgment::Nodes(0).into(),
        journal: false.into(),
        w_timeout: None,
    };
    let options = InsertOneOptions::builder().write_concern(wc).build();
    let error = coll
        .insert_one(doc! {}, options)
        .await
        .expect_err("insert should fail");
    assert!(matches!(*error.kind, ErrorKind::InvalidArgument { .. }));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn snapshot_read_concern() {
    let client = EventClient::new().await;
    // snapshot read concern was introduced in 4.0
    if client.server_version_lt(4, 0) {
        return;
    }

    let coll = client
        .database(function_name!())
        .collection::<Document>(function_name!());

    if client.supports_transactions() {
        let mut session = client.start_session().await.unwrap();
        let options = TransactionOptions::builder()
            .read_concern(ReadConcern::snapshot())
            .build();
        session.start_transaction(options).await.unwrap();
        let result = coll.find_one_with_session(None, None, &mut session).await;
        assert!(result.is_ok());
        assert_event_contains_read_concern(&client).await;
    }

    if client.server_version_lt(4, 9) {
        let options = FindOneOptions::builder()
            .read_concern(ReadConcern::snapshot())
            .build();
        let error = coll
            .find_one(None, options)
            .await
            .expect_err("non-transaction find one with snapshot read concern should fail");
        // ensure that an error from the server is returned
        assert!(matches!(*error.kind, ErrorKind::Command(_)));
        assert_event_contains_read_concern(&client).await;
    }
}

async fn assert_event_contains_read_concern(client: &EventClient) {
    let event = client
        .get_command_started_events(&["find"])
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        event
            .command
            .get_document("readConcern")
            .unwrap()
            .get_str("level")
            .unwrap(),
        "snapshot"
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn command_contains_write_concern_insert_one() {
    let client = EventClient::new().await;
    let coll: Collection<Document> = client.database("test").collection(function_name!());

    coll.drop(None).await.unwrap();
    coll.insert_one(
        doc! { "foo": "bar" },
        InsertOneOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(true)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    coll.insert_one(
        doc! { "foo": "bar" },
        InsertOneOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(false)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();

    assert_eq!(
        command_write_concerns(&client, "insert"),
        vec![
            doc! {
                "w": 1,
                "j": true,
            },
            doc! {
                "w": 1,
                "j": false,
            },
        ]
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn command_contains_write_concern_insert_many() {
    let client = EventClient::new().await;
    let coll: Collection<Document> = client.database("test").collection(function_name!());

    coll.drop(None).await.unwrap();
    coll.insert_many(
        &[doc! { "foo": "bar" }],
        InsertManyOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(true)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    coll.insert_many(
        &[doc! { "foo": "bar" }],
        InsertManyOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(false)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();

    assert_eq!(
        command_write_concerns(&client, "insert"),
        vec![
            doc! {
                "w": 1,
                "j": true,
            },
            doc! {
                "w": 1,
                "j": false,
            },
        ]
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn command_contains_write_concern_update_one() {
    let client = EventClient::new().await;
    let coll: Collection<Document> = client.database("test").collection(function_name!());

    coll.drop(None).await.unwrap();
    coll.insert_one(doc! { "foo": "bar" }, None).await.unwrap();
    coll.update_one(
        doc! { "foo": "bar" },
        doc! { "$set": { "foo": "baz" } },
        UpdateOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(true)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    coll.update_one(
        doc! { "foo": "baz" },
        doc! { "$set": { "foo": "quux" } },
        UpdateOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(false)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();

    assert_eq!(
        command_write_concerns(&client, "update"),
        vec![
            doc! {
                "w": 1,
                "j": true,
            },
            doc! {
                "w": 1,
                "j": false,
            },
        ]
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn command_contains_write_concern_update_many() {
    let client = EventClient::new().await;
    let coll: Collection<Document> = client.database("test").collection(function_name!());

    coll.drop(None).await.unwrap();
    coll.insert_many(&[doc! { "foo": "bar" }, doc! { "foo": "bar" }], None)
        .await
        .unwrap();
    coll.update_many(
        doc! { "foo": "bar" },
        doc! { "$set": { "foo": "baz" } },
        UpdateOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(true)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    coll.update_many(
        doc! { "foo": "baz" },
        doc! { "$set": { "foo": "quux" } },
        UpdateOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(false)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();

    assert_eq!(
        command_write_concerns(&client, "update"),
        vec![
            doc! {
                "w": 1,
                "j": true,
            },
            doc! {
                "w": 1,
                "j": false,
            },
        ]
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn command_contains_write_concern_replace_one() {
    let client = EventClient::new().await;
    let coll: Collection<Document> = client.database("test").collection(function_name!());

    coll.drop(None).await.unwrap();
    coll.insert_one(doc! { "foo": "bar" }, None).await.unwrap();
    coll.replace_one(
        doc! { "foo": "bar" },
        doc! { "baz": "fun" },
        ReplaceOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(true)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    coll.replace_one(
        doc! { "foo": "bar" },
        doc! { "baz": "fun" },
        ReplaceOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(false)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();

    assert_eq!(
        command_write_concerns(&client, "update"),
        vec![
            doc! {
                "w": 1,
                "j": true,
            },
            doc! {
                "w": 1,
                "j": false,
            },
        ]
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn command_contains_write_concern_delete_one() {
    let client = EventClient::new().await;
    let coll: Collection<Document> = client.database("test").collection(function_name!());

    coll.drop(None).await.unwrap();
    coll.insert_many(&[doc! { "foo": "bar" }, doc! { "foo": "bar" }], None)
        .await
        .unwrap();
    coll.delete_one(
        doc! { "foo": "bar" },
        DeleteOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(true)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    coll.delete_one(
        doc! { "foo": "bar" },
        DeleteOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(false)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();

    assert_eq!(
        command_write_concerns(&client, "delete"),
        vec![
            doc! {
                "w": 1,
                "j": true,
            },
            doc! {
                "w": 1,
                "j": false,
            },
        ]
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn command_contains_write_concern_delete_many() {
    let client = EventClient::new().await;
    let coll: Collection<Document> = client.database("test").collection(function_name!());

    coll.drop(None).await.unwrap();
    coll.insert_many(&[doc! { "foo": "bar" }, doc! { "foo": "bar" }], None)
        .await
        .unwrap();
    coll.delete_many(
        doc! { "foo": "bar" },
        DeleteOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(true)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    coll.insert_many(&[doc! { "foo": "bar" }, doc! { "foo": "bar" }], None)
        .await
        .unwrap();
    coll.delete_many(
        doc! { "foo": "bar" },
        DeleteOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(false)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();

    assert_eq!(
        command_write_concerns(&client, "delete"),
        vec![
            doc! {
                "w": 1,
                "j": true,
            },
            doc! {
                "w": 1,
                "j": false,
            },
        ]
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn command_contains_write_concern_find_one_and_delete() {
    let client = EventClient::new().await;
    let coll: Collection<Document> = client.database("test").collection(function_name!());

    coll.drop(None).await.unwrap();
    coll.insert_many(&[doc! { "foo": "bar" }, doc! { "foo": "bar" }], None)
        .await
        .unwrap();
    coll.find_one_and_delete(
        doc! { "foo": "bar" },
        FindOneAndDeleteOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(true)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    coll.find_one_and_delete(
        doc! { "foo": "bar" },
        FindOneAndDeleteOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(false)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();

    assert_eq!(
        command_write_concerns(&client, "findAndModify"),
        vec![
            doc! {
                "w": 1,
                "j": true,
            },
            doc! {
                "w": 1,
                "j": false,
            },
        ]
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn command_contains_write_concern_find_one_and_replace() {
    let client = EventClient::new().await;
    let coll: Collection<Document> = client.database("test").collection(function_name!());

    coll.drop(None).await.unwrap();
    coll.insert_many(&[doc! { "foo": "bar" }, doc! { "foo": "bar" }], None)
        .await
        .unwrap();
    coll.find_one_and_replace(
        doc! { "foo": "bar" },
        doc! { "baz": "fun" },
        FindOneAndReplaceOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(true)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    coll.find_one_and_replace(
        doc! { "foo": "bar" },
        doc! { "baz": "fun" },
        FindOneAndReplaceOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(false)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();

    assert_eq!(
        command_write_concerns(&client, "findAndModify"),
        vec![
            doc! {
                "w": 1,
                "j": true,
            },
            doc! {
                "w": 1,
                "j": false,
            },
        ]
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn command_contains_write_concern_find_one_and_update() {
    let client = EventClient::new().await;
    let coll: Collection<Document> = client.database("test").collection(function_name!());

    coll.drop(None).await.unwrap();
    coll.insert_many(&[doc! { "foo": "bar" }, doc! { "foo": "bar" }], None)
        .await
        .unwrap();
    coll.find_one_and_update(
        doc! { "foo": "bar" },
        doc! { "$set": { "foo": "fun" } },
        FindOneAndUpdateOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(true)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    coll.find_one_and_update(
        doc! { "foo": "bar" },
        doc! { "$set": { "foo": "fun" } },
        FindOneAndUpdateOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(false)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();

    assert_eq!(
        command_write_concerns(&client, "findAndModify"),
        vec![
            doc! {
                "w": 1,
                "j": true,
            },
            doc! {
                "w": 1,
                "j": false,
            },
        ]
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn command_contains_write_concern_aggregate() {
    let client = EventClient::new().await;
    let coll: Collection<Document> = client.database("test").collection(function_name!());

    coll.drop(None).await.unwrap();
    coll.insert_one(doc! { "foo": "bar" }, None).await.unwrap();
    coll.aggregate(
        vec![
            doc! { "$match": { "foo": "bar" } },
            doc! { "$addFields": { "foo": "baz" } },
            doc! { "$out": format!("{}-out", function_name!()) },
        ],
        AggregateOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(true)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    coll.aggregate(
        vec![
            doc! { "$match": { "foo": "bar" } },
            doc! { "$addFields": { "foo": "baz" } },
            doc! { "$out": format!("{}-out", function_name!()) },
        ],
        AggregateOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(false)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();

    assert_eq!(
        command_write_concerns(&client, "aggregate"),
        vec![
            doc! {
                "w": 1,
                "j": true,
            },
            doc! {
                "w": 1,
                "j": false,
            },
        ]
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn command_contains_write_concern_drop() {
    let client = EventClient::new().await;
    let coll: Collection<Document> = client.database("test").collection(function_name!());

    coll.drop(None).await.unwrap();
    client.clear_cached_events();
    coll.insert_one(doc! { "foo": "bar" }, None).await.unwrap();
    coll.drop(
        DropCollectionOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(true)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    coll.insert_one(doc! { "foo": "bar" }, None).await.unwrap();
    coll.drop(
        DropCollectionOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(false)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();

    assert_eq!(
        command_write_concerns(&client, "drop"),
        vec![
            doc! {
                "w": 1,
                "j": true,
            },
            doc! {
                "w": 1,
                "j": false,
            },
        ]
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn command_contains_write_concern_create_collection() {
    let client = EventClient::new().await;
    let db = client.database("test");
    let coll: Collection<Document> = db.collection(function_name!());

    coll.drop(None).await.unwrap();
    db.create_collection(
        function_name!(),
        CreateCollectionOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(true)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();
    coll.drop(None).await.unwrap();
    db.create_collection(
        function_name!(),
        CreateCollectionOptions::builder()
            .write_concern(
                WriteConcern::builder()
                    .w(Acknowledgment::Nodes(1))
                    .journal(false)
                    .build(),
            )
            .build(),
    )
    .await
    .unwrap();

    assert_eq!(
        command_write_concerns(&client, "create"),
        vec![
            doc! {
                "w": 1,
                "j": true,
            },
            doc! {
                "w": 1,
                "j": false,
            },
        ]
    );
}

fn command_write_concerns(client: &EventClient, key: &str) -> Vec<Document> {
    client
        .get_command_started_events(&[key])
        .into_iter()
        .map(|d| d.command.get_document("writeConcern").unwrap().clone())
        .collect()
}
