use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::RwLockReadGuard;

use crate::{
    bson::{doc, Bson, Document},
    error::{ErrorKind, Result},
    event::command::{CommandEventHandler, CommandStartedEvent},
    options::{
        Acknowledgment,
        FindOneOptions,
        InsertManyOptions,
        InsertOneOptions,
        ReadConcern,
        TransactionOptions,
        UpdateOptions,
        WriteConcern,
    },
    test::{EventClient, TestClient, CLIENT_OPTIONS, LOCK},
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
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    let db = client.database(function_name!());
    let error = db
        .run_command(
            doc! {
                "insert": function_name!(),
                "documents": [ {} ],
                "writeConcern": { "w": 0, "j": true }
            },
            None,
        )
        .await
        .expect_err("insert should fail");
    assert!(matches!(*error.kind, ErrorKind::InvalidArgument { .. }));

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
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    let db = client.database(function_name!());
    let error = db
        .run_command(
            doc! {
                "insert": function_name!(),
                "documents": [ {} ],
                "writeConcern": { "w": 0 }
            },
            None,
        )
        .await
        .expect_err("insert should fail");
    assert!(matches!(*error.kind, ErrorKind::InvalidArgument { .. }));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn snapshot_read_concern() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    // snapshot read concern was introduced in 4.0
    if client.server_version_lt(4, 0) {
        return;
    }

    let coll = client
        .database(function_name!())
        .collection::<Document>(function_name!());

    // TODO RUST-122 run this test on sharded clusters
    if client.is_replica_set() && client.server_version_gte(4, 0) {
        let mut session = client.start_session(None).await.unwrap();
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
    let _guard = LOCK.run_concurrently().await;
    let (buffer, coll) = get_monitored_collection(function_name!()).await.unwrap();

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
        buffer.write_concerns("insert"),
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
    let _guard = LOCK.run_concurrently().await;
    let (buffer, coll) = get_monitored_collection(function_name!()).await.unwrap();

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
        buffer.write_concerns("insert"),
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
    let _guard = LOCK.run_concurrently().await;
    let (buffer, coll) = get_monitored_collection(function_name!()).await.unwrap();

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
        buffer.write_concerns("update"),
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
    let _guard = LOCK.run_concurrently().await;
    let (buffer, coll) = get_monitored_collection(function_name!()).await.unwrap();

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
        buffer.write_concerns("update"),
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

async fn get_monitored_collection(name: &str) -> Result<(Arc<CommandBuffer>, Collection)> {
    let buffer = Arc::new(CommandBuffer::new());
    let mut options = CLIENT_OPTIONS.clone();
    options.command_event_handler = Some(buffer.clone());
    let client = TestClient::with_options(Some(options)).await;
    let db = client.database("test");
    let coll: Collection = db.collection(name);
    coll.drop(None).await?;
    Ok((buffer, coll))
}

struct CommandBuffer {
    commands: Mutex<Vec<Document>>,
}

impl CommandBuffer {
    fn new() -> Self {
        CommandBuffer {
            commands: Mutex::new(vec![]),
        }
    }
    fn write_concerns(&self, key: &str) -> Vec<Document> {
        self.commands
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .filter(|d| d.contains_key(key))
            .map(|d| d.get_document("writeConcern").unwrap().clone())
            .collect()
    }
}

impl CommandEventHandler for CommandBuffer {
    fn handle_command_started_event(&self, event: CommandStartedEvent) {
        self.commands.lock().unwrap().push(event.command);
    }

    fn handle_command_succeeded_event(&self, _event: crate::event::command::CommandSucceededEvent) {
    }

    fn handle_command_failed_event(&self, _event: crate::event::command::CommandFailedEvent) {}
}
