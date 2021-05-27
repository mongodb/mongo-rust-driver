use std::time::Duration;
use tokio::sync::RwLockReadGuard;

use crate::{
    bson::{doc, Bson, Document},
    error::ErrorKind,
    options::{
        Acknowledgment,
        FindOneOptions,
        InsertOneOptions,
        ReadConcern,
        TransactionOptions,
        WriteConcern,
    },
    test::{EventClient, TestClient, LOCK},
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
