use std::sync::{Arc, Mutex};

use bson::Document;
use futures::TryStreamExt;
use futures_util::{future::try_join_all, FutureExt};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    bson::doc,
    error::{ErrorKind, Result},
    event::command::{CommandEventHandler, CommandStartedEvent},
    options::SessionOptions,
    test::{TestClient, CLIENT_OPTIONS, LOCK},
    Client,
};

use super::{run_spec_test_with_path, run_unified_format_test};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test_with_path(&["sessions"], run_unified_format_test).await;
}

// Sessions prose test 1
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn snapshot_and_causal_consistency_are_mutually_exclusive() {
    let options = SessionOptions::builder()
        .snapshot(true)
        .causal_consistency(true)
        .build();
    let client = TestClient::new().await;
    assert!(client.start_session(options).await.is_err());
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn explicit_session_created_on_same_client() {
    let _guard: RwLockReadGuard<_> = LOCK.run_concurrently().await;

    let client0 = TestClient::new().await;
    let client1 = TestClient::new().await;

    let mut session0 = client0.start_session(None).await.unwrap();
    let mut session1 = client1.start_session(None).await.unwrap();

    let db = client0.database(function_name!());
    let err = db
        .list_collections_with_session(None, None, &mut session1)
        .await
        .unwrap_err();
    match *err.kind {
        ErrorKind::InvalidArgument { message } => assert!(message.contains("session provided")),
        other => panic!("expected InvalidArgument error, got {:?}", other),
    }

    let coll = client1
        .database(function_name!())
        .collection(function_name!());
    let err = coll
        .insert_one_with_session(doc! {}, None, &mut session0)
        .await
        .unwrap_err();
    match *err.kind {
        ErrorKind::InvalidArgument { message } => assert!(message.contains("session provided")),
        other => panic!("expected InvalidArgument error, got {:?}", other),
    }
}

// Sessions prose test 14
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn implicit_session_after_connection() {
    struct EventHandler {
        lsids: Mutex<Vec<Document>>,
    }

    impl CommandEventHandler for EventHandler {
        fn handle_command_started_event(&self, event: CommandStartedEvent) {
            self.lsids
                .lock()
                .unwrap()
                .push(event.command.get_document("lsid").unwrap().clone());
        }
    }

    let _guard: RwLockReadGuard<_> = LOCK.run_concurrently().await;
    let event_handler = Arc::new(EventHandler {
        lsids: Mutex::new(vec![]),
    });

    let mut min_lsids = usize::MAX;
    let mut max_lsids = 0usize;
    for _ in 0..5 {
        let client = {
            let mut options = CLIENT_OPTIONS.get().await.clone();
            options.max_pool_size = Some(1);
            options.retry_writes = Some(true);
            options.hosts.drain(1..);
            options.command_event_handler = Some(event_handler.clone());
            Client::with_options(options).unwrap()
        };

        let coll = client
            .database("test_lazy_implicit")
            .collection::<Document>("test");

        let mut ops = vec![];
        fn ignore_val<T>(r: Result<T>) -> Result<()> {
            r.map(|_| ())
        }
        ops.push(coll.insert_one(doc! {}, None).map(ignore_val).boxed());
        ops.push(coll.delete_one(doc! {}, None).map(ignore_val).boxed());
        ops.push(
            coll.update_one(doc! {}, doc! { "$set": { "a": 1 } }, None)
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            coll.find_one_and_delete(doc! {}, None)
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            coll.find_one_and_update(doc! {}, doc! { "$set": { "a": 1 } }, None)
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            coll.find_one_and_replace(doc! {}, doc! { "a": 1 }, None)
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            async {
                let cursor = coll.find(doc! {}, None).await.unwrap();
                let r: Result<Vec<_>> = cursor.try_collect().await;
                r.map(|_| ())
            }
            .boxed(),
        );

        let _ = try_join_all(ops).await.unwrap();

        let mut lsids = event_handler.lsids.lock().unwrap();
        let mut unique = vec![];
        'outer: for lsid in lsids.iter() {
            for u in &unique {
                if lsid == *u {
                    continue 'outer;
                }
            }
            unique.push(lsid);
        }

        min_lsids = std::cmp::min(min_lsids, unique.len());
        max_lsids = std::cmp::max(max_lsids, unique.len());
        lsids.clear();
    }
    assert!(
        min_lsids <= 2,
        "min lsids is {}, expected <= 2 (max is {})",
        min_lsids,
        max_lsids,
    );
    assert!(
        max_lsids < 7,
        "max lsids is {}, expected < 7 (min is {})",
        max_lsids,
        min_lsids,
    );
}
