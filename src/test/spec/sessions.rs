#[path = "sessions/sessions_not_supported.rs"]
mod sessions_not_supported_skip_local; // requires mongocryptd

use std::{
    future::IntoFuture,
    sync::{Arc, Mutex},
};

use futures::TryStreamExt;
use futures_util::{future::try_join_all, FutureExt};

use crate::{
    bson::{doc, Document},
    error::{ErrorKind, Result},
    event::command::{CommandEvent, CommandStartedEvent},
    test::{get_client_options, spec::unified_runner::run_unified_tests},
    Client,
};

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    let mut skipped_files = vec![];
    let client = Client::for_test().await;
    if client.is_sharded() && client.server_version_gte(7, 0) {
        // TODO RUST-1666: unskip this file
        skipped_files.push("snapshot-sessions.json");
    }

    run_unified_tests(&["sessions"])
        .skip_files(&skipped_files)
        .await;
}

// Sessions prose test 1
#[tokio::test]
async fn snapshot_and_causal_consistency_are_mutually_exclusive() {
    let client = Client::for_test().await;
    assert!(client
        .start_session()
        .snapshot(true)
        .causal_consistency(true)
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
#[function_name::named]
async fn explicit_session_created_on_same_client() {
    let client0 = Client::for_test().await;
    let client1 = Client::for_test().await;

    let mut session0 = client0.start_session().await.unwrap();
    let mut session1 = client1.start_session().await.unwrap();

    let db = client0.database(function_name!());
    let err = db
        .list_collections()
        .session(&mut session1)
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
        .insert_one(doc! {})
        .session(&mut session0)
        .await
        .unwrap_err();
    match *err.kind {
        ErrorKind::InvalidArgument { message } => assert!(message.contains("session provided")),
        other => panic!("expected InvalidArgument error, got {:?}", other),
    }
}

// Sessions prose test 14
#[tokio::test]
async fn implicit_session_after_connection() {
    let lsids = Arc::new(Mutex::new(vec![]));
    let event_handler = {
        let lsids = Arc::clone(&lsids);
        crate::event::EventHandler::callback(move |ev: CommandEvent| {
            if let CommandEvent::Started(CommandStartedEvent { command, .. }) = ev {
                lsids
                    .lock()
                    .unwrap()
                    .push(command.get_document("lsid").unwrap().clone());
            }
        })
    };

    let mut min_lsids = usize::MAX;
    let mut max_lsids = 0usize;
    for _ in 0..5 {
        let client = {
            let mut options = get_client_options().await.clone();
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
        ops.push(
            coll.insert_one(doc! {})
                .into_future()
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            coll.delete_one(doc! {})
                .into_future()
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            coll.update_one(doc! {}, doc! { "$set": { "a": 1 } })
                .into_future()
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            coll.find_one_and_delete(doc! {})
                .into_future()
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            coll.find_one_and_update(doc! {}, doc! { "$set": { "a": 1 } })
                .into_future()
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            coll.find_one_and_replace(doc! {}, doc! { "a": 1 })
                .into_future()
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            async {
                let cursor = coll.find(doc! {}).await.unwrap();
                let r: Result<Vec<_>> = cursor.try_collect().await;
                r.map(|_| ())
            }
            .boxed(),
        );

        let _ = try_join_all(ops).await.unwrap();

        let mut lsids = lsids.lock().unwrap();
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
    // The spec says the minimum should be 1; however, the background async nature of the Rust
    // driver's session cleanup means that sometimes a session has not yet been returned to the pool
    // when the next one is checked out.
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

// Prose tests 18 and 19 in sessions_not_supported_skip_local module
