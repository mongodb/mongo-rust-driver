#[path = "sessions/sessions_not_supported.rs"]
mod sessions_not_supported_skip_local; // requires mongocryptd

use std::{
    future::IntoFuture,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::TryStreamExt;
use futures_util::{future::try_join_all, FutureExt};

use crate::{
    bson::{doc, Document, Timestamp},
    error::{ErrorKind, Result},
    event::{
        command::{CommandEvent, CommandStartedEvent},
        sdam::SdamEvent,
    },
    test::{
        get_client_options,
        log_uncaptured,
        server_version_gte,
        server_version_lt,
        spec::unified_runner::run_unified_tests,
        topology_is_load_balanced,
        topology_is_replica_set,
        topology_is_sharded,
        Event,
    },
    Client,
};

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    let mut skipped_files = vec![];
    if topology_is_sharded().await && server_version_gte(7, 0).await {
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
        other => panic!("expected InvalidArgument error, got {other:?}"),
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
        other => panic!("expected InvalidArgument error, got {other:?}"),
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
        "min lsids is {min_lsids}, expected <= 2 (max is {max_lsids})",
    );
    assert!(
        max_lsids < 7,
        "max lsids is {max_lsids}, expected < 7 (min is {min_lsids})",
    );
}

// Prose tests 18 and 19 in sessions_not_supported_skip_local module

// Sessions prose test 20
#[tokio::test]
async fn no_cluster_time_in_sdam() {
    if topology_is_load_balanced().await {
        log_uncaptured("Skipping no_cluster_time_in_sdam: load-balanced topology");
        return;
    }
    let mut options = get_client_options().await.clone();
    options.direct_connection = Some(true);
    options.hosts.drain(1..);
    let heartbeat_freq = Duration::from_millis(10);
    options.heartbeat_freq = Some(heartbeat_freq);
    let c1 = Client::for_test()
        .options(options)
        .min_heartbeat_freq(heartbeat_freq)
        .monitor_events()
        .await;

    // Send a ping on c1
    let cluster_time = c1
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .unwrap()
        .get("$clusterTime")
        .cloned();

    // Send a write on c2
    let c2 = Client::for_test().await;
    c2.database("test")
        .collection::<Document>("test")
        .insert_one(doc! {"advance": "$clusterTime"})
        .await
        .unwrap();

    // Wait for the next (heartbeat started, heartbeat succeeded) event pair on c1
    let mut events = c1.events.stream();
    const TIMEOUT: Duration = Duration::from_secs(1);
    crate::runtime::timeout(TIMEOUT, async {
        loop {
            // Find a started event...
            let _started = events
                .next_match(TIMEOUT, |ev| {
                    matches!(ev, Event::Sdam(SdamEvent::ServerHeartbeatStarted(_)))
                })
                .await
                .unwrap();
            // ... and the next heartbeat event after that ...
            let next_hb = events
                .next_map(TIMEOUT, |ev| match ev {
                    Event::Sdam(hb @ SdamEvent::ServerHeartbeatStarted(_)) => Some(hb),
                    Event::Sdam(hb @ SdamEvent::ServerHeartbeatFailed(_)) => Some(hb),
                    Event::Sdam(hb @ SdamEvent::ServerHeartbeatSucceeded(_)) => Some(hb),
                    _ => None,
                })
                .await
                .unwrap();
            // ... and see if it was a succeeded event.
            if matches!(next_hb, SdamEvent::ServerHeartbeatSucceeded(_)) {
                break;
            }
        }
    })
    .await
    .unwrap();

    // Send another ping
    let mut events = c1.events.stream();
    c1.database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .unwrap();
    let (start, _succeded) = events
        .next_successful_command_execution(TIMEOUT, "ping")
        .await
        .unwrap();

    // Assert that the cluster time hasn't changed
    assert_eq!(cluster_time.as_ref(), start.command.get("$clusterTime"));
}

// Sessions prose test 21
#[tokio::test]
async fn snapshot_time_and_snapshot_false_disallowed() {
    if server_version_lt(5, 0).await
        || !(topology_is_replica_set().await || topology_is_sharded().await)
    {
        log_uncaptured(
            "skipping snapshot_time_and_snapshot_false_disallowed: requires 5.0+ replica set or \
             sharded cluster",
        );
        return;
    }

    let client = Client::for_test().await;
    let error = client
        .start_session()
        .snapshot(false)
        .snapshot_time(Timestamp {
            time: 0,
            increment: 0,
        })
        .await
        .unwrap_err();
    assert!(matches!(*error.kind, ErrorKind::InvalidArgument { .. }));
}

// Sessions prose test 22
#[tokio::test]
async fn cannot_call_snapshot_time_on_non_snapshot_session() {
    if server_version_lt(5, 0).await
        || !(topology_is_replica_set().await || topology_is_sharded().await)
    {
        log_uncaptured(
            "skipping cannot_call_snapshot_time_on_non_snapshot_session: requires 5.0+ replica \
             set or sharded cluster",
        );
        return;
    }

    let client = Client::for_test().await;
    let session = client.start_session().snapshot(false).await.unwrap();
    assert!(session.snapshot_time().is_none());
}

// Sessions prose test 23 skipped: "Drivers MAY skip this test if they choose to expose a read-only
// `snapshotTime` property using an accessor method only."
