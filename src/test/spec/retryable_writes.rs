use std::{collections::HashSet, sync::Arc, time::Duration};

use crate::bson::Bson;
use tokio::sync::Mutex;

use crate::{
    bson::{doc, Document},
    error::{ErrorKind, Result, RETRYABLE_WRITE_ERROR},
    event::{
        cmap::{CmapEvent, ConnectionCheckoutFailedReason},
        command::CommandEvent,
    },
    runtime::{self, spawn, AcknowledgedMessage, AsyncJoinHandle},
    test::{
        block_connection_supported,
        fail_command_supported,
        get_client_options,
        log_uncaptured,
        server_version_gt,
        server_version_lt,
        spec::unified_runner::run_unified_tests,
        topology_is_load_balanced,
        topology_is_replica_set,
        topology_is_sharded,
        topology_is_standalone,
        util::{
            event_buffer::EventBuffer,
            fail_point::{FailPoint, FailPointMode},
        },
        Event,
        TestClient,
    },
    Client,
};

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    run_unified_tests(&["retryable-writes", "unified"])
        // The driver does not support unacknowledged writes
        .skip_files(&["unacknowledged-write-concern.json"])
        .await;
}

#[tokio::test]
#[function_name::named]
async fn mmapv1_error_raised() {
    if server_version_gt(4, 0).await || !topology_is_replica_set().await {
        log_uncaptured("skipping mmapv1_error_raised due to test topology");
        return;
    }

    let client = Client::for_test().await;
    let coll = client.init_db_and_coll(function_name!(), "coll").await;

    let server_status = client
        .database(function_name!())
        .run_command(doc! { "serverStatus": 1 })
        .await
        .unwrap();
    let name = server_status
        .get_document("storageEngine")
        .unwrap()
        .get_str("name")
        .unwrap();
    if name != "mmapv1" {
        log_uncaptured("skipping mmapv1_error_raised due to unsupported storage engine");
        return;
    }

    let err = coll.insert_one(doc! { "x": 1 }).await.unwrap_err();
    match *err.kind {
        ErrorKind::Command(err) => {
            assert_eq!(
                err.message,
                "This MongoDB deployment does not support retryable writes. Please add \
                 retryWrites=false to your connection string."
            );
        }
        e => panic!("expected command error, got: {:?}", e),
    }
}

#[tokio::test]
async fn label_not_added_first_read_error() {
    label_not_added(false).await;
}

#[tokio::test]
async fn label_not_added_second_read_error() {
    label_not_added(true).await;
}

#[function_name::named]
async fn label_not_added(retry_reads: bool) {
    if !fail_command_supported().await {
        log_uncaptured("skipping label_not_added due to fail command unsupported");
        return;
    }

    let mut options = get_client_options().await.clone();
    options.retry_reads = Some(retry_reads);
    let client = Client::for_test()
        .options(options)
        .use_single_mongos()
        .await;

    let coll = client
        .init_db_and_coll(&format!("{}{}", function_name!(), retry_reads), "coll")
        .await;

    let failpoint = doc! {
        "configureFailPoint": "failCommand",
        "mode": { "times": if retry_reads { 2 } else { 1 } },
        "data": {
            "failCommands": ["find"],
            "errorCode": 11600
        }
    };
    client
        .database("admin")
        .run_command(failpoint)
        .await
        .unwrap();

    let err = coll.find(doc! {}).await.unwrap_err();

    assert!(!err.contains_label("RetryableWriteError"));
}

/// Prose test from retryable writes spec verifying that PoolClearedErrors are retried.
#[tokio::test(flavor = "multi_thread")]
async fn retry_write_pool_cleared() {
    if topology_is_standalone().await {
        log_uncaptured("skipping retry_write_pool_cleared due standalone topology");
        return;
    }
    if topology_is_load_balanced().await {
        log_uncaptured("skipping retry_write_pool_cleared due to load-balanced topology");
        return;
    }
    if !block_connection_supported().await {
        log_uncaptured(
            "skipping retry_write_pool_cleared due to blockConnection not being supported",
        );
        return;
    }

    let buffer = EventBuffer::new();

    let mut client_options = get_client_options().await.clone();
    client_options.retry_writes = Some(true);
    client_options.max_pool_size = Some(1);
    client_options.cmap_event_handler = Some(buffer.handler());
    client_options.command_event_handler = Some(buffer.handler());
    // on sharded clusters, ensure only a single mongos is used
    if client_options.repl_set_name.is_none() {
        client_options.hosts.drain(1..);
    }

    let client = Client::for_test().options(client_options.clone()).await;

    let collection = client
        .database("retry_write_pool_cleared")
        .collection("retry_write_pool_cleared");

    let fail_point = FailPoint::fail_command(&["insert"], FailPointMode::Times(1))
        .error_code(91)
        .block_connection(Duration::from_secs(1))
        .error_labels(vec![RETRYABLE_WRITE_ERROR]);
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    let mut event_stream = buffer.stream();

    let mut tasks: Vec<AsyncJoinHandle<_>> = Vec::new();
    for _ in 0..2 {
        let coll = collection.clone();
        let task = runtime::spawn(async move { coll.insert_one(doc! {}).await });
        tasks.push(task);
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .expect("all should succeeed");

    let _ = event_stream
        .next_match(Duration::from_millis(500), |event| {
            matches!(event, Event::Cmap(CmapEvent::ConnectionCheckedOut(_)))
        })
        .await
        .expect("first checkout should succeed");

    let _ = event_stream
        .next_match(Duration::from_millis(500), |event| {
            matches!(event, Event::Cmap(CmapEvent::PoolCleared(_)))
        })
        .await
        .expect("pool clear should occur");

    let next_cmap_events = event_stream
        .collect(Duration::from_millis(1000), |event| {
            matches!(event, Event::Cmap(_))
        })
        .await;

    if !next_cmap_events.iter().any(|event| match event {
        Event::Cmap(CmapEvent::ConnectionCheckoutFailed(e)) => {
            matches!(e.reason, ConnectionCheckoutFailedReason::ConnectionError)
        }
        _ => false,
    }) {
        panic!(
            "Expected second checkout to fail, but no ConnectionCheckoutFailed event observed. \
             CMAP events:\n{:?}",
            next_cmap_events
        );
    }

    assert_eq!(buffer.get_command_started_events(&["insert"]).len(), 3);
}

/// Prose test from retryable writes spec verifying that the original error is returned after
/// encountering a WriteConcernError with a RetryableWriteError label.
#[tokio::test(flavor = "multi_thread")]
async fn retry_write_retryable_write_error() {
    if !topology_is_replica_set().await || server_version_lt(6, 0).await {
        log_uncaptured("skipping retry_write_retryable_write_error: invalid topology");
        return;
    }

    let mut client_options = get_client_options().await.clone();
    client_options.retry_writes = Some(true);
    let (event_tx, event_rx) = tokio::sync::mpsc::channel::<AcknowledgedMessage<CommandEvent>>(1);
    // The listener needs to be active on client startup, but also needs a handle to the client
    // itself for the trigger action.
    let listener_client: Arc<Mutex<Option<TestClient>>> = Arc::new(Mutex::new(None));
    // Set up event listener
    let (fp_tx, mut fp_rx) = tokio::sync::mpsc::unbounded_channel();
    {
        let client = listener_client.clone();
        let mut event_rx = event_rx;
        let fp_tx = fp_tx.clone();
        // Spawn a task to watch the event channel
        spawn(async move {
            while let Some(msg) = event_rx.recv().await {
                if let CommandEvent::Succeeded(ev) = &*msg {
                    if let Some(Bson::Document(wc_err)) = ev.reply.get("writeConcernError") {
                        if ev.command_name == "insert"
                            && wc_err.get_i32("code").is_ok_and(|c| c == 91)
                        {
                            // Spawn a new task so events continue to process
                            let client = client.clone();
                            let fp_tx = fp_tx.clone();
                            spawn(async move {
                                // Enable the failpoint.
                                let fp_guard = {
                                    let client = client.lock().await;
                                    let fail_point = FailPoint::fail_command(
                                        &["insert"],
                                        FailPointMode::Times(1),
                                    )
                                    .error_code(10107)
                                    .error_labels(vec!["RetryableWriteError", "NoWritesPerformed"]);
                                    client
                                        .as_ref()
                                        .unwrap()
                                        .enable_fail_point(fail_point)
                                        .await
                                        .unwrap()
                                };
                                fp_tx.send(fp_guard).unwrap();
                                // Defer acknowledging the message until the failpoint has been set
                                // up so the retry hits it.
                                msg.acknowledge(());
                            });
                        }
                    }
                }
            }
        });
    }
    client_options.test_options_mut().async_event_listener = Some(event_tx);
    let client = Client::for_test().options(client_options).await;
    *listener_client.lock().await = Some(client.clone());

    let fail_point = FailPoint::fail_command(&["insert"], FailPointMode::Times(1))
        .write_concern_error(doc! {
            "code": 91,
            "errorLabels": ["RetryableWriteError"],
        });
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    let result = client
        .database("test")
        .collection::<Document>("test")
        .insert_one(doc! { "hello": "there" })
        .await;
    assert_eq!(result.unwrap_err().code(), Some(91));

    // Consume failpoint guard.
    let _ = fp_rx.recv().await;
}

// Test that in a sharded cluster writes are retried on a different mongos if one available
#[tokio::test(flavor = "multi_thread")]
async fn retry_write_different_mongos() {
    if !fail_command_supported().await {
        log_uncaptured("skipping retry_write_different_mongos: requires failCommand");
        return;
    }
    let mut client_options = get_client_options().await.clone();
    if !(topology_is_sharded().await && client_options.hosts.len() >= 2) {
        log_uncaptured(
            "skipping retry_write_different_mongos: requires sharded cluster with at least two \
             hosts",
        );
        return;
    }
    client_options.hosts.drain(2..);
    client_options.retry_writes = Some(true);
    println!("\nstart retry_write_different_mongos");

    let mut guards = vec![];
    for ix in [0, 1] {
        let mut opts = client_options.clone();
        opts.hosts.remove(ix);
        opts.direct_connection = Some(true);
        let client = Client::for_test().options(opts).await;

        let fail_point = FailPoint::fail_command(&["insert"], FailPointMode::Times(1))
            .error_code(6)
            .error_labels(vec![RETRYABLE_WRITE_ERROR])
            .close_connection(true);
        guards.push(client.enable_fail_point(fail_point).await.unwrap());
    }

    let client = Client::for_test()
        .options(client_options)
        .monitor_events()
        .await;
    let result = client
        .database("test")
        .collection::<crate::bson::Document>("retry_write_different_mongos")
        .insert_one(doc! {})
        .await;
    assert!(result.is_err());
    let events = client.events.get_command_events(&["insert"]);
    assert!(
        matches!(
            &events[..],
            &[
                CommandEvent::Started(_),
                CommandEvent::Failed(_),
                CommandEvent::Started(_),
                CommandEvent::Failed(_),
            ]
        ),
        "unexpected events: {:#?}",
        events,
    );
    let mongos_addresses: HashSet<_> = events
        .iter()
        .filter_map(|event| {
            if let CommandEvent::Failed(failed) = event {
                Some(&failed.connection.address)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        mongos_addresses.len(),
        2,
        "Failed commands did not occur on two different mongos instances"
    );
    println!("end retry_write_different_mongos\n");

    drop(guards); // enforce lifetime
}

// Retryable Reads Are Retried on the Same mongos if No Others are Available
#[tokio::test(flavor = "multi_thread")]
async fn retry_write_same_mongos() {
    if !fail_command_supported().await {
        log_uncaptured("skipping retry_write_same_mongos: requires failCommand");
        return;
    }
    if !topology_is_sharded().await {
        log_uncaptured("skipping retry_write_same_mongos: requires sharded cluster");
        return;
    }

    println!("\nstart retry_write_same_mongos");
    let mut client_options = get_client_options().await.clone();
    client_options.hosts.drain(1..);
    client_options.retry_writes = Some(true);
    let fp_guard = {
        let mut client_options = client_options.clone();
        client_options.direct_connection = Some(true);
        let client = Client::for_test().options(client_options).await;

        let fail_point = FailPoint::fail_command(&["insert"], FailPointMode::Times(1))
            .error_code(6)
            .error_labels(vec![RETRYABLE_WRITE_ERROR]);
        client.enable_fail_point(fail_point).await.unwrap()
    };

    client_options.direct_connection = Some(false);
    let client = Client::for_test()
        .options(client_options)
        .monitor_events()
        .await;
    let result = client
        .database("test")
        .collection::<crate::bson::Document>("retry_write_same_mongos")
        .insert_one(doc! {})
        .await;
    assert!(result.is_ok(), "{:?}", result);
    let events = client.events.get_command_events(&["insert"]);
    assert!(
        matches!(
            &events[..],
            &[
                CommandEvent::Started(_),
                CommandEvent::Failed(_),
                CommandEvent::Started(_),
                CommandEvent::Succeeded(_),
            ]
        ),
        "unexpected events: {:#?}",
        events,
    );
    let mongos_addresses: HashSet<_> = events
        .iter()
        .filter_map(|event| {
            if let CommandEvent::Failed(failed) = event {
                Some(&failed.connection.address)
            } else if let CommandEvent::Succeeded(succeeded) = event {
                Some(&succeeded.connection.address)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        mongos_addresses.len(),
        1,
        "Failed commands did not occur on the same mongos instance"
    );
    println!("end retry_write_same_mongos\n");

    drop(fp_guard); // enforce lifetime
}
