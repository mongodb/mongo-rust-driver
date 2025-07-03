use std::{future::IntoFuture, sync::Arc, time::Duration};

use crate::bson::doc;

use crate::{
    error::Result,
    event::{
        cmap::{CmapEvent, ConnectionCheckoutFailedReason},
        command::CommandEvent,
    },
    options::SelectionCriteria,
    runtime::{self, AsyncJoinHandle},
    test::{
        block_connection_supported,
        fail_command_supported,
        get_client_options,
        log_uncaptured,
        spec::unified_runner::run_unified_tests,
        topology_is_load_balanced,
        topology_is_sharded,
        util::{
            event_buffer::EventBuffer,
            fail_point::{FailPoint, FailPointMode},
        },
        Event,
    },
    Client,
};

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    run_unified_tests(&["retryable-reads", "unified"]).await;
}

/// Test ensures that the connection used in the first attempt of a retry is released back into the
/// pool before the second attempt.
#[tokio::test(flavor = "multi_thread")]
async fn retry_releases_connection() {
    if !fail_command_supported().await {
        log_uncaptured("skipping retry_releases_connection due to failCommand not being supported");
        return;
    }

    let mut client_options = get_client_options().await.clone();
    client_options.hosts.drain(1..);
    client_options.retry_reads = Some(true);
    client_options.max_pool_size = Some(1);

    let client = Client::for_test().options(client_options).await;

    let collection = client
        .database("retry_releases_connection")
        .collection("retry_releases_connection");
    collection.insert_one(doc! { "x": 1 }).await.unwrap();

    // Use a connection error to ensure streaming monitor checks get cancelled. Otherwise, we'd have
    // to wait for the entire heartbeatFrequencyMS before the find succeeds.
    let fail_point =
        FailPoint::fail_command(&["find"], FailPointMode::Times(1)).close_connection(true);
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    runtime::timeout(
        Duration::from_secs(1),
        collection.find_one(doc! {}).into_future(),
    )
    .await
    .expect("operation should not time out")
    .expect("find should succeed");
}

/// Prose test from retryable reads spec verifying that PoolClearedErrors are retried.
#[tokio::test(flavor = "multi_thread")]
async fn retry_read_pool_cleared() {
    if !block_connection_supported().await {
        log_uncaptured(
            "skipping retry_read_pool_cleared due to blockConnection not being supported",
        );
        return;
    }
    if topology_is_load_balanced().await {
        log_uncaptured("skipping retry_read_pool_cleared due to load-balanced topology");
        return;
    }

    let buffer = EventBuffer::new();

    let mut client_options = get_client_options().await.clone();
    client_options.retry_reads = Some(true);
    client_options.max_pool_size = Some(1);
    client_options.cmap_event_handler = Some(buffer.handler());
    client_options.command_event_handler = Some(buffer.handler());
    // on sharded clusters, ensure only a single mongos is used
    if client_options.repl_set_name.is_none() {
        client_options.hosts.drain(1..);
    }

    let client = Client::for_test().options(client_options.clone()).await;

    let collection = client
        .database("retry_read_pool_cleared")
        .collection("retry_read_pool_cleared");
    collection.insert_one(doc! { "x": 1 }).await.unwrap();

    let fail_point = FailPoint::fail_command(&["find"], FailPointMode::Times(1))
        .error_code(91)
        .block_connection(Duration::from_secs(1));
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    let mut event_stream = buffer.stream();

    let mut tasks: Vec<AsyncJoinHandle<_>> = Vec::new();
    for _ in 0..2 {
        let coll = collection.clone();
        let task = runtime::spawn(async move { coll.find_one(doc! {}).await });
        tasks.push(task);
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .expect("all should succeed");

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

    assert_eq!(buffer.get_command_started_events(&["find"]).len(), 3);
}

// Retryable Reads Are Retried on a Different mongos if One is Available
#[tokio::test(flavor = "multi_thread")]
async fn retry_read_different_mongos() {
    if !fail_command_supported().await {
        log_uncaptured("skipping retry_read_different_mongos: requires failCommand");
        return;
    }
    let mut client_options = get_client_options().await.clone();
    if !(topology_is_sharded().await && client_options.hosts.len() >= 2) {
        log_uncaptured(
            "skipping retry_read_different_mongos: requires sharded cluster with at least two \
             hosts",
        );
        return;
    }

    client_options.hosts.drain(2..);
    client_options.retry_reads = Some(true);

    let hosts = client_options.hosts.clone();
    let client = Client::for_test()
        .options(client_options)
        .monitor_events()
        .await;

    // NOTE: This test uses a single client to set failpoints on each mongos and run the find
    // operation. This avoids flakiness caused by a race between server discovery and server
    // selection.

    // When a client is first created, it initializes its view of the topology with all configured
    // mongos addresses, but marks each as Unknown until it completes the server discovery process
    // by sending and receiving "hello" messages Unknown servers are not eligible for server
    // selection.

    // Previously, we created a new client for each call to `enable_fail_point` and for the find
    // operation. Each new client restarted the discovery process, and sometimes had not yet marked
    // both mongos servers as usable, leading to test failures when the retry logic couldn't find a
    // second eligible server.

    // By reusing a single client, each `enable_fail_point` call forces discovery to complete for
    // the corresponding mongos. As a result, when the find operation runs, the client has a
    // fully discovered topology and can reliably select between both servers.
    let mut guards = Vec::new();
    for address in hosts {
        let address = address.clone();
        let fail_point = FailPoint::fail_command(&["find"], FailPointMode::Times(1))
            .error_code(6)
            .selection_criteria(SelectionCriteria::Predicate(Arc::new(move |info| {
                info.description.address == address
            })));
        guards.push(client.enable_fail_point(fail_point).await.unwrap());
    }

    let result = client
        .database("test")
        .collection::<crate::bson::Document>("retry_read_different_mongos")
        .find(doc! {})
        .await;
    assert!(result.is_err());
    let events = client.events.get_command_events(&["find"]);
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
    let first_failed = events[1].as_command_failed().unwrap();
    let first_address = &first_failed.connection.address;
    let second_failed = events[3].as_command_failed().unwrap();
    let second_address = &second_failed.connection.address;
    assert_ne!(
        first_address, second_address,
        "Failed commands did not occur on two different mongos instances"
    );

    drop(guards); // enforce lifetime
}

// Retryable Reads Are Retried on the Same mongos if No Others are Available
#[tokio::test(flavor = "multi_thread")]
async fn retry_read_same_mongos() {
    if !fail_command_supported().await {
        log_uncaptured("skipping retry_read_same_mongos: requires failCommand");
        return;
    }
    if !topology_is_sharded().await {
        log_uncaptured("skipping retry_read_same_mongos: requires sharded cluster");
        return;
    }

    let mut client_options = get_client_options().await.clone();
    client_options.hosts.drain(1..);
    client_options.retry_reads = Some(true);
    let fp_guard = {
        let mut client_options = client_options.clone();
        client_options.direct_connection = Some(true);
        let client = Client::for_test().options(client_options).await;

        let fail_point = FailPoint::fail_command(&["find"], FailPointMode::Times(1)).error_code(6);
        client.enable_fail_point(fail_point).await.unwrap()
    };

    client_options.direct_connection = Some(false);
    let client = Client::for_test()
        .options(client_options)
        .monitor_events()
        .await;
    let result = client
        .database("test")
        .collection::<crate::bson::Document>("retry_read_same_mongos")
        .find(doc! {})
        .await;
    assert!(result.is_ok(), "{:?}", result);
    let events = client.events.get_command_events(&["find"]);
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
    let first_failed = events[1].as_command_failed().unwrap();
    let first_address = &first_failed.connection.address;
    let second_failed = events[3].as_command_succeeded().unwrap();
    let second_address = &second_failed.connection.address;
    assert_eq!(
        first_address, second_address,
        "Failed command and retry did not occur on the same mongos instance",
    );

    drop(fp_guard); // enforce lifetime
}
