use std::{future::IntoFuture, time::Duration};

use bson::doc;

use crate::{
    error::Result,
    event::{
        cmap::{CmapEvent, ConnectionCheckoutFailedReason},
        command::CommandEvent,
    },
    runtime::{self, AsyncJoinHandle},
    test::{
        get_client_options,
        log_uncaptured,
        spec::unified_runner::run_unified_tests,
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
    let mut client_options = get_client_options().await.clone();
    client_options.hosts.drain(1..);
    client_options.retry_reads = Some(true);
    client_options.max_pool_size = Some(1);

    let client = Client::test_builder().options(client_options).build().await;
    if !client.supports_fail_command() {
        log_uncaptured("skipping retry_releases_connection due to failCommand not being supported");
        return;
    }

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

    let client = Client::test_builder()
        .options(client_options.clone())
        .build()
        .await;
    if !client.supports_block_connection() {
        log_uncaptured(
            "skipping retry_read_pool_cleared due to blockConnection not being supported",
        );
        return;
    }
    if client.is_load_balanced() {
        log_uncaptured("skipping retry_read_pool_cleared due to load-balanced topology");
        return;
    }

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
    let mut client_options = get_client_options().await.clone();
    if client_options.repl_set_name.is_some() || client_options.hosts.len() < 2 {
        log_uncaptured(
            "skipping retry_read_different_mongos: requires sharded cluster with at least two \
             hosts",
        );
        return;
    }
    client_options.hosts.drain(2..);
    client_options.retry_reads = Some(true);

    let mut guards = vec![];
    for ix in [0, 1] {
        let mut opts = client_options.clone();
        opts.hosts.remove(ix);
        opts.direct_connection = Some(true);
        let client = Client::test_builder().options(opts).build().await;
        if !client.supports_fail_command() {
            log_uncaptured("skipping retry_read_different_mongos: requires failCommand");
            return;
        }

        let fail_point = FailPoint::fail_command(&["find"], FailPointMode::Times(1))
            .error_code(6)
            .close_connection(true);
        guards.push(client.enable_fail_point(fail_point).await.unwrap());
    }

    let client = Client::test_builder()
        .options(client_options)
        .monitor_events()
        .build()
        .await;
    let result = client
        .database("test")
        .collection::<bson::Document>("retry_read_different_mongos")
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

    drop(guards); // enforce lifetime
}

// Retryable Reads Are Retried on the Same mongos if No Others are Available
#[tokio::test(flavor = "multi_thread")]
async fn retry_read_same_mongos() {
    let init_client = Client::test_builder().build().await;
    if !init_client.supports_fail_command() {
        log_uncaptured("skipping retry_read_same_mongos: requires failCommand");
        return;
    }
    if !init_client.is_sharded() {
        log_uncaptured("skipping retry_read_same_mongos: requires sharded cluster");
        return;
    }

    let mut client_options = get_client_options().await.clone();
    client_options.hosts.drain(1..);
    client_options.retry_reads = Some(true);
    let fp_guard = {
        let mut client_options = client_options.clone();
        client_options.direct_connection = Some(true);
        let client = Client::test_builder().options(client_options).build().await;

        let fail_point = FailPoint::fail_command(&["find"], FailPointMode::Times(1))
            .error_code(6)
            .close_connection(true);
        client.enable_fail_point(fail_point).await.unwrap()
    };

    let client = Client::test_builder()
        .options(client_options)
        .monitor_events()
        .build()
        .await;
    let result = client
        .database("test")
        .collection::<bson::Document>("retry_read_same_mongos")
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

    drop(fp_guard); // enforce lifetime
}
