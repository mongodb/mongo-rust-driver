use std::{future::IntoFuture, sync::Arc, time::Duration};

use bson::doc;

use crate::{
    error::Result,
    event::{
        cmap::{CmapEvent, ConnectionCheckoutFailedReason},
        command::CommandEvent,
    },
    runtime::{self, AsyncJoinHandle},
    test::{
        get_client_options, log_uncaptured, spec::{unified_runner::run_unified_tests, v2_runner::run_v2_tests}, util::buffer::EventBuffer, Event, FailCommandOptions, FailPoint, FailPointMode, TestClient
    },
    Client,
};

#[tokio::test(flavor = "multi_thread")]
async fn run_legacy() {
    run_v2_tests(&["retryable-reads", "legacy"]).await;
}

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

    let client = TestClient::with_options(Some(client_options)).await;
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
    let options = FailCommandOptions::builder().close_connection(true).build();
    let failpoint = FailPoint::fail_command(&["find"], FailPointMode::Times(1), Some(options));
    let _fp_guard = client.enable_failpoint(failpoint, None).await.unwrap();

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
    let handler = EventBuffer::new();

    let mut client_options = get_client_options().await.clone();
    client_options.retry_reads = Some(true);
    client_options.max_pool_size = Some(1);
    client_options.cmap_event_handler = Some(handler.handler());
    client_options.command_event_handler = Some(handler.handler());
    // on sharded clusters, ensure only a single mongos is used
    if client_options.repl_set_name.is_none() {
        client_options.hosts.drain(1..);
    }

    let client = TestClient::with_options(Some(client_options.clone())).await;
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

    let options = FailCommandOptions::builder()
        .error_code(91)
        .block_connection(Duration::from_secs(1))
        .build();
    let failpoint = FailPoint::fail_command(&["find"], FailPointMode::Times(1), Some(options));
    let _fp_guard = client.enable_failpoint(failpoint, None).await.unwrap();

    let mut subscriber = handler.subscribe();

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

    let _ = subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::Cmap(CmapEvent::ConnectionCheckedOut(_)))
        })
        .await
        .expect("first checkout should succeed");

    let _ = subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::Cmap(CmapEvent::PoolCleared(_)))
        })
        .await
        .expect("pool clear should occur");

    let next_cmap_events = subscriber
        .collect_events(Duration::from_millis(1000), |event| match event {
            Event::Cmap(_) => true,
            _ => false,
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

    assert_eq!(handler.get_command_started_events(&["find"]).len(), 3);
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
        let fail_opts = FailCommandOptions::builder()
            .error_code(6)
            .close_connection(true)
            .build();
        let fp = FailPoint::fail_command(&["find"], FailPointMode::Times(1), Some(fail_opts));
        guards.push(client.enable_failpoint(fp, None).await.unwrap());
    }

    let client = Client::test_builder()
        .options(client_options)
        .event_client()
        .build()
        .await;
    let result = client
        .database("test")
        .collection::<bson::Document>("retry_read_different_mongos")
        .find(doc! {})
        .await;
    assert!(result.is_err());
    let mut events = client.events.clone();
    let events = events.get_command_events(&["find"]);
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
        let fail_opts = FailCommandOptions::builder()
            .error_code(6)
            .close_connection(true)
            .build();
        let fp = FailPoint::fail_command(&["find"], FailPointMode::Times(1), Some(fail_opts));
        client.enable_failpoint(fp, None).await.unwrap()
    };

    let client = Client::test_builder()
        .options(client_options)
        .event_client()
        .build()
        .await;
    let result = client
        .database("test")
        .collection::<bson::Document>("retry_read_same_mongos")
        .find(doc! {})
        .await;
    assert!(result.is_ok(), "{:?}", result);
    let mut events = client.events.clone();
    let events = events.get_command_events(&["find"]);
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
