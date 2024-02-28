mod test_file;

use std::{sync::Arc, time::Duration};

use bson::Bson;
use futures::stream::TryStreamExt;
use semver::VersionReq;
use tokio::sync::Mutex;

use test_file::{TestFile, TestResult};

use crate::{
    bson::{doc, Document},
    error::{ErrorKind, Result, RETRYABLE_WRITE_ERROR},
    event::{
        cmap::{CmapEvent, ConnectionCheckoutFailedReason},
        command::CommandEvent,
    },
    options::{ClientOptions, FindOptions, InsertManyOptions},
    runtime,
    runtime::{spawn, AcknowledgedMessage, AsyncJoinHandle},
    sdam::MIN_HEARTBEAT_FREQUENCY,
    test::{
        assert_matches,
        get_client_options,
        log_uncaptured,
        run_spec_test,
        spec::unified_runner::run_unified_tests,
        util::get_default_name,
        Event,
        EventClient,
        EventHandler,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
    },
    Client,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    run_unified_tests(&["retryable-writes", "unified"]).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_legacy() {
    async fn run_test(test_file: TestFile) {
        for mut test_case in test_file.tests {
            if test_case.operation.name == "bulkWrite" {
                continue;
            }
            let mut options = test_case.client_options.unwrap_or_default();
            options.hosts = get_client_options().await.hosts.clone();
            if options.heartbeat_freq.is_none() {
                options.heartbeat_freq = Some(MIN_HEARTBEAT_FREQUENCY);
            }

            let client = EventClient::with_additional_options(
                options,
                Some(Duration::from_millis(50)),
                test_case.use_multiple_mongoses,
                None,
            )
            .await;

            if let Some(ref run_on) = test_file.run_on {
                let can_run_on = run_on.iter().any(|run_on| run_on.can_run_on(&client));
                if !can_run_on {
                    log_uncaptured(format!("Skipping {}", test_case.description));
                    continue;
                }
            }

            let db_name = get_default_name(&test_case.description);
            let coll_name = "coll";

            let coll = client.init_db_and_coll(&db_name, coll_name).await;

            if !test_file.data.is_empty() {
                coll.insert_many(test_file.data.clone(), None)
                    .await
                    .expect(&test_case.description);
            }

            let _fp_guard = if let Some(ref mut fail_point) = test_case.fail_point {
                Some(fail_point.enable(&client, None).await.unwrap_or_else(|e| {
                    panic!(
                        "{}: error enabling failpoint: {:#?}",
                        test_case.description, e
                    )
                }))
            } else {
                None
            };

            let coll = client.database(&db_name).collection(coll_name);
            let result = test_case.operation.execute_on_collection(&coll, None).await;

            // Disable the failpoint, if any.
            drop(_fp_guard);

            if let Some(error) = test_case.outcome.error {
                assert_eq!(
                    error,
                    result.is_err(),
                    "{}: expected error: {}, got {:?}",
                    &test_case.description,
                    error,
                    result
                );
            }

            if let Some(expected_result) = test_case.outcome.result {
                match expected_result {
                    TestResult::Value(value) => {
                        let description = &test_case.description;
                        let result = result
                            .unwrap_or_else(|e| {
                                panic!(
                                    "{:?}: operation should succeed, got error: {}",
                                    description, e
                                )
                            })
                            .unwrap();
                        assert_matches(&result, &value, Some(description));
                    }
                    TestResult::Labels(expected_labels) => {
                        let error = result.expect_err(&format!(
                            "{:?}: operation should fail",
                            &test_case.description
                        ));

                        let description = &test_case.description;
                        if let Some(contain) = expected_labels.error_labels_contain {
                            contain.iter().for_each(|label| {
                                assert!(
                                    error.contains_label(label),
                                    "{}: error labels should include {}",
                                    description,
                                    label,
                                );
                            });
                        }

                        if let Some(omit) = expected_labels.error_labels_omit {
                            omit.iter().for_each(|label| {
                                assert!(
                                    !error.contains_label(label),
                                    "{}: error labels should not include {}",
                                    description,
                                    label,
                                );
                            });
                        }
                    }
                };
            }

            let coll_name = match test_case.outcome.collection.name {
                Some(name) => name,
                None => coll_name.to_string(),
            };

            let coll = client.get_coll(&db_name, &coll_name);
            let options = FindOptions::builder().sort(doc! { "_id": 1 }).build();
            let actual_data: Vec<Document> = coll
                .find(None, options)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(test_case.outcome.collection.data, actual_data);

            client.database(&db_name).drop().await.unwrap();
        }
    }

    run_spec_test(&["retryable-writes", "legacy"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn transaction_ids_excluded() {
    let client = EventClient::new().await;

    if !(client.is_replica_set() || client.is_sharded()) {
        log_uncaptured("skipping transaction_ids_excluded due to test topology");
        return;
    }

    let coll = client.init_db_and_coll(function_name!(), "coll").await;

    let excludes_txn_number = |command_name: &str| -> bool {
        let (started, _) = client.get_successful_command_execution(command_name);
        !started.command.contains_key("txnNumber")
    };

    coll.update_many(doc! {}, doc! { "$set": doc! { "x": 1 } })
        .await
        .unwrap();
    assert!(excludes_txn_number("update"));

    coll.delete_many(doc! {}).await.unwrap();
    assert!(excludes_txn_number("delete"));

    coll.aggregate(vec![
        doc! { "$match": doc! { "x": 1 } },
        doc! { "$out": "other_coll" },
    ])
    .await
    .unwrap();
    assert!(excludes_txn_number("aggregate"));

    let req = semver::VersionReq::parse(">=4.2").unwrap();
    if req.matches(&client.server_version) {
        coll.aggregate(vec![
            doc! { "$match": doc! { "x": 1 } },
            doc! { "$merge": "other_coll" },
        ])
        .await
        .unwrap();
        assert!(excludes_txn_number("aggregate"));
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn transaction_ids_included() {
    let client = EventClient::new().await;

    if !(client.is_replica_set() || client.is_sharded()) {
        log_uncaptured("skipping transaction_ids_included due to test topology");
        return;
    }

    let coll = client.init_db_and_coll(function_name!(), "coll").await;

    let includes_txn_number = |command_name: &str| -> bool {
        let (started, _) = client.get_successful_command_execution(command_name);
        started.command.contains_key("txnNumber")
    };

    coll.insert_one(doc! { "x": 1 }, None).await.unwrap();
    assert!(includes_txn_number("insert"));

    coll.update_one(doc! {}, doc! { "$set": doc! { "x": 1 } })
        .await
        .unwrap();
    assert!(includes_txn_number("update"));

    coll.replace_one(doc! {}, doc! { "x": 1 }, None)
        .await
        .unwrap();
    assert!(includes_txn_number("update"));

    coll.delete_one(doc! {}).await.unwrap();
    assert!(includes_txn_number("delete"));

    coll.find_one_and_delete(doc! {}, None).await.unwrap();
    assert!(includes_txn_number("findAndModify"));

    coll.find_one_and_replace(doc! {}, doc! { "x": 1 }, None)
        .await
        .unwrap();
    assert!(includes_txn_number("findAndModify"));

    coll.find_one_and_update(doc! {}, doc! { "$set": doc! { "x": 1 } }, None)
        .await
        .unwrap();
    assert!(includes_txn_number("findAndModify"));

    let options = InsertManyOptions::builder().ordered(true).build();
    coll.insert_many(vec![doc! { "x": 1 }], options)
        .await
        .unwrap();
    assert!(includes_txn_number("insert"));

    let options = InsertManyOptions::builder().ordered(false).build();
    coll.insert_many(vec![doc! { "x": 1 }], options)
        .await
        .unwrap();
    assert!(includes_txn_number("insert"));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn mmapv1_error_raised() {
    let client = TestClient::new().await;

    let req = semver::VersionReq::parse("<=4.0").unwrap();
    if !req.matches(&client.server_version) || !client.is_replica_set() {
        log_uncaptured("skipping mmapv1_error_raised due to test topology");
        return;
    }

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

    let err = coll.insert_one(doc! { "x": 1 }, None).await.unwrap_err();
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn label_not_added_first_read_error() {
    label_not_added(false).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn label_not_added_second_read_error() {
    label_not_added(true).await;
}

#[function_name::named]
async fn label_not_added(retry_reads: bool) {
    let options = ClientOptions::builder()
        .hosts(vec![])
        .retry_reads(retry_reads)
        .build();
    let client = TestClient::with_additional_options(Some(options)).await;

    // Configuring a failpoint is only supported on 4.0+ replica sets and 4.1.5+ sharded clusters.
    let req = VersionReq::parse(">=4.0").unwrap();
    let sharded_req = VersionReq::parse(">=4.1.5").unwrap();
    if client.is_sharded() && !sharded_req.matches(&client.server_version)
        || !req.matches(&client.server_version)
    {
        log_uncaptured(
            "skipping label_not_added due to unsupported replica set or sharded cluster version",
        );
        return;
    }

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

    let err = coll.find(doc! {}, None).await.unwrap_err();

    assert!(!err.contains_label("RetryableWriteError"));
}

/// Prose test from retryable writes spec verifying that PoolClearedErrors are retried.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn retry_write_pool_cleared() {
    let handler = Arc::new(EventHandler::new());

    let mut client_options = get_client_options().await.clone();
    client_options.retry_writes = Some(true);
    client_options.max_pool_size = Some(1);
    client_options.cmap_event_handler = Some(handler.clone().into());
    client_options.command_event_handler = Some(handler.clone().into());
    // on sharded clusters, ensure only a single mongos is used
    if client_options.repl_set_name.is_none() {
        client_options.hosts.drain(1..);
    }

    let client = TestClient::with_options(Some(client_options.clone())).await;
    if !client.supports_block_connection() {
        log_uncaptured(
            "skipping retry_write_pool_cleared due to blockConnection not being supported",
        );
        return;
    }

    if client.is_standalone() {
        log_uncaptured("skipping retry_write_pool_cleared due standalone topology");
        return;
    }

    if client.is_load_balanced() {
        log_uncaptured("skipping retry_write_pool_cleared due to load-balanced topology");
        return;
    }

    let collection = client
        .database("retry_write_pool_cleared")
        .collection("retry_write_pool_cleared");

    let options = FailCommandOptions::builder()
        .error_code(91)
        .block_connection(Duration::from_secs(1))
        .error_labels(vec![RETRYABLE_WRITE_ERROR.to_string()])
        .build();
    let failpoint = FailPoint::fail_command(&["insert"], FailPointMode::Times(1), Some(options));
    let _fp_guard = client.enable_failpoint(failpoint, None).await.unwrap();

    let mut subscriber = handler.subscribe();

    let mut tasks: Vec<AsyncJoinHandle<_>> = Vec::new();
    for _ in 0..2 {
        let coll = collection.clone();
        let task = runtime::spawn(async move { coll.insert_one(doc! {}, None).await });
        tasks.push(task);
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .expect("all should succeeed");

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

    assert_eq!(handler.get_command_started_events(&["insert"]).len(), 3);
}

/// Prose test from retryable writes spec verifying that the original error is returned after
/// encountering a WriteConcernError with a RetryableWriteError label.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn retry_write_retryable_write_error() {
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
                        if ev.command_name == "insert" && wc_err.get_i32("code") == Ok(91) {
                            // Spawn a new task so events continue to process
                            let client = client.clone();
                            let fp_tx = fp_tx.clone();
                            spawn(async move {
                                // Enable the failpoint.
                                let fp_guard = {
                                    let client = client.lock().await;
                                    FailPoint::fail_command(
                                        &["insert"],
                                        FailPointMode::Times(1),
                                        FailCommandOptions::builder()
                                            .error_code(10107)
                                            .error_labels(vec![
                                                "RetryableWriteError".to_string(),
                                                "NoWritesPerformed".to_string(),
                                            ])
                                            .build(),
                                    )
                                    .enable(client.as_ref().unwrap(), None)
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
    let client = Client::test_builder().options(client_options).build().await;
    *listener_client.lock().await = Some(client.clone());

    if !client.is_replica_set() || client.server_version_lt(6, 0) {
        log_uncaptured("skipping retry_write_retryable_write_error: invalid topology");
        return;
    }

    let _fp_guard = FailPoint::fail_command(
        &["insert"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .write_concern_error(doc! {
                "code": 91,
                "errorLabels": ["RetryableWriteError"],
            })
            .build(),
    )
    .enable(&client, None)
    .await
    .unwrap();

    let result = client
        .database("test")
        .collection::<Document>("test")
        .insert_one(doc! { "hello": "there" }, None)
        .await;
    assert_eq!(result.unwrap_err().code(), Some(91));

    // Consume failpoint guard.
    let _ = fp_rx.recv().await;
}

// Test that in a sharded cluster writes are retried on a different mongos if one available
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn retry_write_different_mongos() {
    let mut client_options = get_client_options().await.clone();
    if client_options.repl_set_name.is_some() || client_options.hosts.len() < 2 {
        log_uncaptured(
            "skipping retry_write_different_mongos: requires sharded cluster with at least two \
             hosts",
        );
        return;
    }
    client_options.hosts.drain(2..);
    client_options.retry_writes = Some(true);

    let mut guards = vec![];
    for ix in [0, 1] {
        let mut opts = client_options.clone();
        opts.hosts.remove(ix);
        opts.direct_connection = Some(true);
        let client = Client::test_builder().options(opts).build().await;
        if !client.supports_fail_command() {
            log_uncaptured("skipping retry_write_different_mongos: requires failCommand");
            return;
        }
        let fail_opts = FailCommandOptions::builder()
            .error_code(6)
            .error_labels(vec!["RetryableWriteError".to_string()])
            .close_connection(true)
            .build();
        let fp = FailPoint::fail_command(&["insert"], FailPointMode::Times(1), Some(fail_opts));
        guards.push(client.enable_failpoint(fp, None).await.unwrap());
    }

    let client = Client::test_builder()
        .options(client_options)
        .event_client()
        .build()
        .await;
    let result = client
        .database("test")
        .collection::<bson::Document>("retry_write_different_mongos")
        .insert_one(doc! {}, None)
        .await;
    assert!(result.is_err());
    let events = client.get_command_events(&["insert"]);
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
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn retry_write_same_mongos() {
    let init_client = Client::test_builder().build().await;
    if !init_client.supports_fail_command() {
        log_uncaptured("skipping retry_write_same_mongos: requires failCommand");
        return;
    }
    if !init_client.is_sharded() {
        log_uncaptured("skipping retry_write_same_mongos: requires sharded cluster");
        return;
    }

    let mut client_options = get_client_options().await.clone();
    client_options.hosts.drain(1..);
    client_options.retry_writes = Some(true);
    let fp_guard = {
        let mut client_options = client_options.clone();
        client_options.direct_connection = Some(true);
        let client = Client::test_builder().options(client_options).build().await;
        let fail_opts = FailCommandOptions::builder()
            .error_code(6)
            .error_labels(vec!["RetryableWriteError".to_string()])
            .close_connection(true)
            .build();
        let fp = FailPoint::fail_command(&["insert"], FailPointMode::Times(1), Some(fail_opts));
        client.enable_failpoint(fp, None).await.unwrap()
    };

    let client = Client::test_builder()
        .options(client_options)
        .event_client()
        .build()
        .await;
    let result = client
        .database("test")
        .collection::<bson::Document>("retry_write_same_mongos")
        .insert_one(doc! {}, None)
        .await;
    assert!(result.is_ok(), "{:?}", result);
    let events = client.get_command_events(&["insert"]);
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
