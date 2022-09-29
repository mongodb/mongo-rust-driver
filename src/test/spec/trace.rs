use crate::{
    bson::doc,
    coll::options::FindOptions,
    test::{
        log_uncaptured,
        run_spec_test_with_path,
        spec::run_unified_format_test,
        TestClient,
        CLIENT_OPTIONS,
        DEFAULT_GLOBAL_TRACING_HANDLER,
        LOCK,
        SERVER_API,
    },
    trace::{
        command::{truncate_on_char_boundary, DEFAULT_MAX_DOCUMENT_LENGTH_BYTES},
        COMMAND_TRACING_EVENT_TARGET,
    },
};
use std::{collections::HashMap, iter, time::Duration};

#[test]
fn tracing_truncation() {
    let two_emoji = String::from("🤔🤔");

    let mut s = two_emoji.clone();
    assert_eq!(s.len(), 8);

    // start of string is a boundary, so we should truncate there
    truncate_on_char_boundary(&mut s, 0);
    assert_eq!(s, String::from("..."));

    // we should "round up" to the end of the first emoji
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 1);
    assert_eq!(s, String::from("🤔..."));

    // 4 is a boundary, so we should truncate there
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 4);
    assert_eq!(s, String::from("🤔..."));

    // we should round up to the full string
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 5);
    assert_eq!(s, two_emoji);

    // end of string is a boundary, so we should truncate there
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 8);
    assert_eq!(s, two_emoji);

    // we should get the full string back if the new length is longer than the original
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 10);
    assert_eq!(s, two_emoji);
}

/// Prose test 1: Default truncation limit
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_logging_truncation_default_limit() {
    let _guard = LOCK.run_exclusively().await;
    let client = TestClient::new().await;
    let coll = client.init_db_and_coll("tracing_test", "truncation").await;

    let _levels_guard = DEFAULT_GLOBAL_TRACING_HANDLER.set_levels(HashMap::from([(
        COMMAND_TRACING_EVENT_TARGET.to_string(),
        tracing::Level::DEBUG,
    )]));
    let mut tracing_subscriber = DEFAULT_GLOBAL_TRACING_HANDLER.subscribe();

    let docs = iter::repeat(doc! { "x": "y" }).take(100);
    coll.insert_many(docs, None)
        .await
        .expect("insert many should succeed");

    let events = tracing_subscriber
        .collect_events(Duration::from_millis(500), |_| true)
        .await;
    assert_eq!(events.len(), 2);

    let started = &events[0];
    let command = started.get_value_as_string("command");
    assert_eq!(command.len(), DEFAULT_MAX_DOCUMENT_LENGTH_BYTES + 3); // +3 for trailing "..."

    let succeeded = &events[1];
    let reply = succeeded.get_value_as_string("reply");
    assert!(reply.len() <= DEFAULT_MAX_DOCUMENT_LENGTH_BYTES + 3); // +3 for trailing "..."

    coll.find(None, None).await.expect("find should succeed");
    let succeeded = tracing_subscriber
        .wait_for_event(Duration::from_millis(500), |e| {
            e.get_value_as_string("message") == "Command succeeded"
        })
        .await
        .unwrap();
    let reply = succeeded.get_value_as_string("reply");
    assert_eq!(reply.len(), DEFAULT_MAX_DOCUMENT_LENGTH_BYTES + 3); // +3 for trailing "..."
}

/// Prose test 2: explicitly configured truncation limit
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_logging_truncation_explicit_limit() {
    let _guard = LOCK.run_exclusively().await;

    let mut client_opts = CLIENT_OPTIONS.get().await.clone();
    client_opts.tracing_max_document_length_bytes = Some(5);
    let client = TestClient::with_options(Some(client_opts)).await;

    let _levels_guard = DEFAULT_GLOBAL_TRACING_HANDLER.set_levels(HashMap::from([(
        COMMAND_TRACING_EVENT_TARGET.to_string(),
        tracing::Level::DEBUG,
    )]));
    let mut tracing_subscriber = DEFAULT_GLOBAL_TRACING_HANDLER.subscribe();

    client
        .database("tracing_test")
        .run_command(doc! { "hello" : "true" }, None)
        .await
        .expect("hello command should succeed");

    let events = tracing_subscriber
        .collect_events(Duration::from_millis(500), |_| true)
        .await;
    assert_eq!(events.len(), 2);

    let started = &events[0];
    let command = started.get_value_as_string("command");
    assert_eq!(command.len(), 8); // 5 + 3 for trailing "..."

    let succeeded = &events[1];
    let reply = succeeded.get_value_as_string("reply");
    assert_eq!(reply.len(), 8); // 5 + 3 for trailing "..."

    // TODO RUST-1405: when we expose the full server reply for command errors, we should confirm
    // that gets correctly truncated in command failed events here as well.
}

/// Prose test 3: mid-codepoint truncation
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_logging_truncation_mid_codepoint() {
    let _guard = LOCK.run_exclusively().await;

    let mut client_opts = CLIENT_OPTIONS.get().await.clone();
    client_opts.tracing_max_document_length_bytes = Some(215);
    let client = TestClient::with_options(Some(client_opts)).await;
    // On non-standalone topologies the command includes a clusterTime and so gets truncated
    // differently.
    if !client.is_standalone() {
        log_uncaptured("Skipping test due to incompatible topology type");
        return;
    }
    // Truncation happens differently when a server API version is included in the command.
    if SERVER_API.is_some() {
        log_uncaptured("Skipping test due to server API version being specified");
        return;
    }

    let coll = client.init_db_and_coll("tracing_test", "truncation").await;

    let _levels_guard = DEFAULT_GLOBAL_TRACING_HANDLER.set_levels(HashMap::from([(
        COMMAND_TRACING_EVENT_TARGET.to_string(),
        tracing::Level::DEBUG,
    )]));
    let mut tracing_subscriber = DEFAULT_GLOBAL_TRACING_HANDLER.subscribe();

    let docs = iter::repeat(doc! { "🤔": "🤔🤔🤔🤔🤔🤔" }).take(10);
    coll.insert_many(docs, None)
        .await
        .expect("insert many should succeed");

    let started = tracing_subscriber
        .wait_for_event(Duration::from_millis(500), |e| {
            e.get_value_as_string("message") == "Command started"
        })
        .await
        .unwrap();

    let command = started.get_value_as_string("command");

    // 215 falls in the middle of an emoji (each is 4 bytes), so we should round up to 218, + 3 for
    // trailing "..."
    assert_eq!(command.len(), 221);

    let find_options = FindOptions::builder()
        .projection(doc! { "_id": 0, "🤔": 1 })
        .build();
    coll.find(None, find_options)
        .await
        .expect("find should succeed");
    let succeeded = tracing_subscriber
        .wait_for_event(Duration::from_millis(500), |e| {
            e.get_value_as_string("message") == "Command succeeded"
                && e.get_value_as_string("command_name") == "find"
        })
        .await
        .unwrap();
    let reply = succeeded.get_value_as_string("reply");
    // 215 falls in the middle of an emoji (each is 4 bytes), so we should round up to 218, + 3 for
    // trailing "..."
    assert_eq!(reply.len(), 221);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_logging_unified() {
    let _guard = LOCK.run_exclusively().await;
    run_spec_test_with_path(
        &["command-logging-and-monitoring", "logging"],
        run_unified_format_test,
    )
    .await;
}
