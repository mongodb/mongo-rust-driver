use crate::{
    bson::doc,
    test::{
        util::{TracingEvent, TracingEventValue, TracingHandler},
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
        LOCK,
    },
    trace::{truncate_on_char_boundary, COMMAND_TRACING_EVENT_TARGET},
};
use std::time::Duration;
use tokio::sync::RwLockWriteGuard;

#[cfg(feature = "tracing-unstable")]
#[test]
fn tracing_truncation() {
    let single_emoji = String::from("🤔");
    let two_emoji = String::from("🤔🤔");

    let mut s = two_emoji.clone();
    assert_eq!(s.len(), 8);

    // start of string is a boundary, so we should truncate there
    truncate_on_char_boundary(&mut s, 0);
    assert_eq!(s, String::from(""));

    // we should "round up" to the end of the first emoji
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 1);
    assert_eq!(s, single_emoji);

    // 4 is a boundary, so we should truncate there
    s = two_emoji.clone();
    truncate_on_char_boundary(&mut s, 4);
    assert_eq!(s, single_emoji);

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

#[cfg(feature = "tracing-unstable")]
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_tracing_command_success() {
    let _lock_guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let handler = TracingHandler::new(tracing::Level::DEBUG);
    let _handler_guard = handler.set_as_default();

    let client = TestClient::new().await;
    let mut subscriber = handler.subscribe();

    let coll = client.database("tracing").collection("test");
    coll.insert_one(doc! { "x" : 1 }, None)
        .await
        .expect("insert_one should succeed");

    let events = subscriber
        .collect_events(Duration::from_millis(500), |e| {
            e.target == COMMAND_TRACING_EVENT_TARGET
        })
        .await;
    for event in events.iter() {
        assert_eq!(event.level, tracing::Level::DEBUG);
        assert_eq!(event.target, COMMAND_TRACING_EVENT_TARGET);
    }
    let insert_events: Vec<&TracingEvent> = events
        .iter()
        .filter(|e| match e.fields.get("command_name") {
            Some(TracingEventValue::String(ref name)) => name == "insert",
            _ => panic!("event unexpectedly missing command name"),
        })
        .collect();
    assert_eq!(insert_events.len(), 2);

    let started = insert_events[0];
    match started.fields.get("message") {
        Some(TracingEventValue::String(ref msg)) => {
            assert_eq!(msg, "Command started");
        }
        _ => panic!("event unexpectedly missing command event message"),
    }

    let succeeded = insert_events[1];
    match succeeded.fields.get("message") {
        Some(TracingEventValue::String(ref msg)) => {
            assert_eq!(msg, "Command succeeded");
        }
        _ => panic!("event unexpectedly missing command event message"),
    }
}

#[cfg(feature = "tracing-unstable")]
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_tracing_command_failed() {
    let _lock_guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let client = TestClient::new().await;
    let fp = FailPoint::fail_command(
        &["insert"],
        FailPointMode::Times(1),
        FailCommandOptions::builder().error_code(11601).build(),
    );
    let _fp_guard = client
        .enable_failpoint(fp, None)
        .await
        .expect("enabling failpoint should succeed");

    let handler = TracingHandler::new(tracing::Level::DEBUG);
    let _handler_guard = handler.set_as_default();
    let mut subscriber = handler.subscribe();

    let coll = client.database("tracing").collection("test");
    coll.insert_one(doc! { "x" : 1 }, None)
        .await
        .expect_err("insert_one should fail due to failpoint");

    let events = subscriber
        .collect_events(Duration::from_millis(500), |e| {
            e.target == COMMAND_TRACING_EVENT_TARGET
        })
        .await;
    let insert_events: Vec<&TracingEvent> = events
        .iter()
        .filter(|e| match e.fields.get("command_name") {
            Some(TracingEventValue::String(ref name)) => name == "insert",
            _ => panic!("event unexpectedly missing command name"),
        })
        .collect();
    assert_eq!(insert_events.len(), 2);

    let started = insert_events[0];
    match started.fields.get("message") {
        Some(TracingEventValue::String(ref msg)) => {
            assert_eq!(msg, "Command started");
        }
        _ => panic!("event unexpectedly missing command event message"),
    }

    let failed = insert_events[1];
    match failed.fields.get("message") {
        Some(TracingEventValue::String(ref msg)) => {
            assert_eq!(msg, "Command failed");
        }
        _ => panic!("event unexpectedly missing command event message"),
    }

    match failed.fields.get("failure") {
        Some(TracingEventValue::String(ref failure)) => {
            crate::test::log_uncaptured(failure);
        }
        _ => panic!("event unexpectedly missing command failure description"),
    }
}
