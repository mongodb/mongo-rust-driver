use std::time::Duration;

use crate::{
    bson::doc,
    operation::{
        test::{self, handle_response_test},
        Find,
    },
    options::{CursorType, FindOptions},
    Namespace,
};

#[test]
fn op_selection_criteria() {
    test::op_selection_criteria(|selection_criteria| {
        let options = FindOptions {
            selection_criteria,
            ..Default::default()
        };
        Find::new(Namespace::empty(), None, Some(options))
    });
}

fn verify_max_await_time(max_await_time: Option<Duration>, cursor_type: Option<CursorType>) {
    let ns = Namespace::empty();
    let find = Find::new(
        ns,
        None,
        Some(FindOptions {
            cursor_type,
            max_await_time,
            ..Default::default()
        }),
    );

    let spec = handle_response_test(
        &find,
        doc! {
            "cursor": {
                "id": 123,
                "ns": "a.b",
                "firstBatch": [],
            },
            "ok": 1
        },
    )
    .unwrap();
    assert_eq!(spec.max_time(), max_await_time);
}

#[test]
fn handle_max_await_time() {
    verify_max_await_time(None, None);
    verify_max_await_time(Some(Duration::from_millis(5)), None);
    verify_max_await_time(
        Some(Duration::from_millis(5)),
        Some(CursorType::NonTailable),
    );
    verify_max_await_time(Some(Duration::from_millis(5)), Some(CursorType::Tailable));
    verify_max_await_time(
        Some(Duration::from_millis(5)),
        Some(CursorType::TailableAwait),
    );
}

#[test]
fn handle_invalid_response() {
    let find = Find::empty();

    let garbled = doc! { "asdfasf": "ASdfasdf" };
    handle_response_test(&find, garbled).unwrap_err();

    let missing_cursor_field = doc! {
        "cursor": {
            "ns": "test.test",
            "firstBatch": [],
        }
    };
    handle_response_test(&find, missing_cursor_field).unwrap_err();
}
