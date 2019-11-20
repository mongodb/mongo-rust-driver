use std::time::Duration;

use bson::{bson, doc, Document};

use crate::{
    bson_util,
    cmap::{CommandResponse, StreamDescription},
    operation::{test, Find, Operation},
    options::{CursorType, FindOptions, Hint, ReadConcern, StreamAddress},
    Namespace,
};

fn build_test(
    ns: Namespace,
    filter: Option<Document>,
    options: Option<FindOptions>,
    mut expected_body: Document,
) {
    let find = Find::new(ns.clone(), filter, options);

    let mut cmd = find.build(&StreamDescription::new_testing()).unwrap();

    assert_eq!(cmd.name.as_str(), "find");
    assert_eq!(cmd.target_db.as_str(), ns.db.as_str());

    bson_util::sort_document(&mut expected_body);
    bson_util::sort_document(&mut cmd.body);

    assert_eq!(cmd.body, expected_body);
}

#[test]
fn build() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let filter = doc! {
        "x": 2,
        "y": { "$gt": 1 },
    };

    let options = FindOptions::builder()
        .hint(Hint::Keys(doc! { "x": 1, "y": 2 }))
        .projection(doc! { "x": 0 })
        .allow_partial_results(true)
        .read_concern(ReadConcern::Available)
        .build();

    let expected_body = doc! {
        "find": "test_coll",
        "filter": filter.clone(),
        "hint": {
            "x": 1,
            "y": 2,
        },
        "projection": {
            "x": 0
        },
        "allowPartialResults": true,
        "readConcern": {
            "level": "available"
        }
    };

    build_test(ns, Some(filter), Some(options), expected_body);
}

#[test]
fn build_cursor_type() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let non_tailable_options = FindOptions::builder()
        .cursor_type(CursorType::NonTailable)
        .build();

    let non_tailable_body = doc! {
        "find": "test_coll",
    };

    build_test(
        ns.clone(),
        None,
        Some(non_tailable_options),
        non_tailable_body,
    );

    let tailable_options = FindOptions::builder()
        .cursor_type(CursorType::Tailable)
        .build();

    let tailable_body = doc! {
        "find": "test_coll",
        "tailable": true
    };

    build_test(ns.clone(), None, Some(tailable_options), tailable_body);

    let tailable_await_options = FindOptions::builder()
        .cursor_type(CursorType::TailableAwait)
        .build();

    let tailable_await_body = doc! {
        "find": "test_coll",
        "tailable": true,
        "awaitData": true,
    };

    build_test(
        ns.clone(),
        None,
        Some(tailable_await_options),
        tailable_await_body,
    );
}

#[test]
fn build_max_await_time() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let options = FindOptions::builder()
        .max_await_time(Duration::from_millis(5))
        .max_time(Duration::from_millis(10))
        .build();

    let body = doc! {
        "find": "test_coll",
        "maxTimeMS": 10 as i64
    };

    build_test(ns.clone(), None, Some(options), body);
}

#[test]
fn build_limit() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let positive_options = FindOptions::builder().limit(5).build();

    let positive_body = doc! {
        "find": "test_coll",
        "limit": 5 as i64
    };

    build_test(ns.clone(), None, Some(positive_options), positive_body);

    let negative_options = FindOptions::builder().limit(-5).build();

    let negative_body = doc! {
        "find": "test_coll",
        "limit": 5 as i64,
        "singleBatch": true
    };

    build_test(ns.clone(), None, Some(negative_options), negative_body);
}

#[test]
fn handle_success() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let address = StreamAddress {
        hostname: "localhost".to_string(),
        port: None,
    };

    let find = Find::empty();

    let first_batch = vec![doc! {"_id": 1}, doc! {"_id": 2}];

    let response = doc! {
        "cursor": {
            "id": 123,
            "ns": format!("{}.{}", ns.db, ns.coll),
            "firstBatch": bson_util::to_bson_array(&first_batch),
        },
        "ok": 1.0
    };

    let result = find.handle_response(CommandResponse::with_document_and_address(
        address.clone(),
        response.clone(),
    ));
    assert!(result.is_ok());

    let cursor_spec = result.unwrap();
    assert_eq!(cursor_spec.address, address);
    assert_eq!(cursor_spec.id, 123);
    assert_eq!(cursor_spec.batch_size, None);
    assert_eq!(
        cursor_spec.buffer.into_iter().collect::<Vec<Document>>(),
        first_batch
    );

    let find = Find::new(
        ns.clone(),
        None,
        Some(FindOptions::builder().batch_size(123).build()),
    );
    let result = find.handle_response(CommandResponse::with_document_and_address(
        address.clone(),
        response,
    ));
    assert!(result.is_ok());

    let cursor_spec = result.unwrap();
    assert_eq!(cursor_spec.address, address);
    assert_eq!(cursor_spec.id, 123);
    assert_eq!(cursor_spec.batch_size, Some(123));
    assert_eq!(
        cursor_spec.buffer.into_iter().collect::<Vec<Document>>(),
        first_batch
    );
}

fn verify_max_await_time(
    max_await_time: Option<Duration>,
    cursor_type: Option<CursorType>,
    expects_max_time: bool,
) {
    let ns = Namespace::empty();
    let address = StreamAddress {
        hostname: "localhost".to_string(),
        port: None,
    };
    let find = Find::new(
        ns,
        None,
        Some(FindOptions {
            cursor_type,
            max_await_time,
            ..Default::default()
        }),
    );

    let response = CommandResponse::with_document_and_address(
        address,
        doc! {
            "cursor": {
                "id": 123,
                "ns": "a.b",
                "firstBatch": [],
            },
            "ok": 1
        },
    );

    let spec = find
        .handle_response(response)
        .expect("should handle correctly");
    if expects_max_time {
        assert_eq!(spec.max_time, max_await_time);
    } else {
        assert!(spec.max_time.is_none());
    }
}

#[test]
fn handle_max_await_time() {
    verify_max_await_time(None, None, false);
    verify_max_await_time(Some(Duration::from_millis(5)), None, false);
    verify_max_await_time(
        Some(Duration::from_millis(5)),
        Some(CursorType::NonTailable),
        false,
    );
    verify_max_await_time(
        Some(Duration::from_millis(5)),
        Some(CursorType::Tailable),
        false,
    );
    verify_max_await_time(
        Some(Duration::from_millis(5)),
        Some(CursorType::TailableAwait),
        true,
    );
}

#[test]
fn handle_command_error() {
    test::handle_command_error(Find::empty())
}

#[test]
fn handle_invalid_response() {
    let find = Find::empty();

    let garbled = doc! { "asdfasf": "ASdfasdf" };
    assert!(find
        .handle_response(CommandResponse::with_document(garbled))
        .is_err());

    let missing_cursor_field = doc! {
        "cursor": {
            "ns": "test.test",
            "firstBatch": [],
        }
    };
    assert!(find
        .handle_response(CommandResponse::with_document(missing_cursor_field))
        .is_err());
}
