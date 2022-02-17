use std::time::Duration;

use crate::{
    bson::{doc, Document},
    bson_util,
    cmap::StreamDescription,
    operation::{
        test::{self, handle_response_test},
        Find,
        Operation,
    },
    options::{CursorType, FindOptions, Hint, ReadConcern, ReadConcernLevel},
    Namespace,
};

fn build_test(
    ns: Namespace,
    filter: Option<Document>,
    options: Option<FindOptions>,
    mut expected_body: Document,
) {
    let mut find = Find::new(ns.clone(), filter, options);

    let cmd = find.build(&StreamDescription::new_testing()).unwrap();

    assert_eq!(cmd.name.as_str(), "find");
    assert_eq!(cmd.target_db.as_str(), ns.db.as_str());

    let cmd_bytes = find.serialize_command(cmd).unwrap();
    let mut cmd_doc = bson::from_slice(&cmd_bytes).unwrap();

    bson_util::sort_document(&mut expected_body);
    bson_util::sort_document(&mut cmd_doc);

    assert_eq!(cmd_doc, expected_body);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
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
        .read_concern(ReadConcern::from(ReadConcernLevel::Available))
        .build();

    let expected_body = doc! {
        "find": "test_coll",
        "$db": "test_db",
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_cursor_type() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let non_tailable_options = FindOptions::builder()
        .cursor_type(CursorType::NonTailable)
        .build();

    let non_tailable_body = doc! {
        "find": "test_coll",
        "$db": "test_db",
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
        "tailable": true,
        "$db": "test_db",
    };

    build_test(ns.clone(), None, Some(tailable_options), tailable_body);

    let tailable_await_options = FindOptions::builder()
        .cursor_type(CursorType::TailableAwait)
        .build();

    let tailable_await_body = doc! {
        "find": "test_coll",
        "$db": "test_db",
        "tailable": true,
        "awaitData": true,
    };

    build_test(ns, None, Some(tailable_await_options), tailable_await_body);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_max_await_time() {
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
        "$db": "test_db",
        "maxTimeMS": 10i32
    };

    build_test(ns, None, Some(options), body);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_limit() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let positive_options = FindOptions::builder().limit(5).build();

    let positive_body = doc! {
        "find": "test_coll",
        "$db": "test_db",
        "limit": 5_i64
    };

    build_test(ns.clone(), None, Some(positive_options), positive_body);

    let negative_options = FindOptions::builder().limit(-5).build();

    let negative_body = doc! {
        "find": "test_coll",
        "$db": "test_db",
        "limit": 5_i64,
        "singleBatch": true
    };

    build_test(ns, None, Some(negative_options), negative_body);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_batch_size() {
    let options = FindOptions::builder().batch_size(1).build();
    let body = doc! {
        "find": "",
        "$db": "",
        "batchSize": 1
    };
    build_test(Namespace::empty(), None, Some(options), body);

    let options = FindOptions::builder()
        .batch_size((std::i32::MAX as u32) + 1)
        .build();
    let mut op = Find::new(Namespace::empty(), None, Some(options));
    assert!(op.build(&StreamDescription::new_testing()).is_err())
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn op_selection_criteria() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_max_await_time() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_invalid_response() {
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
