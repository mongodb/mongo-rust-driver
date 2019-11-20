use std::time::Duration;

use bson::{bson, doc, Document};

use crate::{
    bson_util,
    cmap::{CommandResponse, StreamDescription},
    operation::{test, GetMore, Operation},
    options::StreamAddress,
    Namespace,
};

fn build_test(
    ns: Namespace,
    cursor_id: i64,
    address: StreamAddress,
    batch_size: Option<u32>,
    max_time: Option<Duration>,
    mut expected_body: Document,
) {
    let get_more = GetMore::new(ns.clone(), cursor_id, address.clone(), batch_size, max_time);

    let build_result = get_more.build(&StreamDescription::new_testing());
    assert!(build_result.is_ok());

    let mut cmd = build_result.unwrap();
    assert_eq!(cmd.name, "getMore".to_string());
    assert_eq!(cmd.read_pref, None);
    assert_eq!(cmd.target_db, ns.db);

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
    let cursor_id: i64 = 123;
    let address = StreamAddress {
        hostname: "localhost".to_string(),
        port: Some(1234),
    };
    let batch_size: u32 = 123;
    let max_time = Duration::from_millis(456);

    let expected_body = doc! {
        "getMore": cursor_id,
        "collection": ns.coll.clone(),
        "batchSize": 123,
        "maxTimeMS": 456,
    };

    build_test(
        ns,
        cursor_id,
        address.clone(),
        Some(batch_size),
        Some(max_time),
        expected_body,
    );
}

#[test]
fn build_batch_size() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let cursor_id: i64 = 123;
    let address = StreamAddress {
        hostname: "localhost".to_string(),
        port: Some(1234),
    };

    build_test(
        ns.clone(),
        cursor_id,
        address.clone(),
        Some(0),
        None,
        doc! {
            "getMore": cursor_id,
            "collection": ns.coll.clone(),
        },
    );

    build_test(
        ns.clone(),
        cursor_id,
        address.clone(),
        Some(123),
        None,
        doc! {
            "getMore": cursor_id,
            "collection": ns.coll.clone(),
            "batchSize": 123,
        },
    );
}

#[test]
fn handle_success() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let cursor_id: i64 = 123;
    let address = StreamAddress {
        hostname: "localhost".to_string(),
        port: Some(1234),
    };

    let get_more = GetMore::new(ns, cursor_id, address, None, None);

    let batch = vec![doc! { "_id": 1 }, doc! { "_id": 2 }, doc! { "_id": 3 }];

    let response = CommandResponse::with_document(doc! {
        "cursor": {
            "id": 123,
            "ns": "test_db.test_coll",
            "nextBatch": bson_util::to_bson_array(&batch),
        },
        "ok": 1
    });

    let result = get_more
        .handle_response(response)
        .expect("handle success case failed");
    assert!(!result.exhausted);
    assert_eq!(result.batch, batch);

    let response = CommandResponse::with_document(doc! {
        "cursor": {
            "id": 0,
            "ns": "test_db.test_coll",
            "nextBatch": bson_util::to_bson_array(&batch),
        },
        "ok": 1
    });
    let result = get_more
        .handle_response(response)
        .expect("handle success case failed");
    assert!(result.exhausted);
    assert_eq!(result.batch, batch);
}

#[test]
fn handle_command_error() {
    test::handle_command_error(GetMore::empty());
}
