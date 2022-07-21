use std::time::Duration;

use crate::{
    bson::{doc, Document},
    bson_util,
    cmap::StreamDescription,
    cursor::CursorInformation,
    operation::{GetMore, Operation},
    options::ServerAddress,
    sdam::{ServerDescription, ServerInfo, ServerType},
    Namespace,
};

fn build_test(
    ns: Namespace,
    cursor_id: i64,
    address: ServerAddress,
    batch_size: Option<u32>,
    max_time: Option<Duration>,
    mut expected_body: Document,
) {
    let info = CursorInformation {
        ns: ns.clone(),
        id: cursor_id,
        address,
        batch_size,
        max_time,
        comment: None,
    };
    let mut get_more = GetMore::new(info, None);

    let build_result = get_more.build(&StreamDescription::new_testing());
    assert!(build_result.is_ok());

    let mut cmd = build_result.unwrap();
    assert_eq!(cmd.name, "getMore".to_string());
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
    let address = ServerAddress::Tcp {
        host: "localhost".to_string(),
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
        address,
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
    let address = ServerAddress::Tcp {
        host: "localhost".to_string(),
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
            "collection": ns.coll,
            "batchSize": 123,
        },
    );

    let info = CursorInformation {
        ns: Namespace::empty(),
        address,
        id: cursor_id,
        batch_size: Some((std::i32::MAX as u32) + 1),
        max_time: None,
        comment: None,
    };
    let mut op = GetMore::new(info, None);
    assert!(op.build(&StreamDescription::new_testing()).is_err())
}

#[test]
fn op_selection_criteria() {
    let address = ServerAddress::Tcp {
        host: "myhost.com".to_string(),
        port: Some(1234),
    };

    let info = CursorInformation {
        ns: Namespace::empty(),
        address: address.clone(),
        id: 123,
        batch_size: None,
        max_time: None,
        comment: None,
    };
    let get_more = GetMore::new(info, None);
    let server_description = ServerDescription {
        address,
        server_type: ServerType::Unknown,
        reply: Ok(None),
        last_update_time: None,
        average_round_trip_time: None,
    };
    let server_info = ServerInfo::new_borrowed(&server_description);

    let predicate = get_more
        .selection_criteria()
        .expect("should not be none")
        .as_predicate()
        .expect("should be predicate");
    assert!(predicate(&server_info));

    let server_description = ServerDescription {
        address: ServerAddress::default(),
        ..server_description
    };
    let server_info = ServerInfo::new_borrowed(&server_description);
    assert!(!predicate(&server_info));
}
