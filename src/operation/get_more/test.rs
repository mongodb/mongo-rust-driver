use crate::{
    cursor::CursorInformation,
    operation::{GetMore, Operation},
    options::ServerAddress,
    sdam::{ServerDescription, ServerInfo, ServerType},
    Namespace,
};

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
