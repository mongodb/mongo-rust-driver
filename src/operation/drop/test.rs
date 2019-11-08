use bson::{bson, doc};

use crate::{
    cmap::{CommandResponse, StreamDescription},
    concern::{Acknowledgment, WriteConcern},
    error::{ErrorKind, WriteFailure},
    operation::{test, Drop, Operation},
    Namespace,
};

#[test]
fn build() {
    let op = Drop::new(
        Namespace {
            db: "test_db".to_string(),
            coll: "test_coll".to_string(),
        },
        Some(WriteConcern {
            w: Some(Acknowledgment::Tag("abc".to_string())),
            ..Default::default()
        }),
    );

    let description = StreamDescription::new_testing();
    let cmd = op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "drop");
    assert_eq!(cmd.target_db.as_str(), "test_db");
    assert_eq!(cmd.read_pref.as_ref(), None);
    assert_eq!(
        cmd.body,
        doc! {
            "drop": "test_coll",
            "writeConcern": { "w": "abc" }
        }
    );
}

#[test]
fn handle_success() {
    let op = Drop::empty();

    let ok_response = CommandResponse::from_document(doc! { "ok": 1.0 });
    assert!(op.handle_response(ok_response).is_ok());
    let ok_extra = CommandResponse::from_document(doc! { "ok": 1.0, "hello": "world" });
    assert!(op.handle_response(ok_extra).is_ok());
}

#[test]
fn handle_command_error() {
    test::handle_command_error(Drop::empty());
}

#[test]
fn handle_write_concern_error() {
    let op = Drop::empty();

    let response = CommandResponse::from_document(doc! {
        "writeConcernError": {
            "code": 100,
            "codeName": "hello world",
            "errmsg": "12345"
        },
        "ok": 1
    });

    let result = op.handle_response(response);
    assert!(result.is_err());

    match *result.unwrap_err().kind {
        ErrorKind::WriteError(WriteFailure::WriteConcernError(ref wc_err)) => {
            assert_eq!(wc_err.code, 100);
            assert_eq!(wc_err.code_name, "hello world");
            assert_eq!(wc_err.message, "12345");
        }
        ref e => panic!("expected write concern error, got {:?}", e),
    }
}
