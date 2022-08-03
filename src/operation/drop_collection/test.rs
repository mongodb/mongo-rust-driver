use crate::{
    bson::doc,
    error::{ErrorKind, WriteFailure},
    operation::{test::handle_response_test, DropCollection},
};

#[test]
fn handle_success() {
    let op = DropCollection::empty();

    let ok_response = doc! { "ok": 1.0 };
    handle_response_test(&op, ok_response).unwrap();
    let ok_extra = doc! { "ok": 1.0, "hello": "world" };
    handle_response_test(&op, ok_extra).unwrap();
}

#[test]
fn handle_write_concern_error() {
    let op = DropCollection::empty();

    let response = doc! {
        "writeConcernError": {
            "code": 100,
            "codeName": "hello world",
            "errmsg": "12345"
        },
        "ok": 1
    };

    let err = handle_response_test(&op, response).unwrap_err();
    match *err.kind {
        ErrorKind::Write(WriteFailure::WriteConcernError(ref wc_err)) => {
            assert_eq!(wc_err.code, 100);
            assert_eq!(wc_err.code_name, "hello world");
            assert_eq!(wc_err.message, "12345");
        }
        ref e => panic!("expected write concern error, got {:?}", e),
    }
}
