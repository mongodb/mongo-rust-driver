use crate::{
    bson::doc,
    cmap::{CommandResponse, StreamDescription},
    concern::WriteConcern,
    error::{ErrorKind, WriteFailure},
    operation::{Create, Operation},
    options::{CreateCollectionOptions, ValidationAction, ValidationLevel},
    Namespace,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let op = Create::new(
        Namespace {
            db: "test_db".to_string(),
            coll: "test_coll".to_string(),
        },
        Some(CreateCollectionOptions {
            write_concern: Some(WriteConcern {
                journal: Some(true),
                ..Default::default()
            }),
            validation_level: Some(ValidationLevel::Moderate),
            validation_action: Some(ValidationAction::Warn),
            ..Default::default()
        }),
    );

    let description = StreamDescription::new_testing();
    let cmd = op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "create");
    assert_eq!(cmd.target_db.as_str(), "test_db");
    assert_eq!(cmd.read_pref.as_ref(), None);
    assert_eq!(
        cmd.body,
        doc! {
            "create": "test_coll",
            "validationLevel": "moderate",
            "validationAction": "warn",
            "writeConcern": { "j": true },
        }
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let op = Create::empty();

    let ok_response = CommandResponse::with_document(doc! { "ok": 1.0 });
    assert!(op.handle_response(ok_response).is_ok());
    let ok_extra = CommandResponse::with_document(doc! { "ok": 1.0, "hello": "world" });
    assert!(op.handle_response(ok_extra).is_ok());
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_write_concern_error() {
    let op = Create::empty();

    let response = CommandResponse::with_document(doc! {
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
