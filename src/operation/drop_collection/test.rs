use crate::{
    bson::doc,
    cmap::{CommandResponse, StreamDescription},
    concern::{Acknowledgment, WriteConcern},
    error::{ErrorKind, WriteFailure},
    operation::{DropCollection, Operation},
    options::DropCollectionOptions,
    Namespace,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let options = DropCollectionOptions {
        write_concern: Some(WriteConcern {
            w: Some(Acknowledgment::Custom("abc".to_string())),
            ..Default::default()
        }),
    };

    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let op = DropCollection::new(ns.clone(), Some(options));

    let description = StreamDescription::new_testing();
    let cmd = op.build(&description).expect("build should succeed");

    assert_eq!(cmd.name.as_str(), "drop");
    assert_eq!(cmd.target_db.as_str(), "test_db");
    assert_eq!(
        cmd.body,
        doc! {
            "drop": "test_coll",
            "writeConcern": { "w": "abc" }
        }
    );

    let op = DropCollection::new(ns, None);
    let cmd = op.build(&description).expect("build should succeed");
    assert_eq!(cmd.name.as_str(), "drop");
    assert_eq!(cmd.target_db.as_str(), "test_db");
    assert_eq!(
        cmd.body,
        doc! {
            "drop": "test_coll",
        }
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let op = DropCollection::empty();

    let ok_response = CommandResponse::with_document(doc! { "ok": 1.0 });
    assert!(op.handle_response(ok_response, &Default::default()).is_ok());
    let ok_extra = CommandResponse::with_document(doc! { "ok": 1.0, "hello": "world" });
    assert!(op.handle_response(ok_extra, &Default::default()).is_ok());
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_write_concern_error() {
    let op = DropCollection::empty();

    let response = CommandResponse::with_document(doc! {
        "writeConcernError": {
            "code": 100,
            "codeName": "hello world",
            "errmsg": "12345"
        },
        "ok": 1
    });

    let result = op.handle_response(response, &Default::default());
    assert!(result.is_err());

    match result.unwrap_err().kind {
        ErrorKind::WriteError(WriteFailure::WriteConcernError(ref wc_err)) => {
            assert_eq!(wc_err.code, 100);
            assert_eq!(wc_err.code_name, "hello world");
            assert_eq!(wc_err.message, "12345");
        }
        ref e => panic!("expected write concern error, got {:?}", e),
    }
}
