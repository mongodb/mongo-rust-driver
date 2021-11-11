use crate::{
    bson::doc,
    cmap::StreamDescription,
    concern::{Acknowledgment, WriteConcern},
    error::{ErrorKind, WriteFailure},
    operation::{test::handle_response_test, DropDatabase, Operation},
    options::DropDatabaseOptions,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let mut op = DropDatabase {
        target_db: "test_db".to_string(),
        options: Some(DropDatabaseOptions {
            write_concern: Some(WriteConcern {
                w: Some(Acknowledgment::Custom("abc".to_string())),
                ..Default::default()
            }),
        }),
    };

    let description = StreamDescription::new_testing();
    let cmd = op.build(&description).expect("build should succeed");

    assert_eq!(cmd.name.as_str(), "dropDatabase");
    assert_eq!(cmd.target_db.as_str(), "test_db");
    assert_eq!(
        cmd.body,
        doc! {
            "dropDatabase": 1,
            "writeConcern": { "w": "abc" }
        }
    );

    let mut op = DropDatabase {
        target_db: "test_db".to_string(),
        options: None,
    };
    let cmd = op.build(&description).expect("build should succeed");
    assert_eq!(cmd.name.as_str(), "dropDatabase");
    assert_eq!(cmd.target_db.as_str(), "test_db");
    assert_eq!(
        cmd.body,
        doc! {
            "dropDatabase": 1,
        }
    );
}

#[test]
fn build_no_write_concern() {
    let mut op = DropDatabase {
        target_db: "test_db".to_string(),
        options: Some(DropDatabaseOptions {
            write_concern: Some(WriteConcern {
                ..Default::default()
            }),
        }),
    };

    let description = StreamDescription::new_testing();
    let cmd = op.build(&description).expect("build should succeed");

    assert_eq!(cmd.name.as_str(), "dropDatabase");
    assert_eq!(cmd.target_db.as_str(), "test_db");
    assert_eq!(
        cmd.body,
        doc! {
            "dropDatabase": 1,
        }
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let op = DropDatabase::empty();

    let ok_response = doc! { "ok": 1.0 };
    handle_response_test(&op, ok_response).unwrap();
    let ok_extra = doc! { "ok": 1.0, "hello": "world" };
    handle_response_test(&op, ok_extra).unwrap();
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_write_concern_error() {
    let op = DropDatabase::empty();

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
