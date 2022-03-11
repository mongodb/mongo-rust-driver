use std::time::Duration;

use crate::{
    bson::doc,
    cmap::StreamDescription,
    coll::{options::DropIndexOptions, Namespace},
    concern::WriteConcern,
    operation::{test::handle_response_test, DropIndexes, Operation},
};

#[test]
fn build() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let options = DropIndexOptions::builder()
        .max_time(Some(Duration::from_secs(1)))
        .write_concern(Some(WriteConcern::builder().journal(Some(true)).build()))
        .build();

    let mut drop_index = DropIndexes::new(ns, "foo".to_string(), Some(options));
    let cmd = drop_index
        .build(&StreamDescription::new_testing())
        .expect("DropIndex command failed to build when it should have succeeded.");
    assert_eq!(
        cmd.body,
        doc! {
            "dropIndexes": "test_coll",
            "index": "foo",
            "maxTimeMS": 1000,
            "writeConcern": { "j": true },
        }
    )
}

#[test]
fn handle_success() {
    let op = DropIndexes::empty();
    let response = doc! { "ok": 1 };
    handle_response_test(&op, response).unwrap();
}
