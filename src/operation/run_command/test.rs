use std::convert::TryInto;

use bson::Timestamp;

use super::RunCommand;
use crate::{
    bson::doc,
    cmap::StreamDescription,
    operation::{test::handle_response_test, Operation},
};

#[test]
fn handle_success() {
    let op = RunCommand::new("foo".into(), doc! { "hello": 1 }, None, None).unwrap();

    let doc = doc! {
        "ok": 1,
        "some": "field",
        "other": true,
        "$clusterTime": {
            "clusterTime": Timestamp {
                time: 123,
                increment: 345,
            },
            "signature": {}
        }
    };
    let result_doc = handle_response_test(&op, doc.clone()).unwrap();
    assert_eq!(result_doc, doc);
}
