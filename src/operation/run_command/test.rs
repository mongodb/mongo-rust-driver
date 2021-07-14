use bson::Timestamp;

use super::RunCommand;
use crate::{
    bson::doc,
    client::ClusterTime,
    cmap::{RawCommandResponse, StreamDescription},
    operation::{test::handle_response_test, Operation, Response},
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let mut op = RunCommand::new("foo".into(), doc! { "isMaster": 1 }, None).unwrap();
    assert!(op.selection_criteria().is_none());

    let command = op.build(&StreamDescription::new_testing()).unwrap();

    assert_eq!(command.name, "isMaster");
    assert_eq!(command.target_db, "foo");
    assert_eq!(
        command
            .body
            .get("isMaster")
            .and_then(crate::bson_util::get_int),
        Some(1)
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let op = RunCommand::new("foo".into(), doc! { "hello": 1 }, None).unwrap();

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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn response() {
    let cluster_timestamp = Timestamp {
        time: 123,
        increment: 345,
    };
    let doc = doc! {
        "ok": 1,
        "some": "field",
        "other": true,
        "$clusterTime": {
            "clusterTime": cluster_timestamp,
            "signature": {}
        }
    };
    let raw = RawCommandResponse::with_document(doc).unwrap();
    let response = <RunCommand as Operation>::Response::deserialize_response(&raw).unwrap();

    assert!(response.is_success());
    assert_eq!(
        response.cluster_time(),
        Some(&ClusterTime {
            cluster_time: cluster_timestamp,
            signature: doc! {},
        })
    );
}
