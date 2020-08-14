use pretty_assertions::assert_eq;

use std::collections::HashMap;

use crate::{
    bson::doc,
    bson_util,
    cmap::{CommandResponse, StreamDescription},
    concern::{Acknowledgment, WriteConcern},
    error::{ErrorKind, WriteConcernError, WriteError, WriteFailure},
    operation::{CreateIndexes, Operation},
    options::{CreateIndexesOptions, Index, IndexType},
    Namespace,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_one() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let mut indexes = Vec::new();

    let mut keys = HashMap::new();
    keys.insert("test".to_string(), IndexType::Ascending);

    indexes.push(Index::builder().keys(keys).name("test".to_string()).build());

    let wc = WriteConcern {
        w: Some(Acknowledgment::Majority),
        ..Default::default()
    };
    let options = CreateIndexesOptions::builder().write_concern(wc).build();

    let op = CreateIndexes::new(ns, indexes, Some(options));

    let description = StreamDescription::new_testing();
    let mut cmd = op.build(&description).unwrap();

    assert_eq!(cmd.name.as_str(), "createIndexes");
    assert_eq!(cmd.target_db.as_str(), "test_db");
    assert_eq!(cmd.read_pref.as_ref(), None);

    let mut expected_body = doc! {
        "createIndexes": "test_coll",
        "indexes": [{"key": {"test": 1}, "name": "test"}],
        "writeConcern": {
            "w": "majority",
        }
    };

    bson_util::sort_document(&mut cmd.body);
    bson_util::sort_document(&mut expected_body);

    assert_eq!(cmd.body, expected_body);
}