use std::time::Duration;

use crate::{
    bson::doc,
    cmap::StreamDescription,
    coll::{
        options::{CommitQuorum, CreateIndexOptions},
        Namespace,
    },
    concern::WriteConcern,
    index::{options::IndexOptions, IndexModel},
    operation::{test::handle_response_test, CreateIndexes, Operation},
    results::CreateIndexesResult,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let index_options = IndexOptions::builder()
        .name(Some("foo".to_string()))
        .build();
    let index_model = IndexModel::builder()
        .keys(doc! { "x": 1 })
        .options(Some(index_options))
        .build();
    let create_options = CreateIndexOptions::builder()
        .commit_quorum(Some(CommitQuorum::Majority))
        .max_time(Some(Duration::from_millis(42)))
        .write_concern(Some(WriteConcern::builder().journal(Some(true)).build()))
        .build();
    let mut create_indexes = CreateIndexes::new(ns, vec![index_model], Some(create_options));

    let cmd = create_indexes
        .build(&StreamDescription::with_wire_version(10))
        .expect("CreateIndexes command failed to build when it should have succeeded.");

    assert_eq!(
        cmd.body,
        doc! {
            "createIndexes": "test_coll",
            "indexes": [{
                "key": { "x": 1 },
                "name": "foo"
            }],
            "commitQuorum": "majority",
            "maxTimeMS": 42,
            "writeConcern": { "j": true },
        }
    )
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let a = IndexModel::builder()
        .keys(doc! { "a": 1 })
        .options(Some(
            IndexOptions::builder().name(Some("a".to_string())).build(),
        ))
        .build();
    let b = IndexModel::builder()
        .keys(doc! { "b": 1 })
        .options(Some(
            IndexOptions::builder().name(Some("b".to_string())).build(),
        ))
        .build();
    let op = CreateIndexes::with_indexes(vec![a, b]);

    let response = doc! {
        "ok": 1,
        "createdCollectionAutomatically": false,
        "numIndexesBefore": 1,
        "numIndexesAfter": 3,
        "commitQuorum": "votingMembers",
    };

    let expected_values = CreateIndexesResult {
        index_names: vec!["a".to_string(), "b".to_string()],
    };
    let actual_values = handle_response_test(&op, response).unwrap();
    assert_eq!(actual_values, expected_values);
}
