use std::time::Duration;

use crate::{
    bson::doc,
    client::options::ServerAddress,
    cmap::StreamDescription,
    operation::{test::handle_response_test, ListIndexes, Operation},
    options::{IndexOptions, IndexVersion, ListIndexesOptions, TextIndexVersion},
    IndexModel,
    Namespace,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let list_options = ListIndexesOptions::builder()
        .max_time(Some(Duration::from_millis(42)))
        .batch_size(Some(4))
        .build();
    let mut list_indexes = ListIndexes::new(ns, Some(list_options));

    let cmd = list_indexes
        .build(&StreamDescription::new_testing())
        .expect("ListIndexes command failed to build when it should have succeeded.");

    assert_eq!(
        cmd.body,
        doc! {
            "listIndexes": "test_coll",
            "maxTimeMS": 42,
            "batchSize": 4,
        }
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let op = ListIndexes::empty();

    let first_batch = vec![
        IndexModel::builder()
            .keys(doc! {"x": 1})
            .options(Some(
                IndexOptions::builder()
                    .version(Some(IndexVersion::V1))
                    .name(Some("foo".to_string()))
                    .sparse(Some(false))
                    .build(),
            ))
            .build(),
        IndexModel::builder()
            .keys(doc! {"y": 1, "z": -1})
            .options(Some(
                IndexOptions::builder()
                    .version(Some(IndexVersion::V1))
                    .name(Some("x_1_z_-1".to_string()))
                    .text_index_version(Some(TextIndexVersion::V3))
                    .default_language(Some("spanish".to_string()))
                    .build(),
            ))
            .build(),
    ];

    let response = doc! {
        "cursor": {
            "id": 123,
            "ns": "test_db.test_coll",
            "firstBatch": bson::to_bson(&first_batch).unwrap(),
        },
        "ok": 1,
    };

    let cursor_spec = handle_response_test(&op, response).unwrap();

    assert_eq!(cursor_spec.id(), 123);
    assert_eq!(cursor_spec.address(), &ServerAddress::default());
    assert_eq!(cursor_spec.batch_size(), None);
    assert_eq!(cursor_spec.max_time(), None);

    assert_eq!(
        bson::to_bson(&cursor_spec.initial_buffer).unwrap(),
        bson::to_bson(&first_batch).unwrap(),
    );
}
