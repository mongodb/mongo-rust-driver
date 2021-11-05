use pretty_assertions::assert_eq;

use crate::{
    bson::{doc},
    bson_util,
    client::session::TransactionPin,
    concern::{Acknowledgment, WriteConcern},
    cmap::StreamDescription,
    operation::{AbortTransaction, Operation},
    selection_criteria::{SelectionCriteria::ReadPreference, ReadPreference::Primary},
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let wc = WriteConcern {
        w: Some(Acknowledgment::Majority),
        ..Default::default()
    };
    let pinned = TransactionPin::Mongos(ReadPreference(Primary));

    let mut op = AbortTransaction::new(Some(wc), Some(pinned));
    let mut cmd = op.build(&StreamDescription::new_testing()).unwrap();

    assert_eq!(cmd.name, "abortTransaction");

    let mut expected_body = doc! {
        "abortTransaction": 1,
        "writeConcern": {
            "w": "majority"
        },
    };

    bson_util::sort_document(&mut cmd.body);
    bson_util::sort_document(&mut expected_body);

    assert_eq!(cmd.body, expected_body);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_no_write_concern() {
    let pinned = TransactionPin::Mongos(ReadPreference(Primary));

    let mut op = AbortTransaction::new(None, Some(pinned));
    let mut cmd = op.build(&StreamDescription::new_testing()).unwrap();

    assert_eq!(cmd.name, "abortTransaction");

    let mut expected_body = doc! {
        "abortTransaction": 1,
    };

    bson_util::sort_document(&mut cmd.body);
    bson_util::sort_document(&mut expected_body);

    assert_eq!(cmd.body, expected_body);
}