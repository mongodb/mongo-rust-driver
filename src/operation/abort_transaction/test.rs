use pretty_assertions::assert_eq;

use crate::{
    bson::doc,
    client::session::TransactionPin,
    cmap::StreamDescription,
    concern::{Acknowledgment, WriteConcern},
    operation::{AbortTransaction, Operation},
    selection_criteria::{ReadPreference::Primary, SelectionCriteria::ReadPreference},
};

#[test]
fn build() {
    let wc = WriteConcern {
        w: Some(Acknowledgment::Majority),
        ..Default::default()
    };
    let pinned = TransactionPin::Mongos(ReadPreference(Primary));

    let mut op = AbortTransaction::new(Some(wc), Some(pinned));
    let description = StreamDescription::new_testing();
    let cmd = op.build(&description).expect("build should succeed");

    assert_eq!(cmd.name, "abortTransaction");
    assert_eq!(
        cmd.body,
        doc! {
        "abortTransaction": 1,
        "writeConcern": {
            "w": "majority"
            },
        }
    );

    let mut op = AbortTransaction::new(None, None);
    let cmd = op.build(&description).expect("build should succeed");

    assert_eq!(cmd.name, "abortTransaction");
    assert_eq!(
        cmd.body,
        doc! {
        "abortTransaction": 1,
        }
    );
}
