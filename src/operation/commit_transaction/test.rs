use crate::{
    bson::doc,
    client::options::TransactionOptions,
    cmap::StreamDescription,
    concern::{Acknowledgment, WriteConcern},
    operation::{CommitTransaction, Operation},
};

#[test]
fn build() {
    let mut op = CommitTransaction {
        options: Some(TransactionOptions {
            write_concern: Some(WriteConcern {
                w: Some(Acknowledgment::Custom("abc".to_string())),
                ..Default::default()
            }),
            ..Default::default()
        }),
    };

    let description = StreamDescription::new_testing();
    let cmd = op.build(&description).expect("build should succeed");

    assert_eq!(cmd.name.as_str(), "commitTransaction");
    assert_eq!(
        cmd.body,
        doc! {
            "commitTransaction": 1,
            "writeConcern": { "w": "abc" }
        }
    );

    let mut op = CommitTransaction { options: None };
    let cmd = op.build(&description).expect("build should succeed");
    assert_eq!(cmd.name.as_str(), "commitTransaction");
    assert_eq!(
        cmd.body,
        doc! {
            "commitTransaction": 1,
        }
    );
}
