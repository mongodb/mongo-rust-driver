use bson::{bson, doc};

use super::RunCommand;
use crate::{cmap::StreamDescription, operation::Operation};

#[test]
fn basic_construction() {
    let op = RunCommand::new("foo".into(), doc! { "isMaster": 1 }, None);
    assert!(op.selection_criteria().is_none());

    let command = op.build(&StreamDescription::new_testing()).unwrap();

    assert_eq!(command.name, "isMaster");
    assert_eq!(command.target_db, "foo");
    assert!(command.read_pref.is_none());
    assert_eq!(
        command
            .body
            .get("isMaster")
            .and_then(crate::bson_util::get_int),
        Some(1)
    );
}
