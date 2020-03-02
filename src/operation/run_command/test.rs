use bson::{bson, doc};

use super::RunCommand;
use crate::{
    cmap::{CommandResponse, StreamDescription},
    operation::Operation,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn no_error_ok_0() {
    let op = RunCommand::new("foo".into(), doc! { "isMaster": 1 }, None);
    assert!(op.selection_criteria().is_none());

    let command_response = CommandResponse::with_document(doc! {
        "ok": 0
    });

    assert_eq!(
        op.handle_response(command_response).ok(),
        Some(doc! { "ok": 0 })
    );
}
