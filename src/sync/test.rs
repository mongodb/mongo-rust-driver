use crate::{sync::Client, test::CLIENT_OPTIONS};
use bson::Document;

#[test]
#[function_name::named]
fn list_databases() {
    let options = CLIENT_OPTIONS.clone();
    let client = Client::with_options(options).expect("client creation should succeed");

    client
        .database(function_name!())
        .collection(function_name!())
        .insert_one(Document::new(), None)
        .expect("insert should succeed");

    let db_names = client
        .list_database_names(None)
        .expect("list_database_names should succeed");
    assert!(db_names.contains(&function_name!().to_string()));
}
