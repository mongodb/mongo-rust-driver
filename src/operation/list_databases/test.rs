use bson::{bson, doc, Bson, Document};

use crate::{
    bson_util,
    cmap::{CommandResponse, StreamDescription},
    error::ErrorKind,
    operation::{ListDatabases, Operation},
    selection_criteria::ReadPreference,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let list_databases_op = ListDatabases::empty();
    let list_databases_command = list_databases_op
        .build(&StreamDescription::new_testing())
        .expect("error on build");
    assert_eq!(
        list_databases_command.body,
        doc! {
            "listDatabases": 1,
            "nameOnly": false
        }
    );
    assert_eq!(list_databases_command.target_db, "admin");
    assert_eq!(list_databases_command.read_pref, None);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_with_name_only() {
    let name_only = true;

    let list_databases_op = ListDatabases::new(None, name_only);
    let list_databases_command = list_databases_op
        .build(&StreamDescription::new_testing())
        .expect("error on build");

    assert_eq!(
        list_databases_command.body,
        doc! {
            "listDatabases": 1,
            "nameOnly": name_only
        }
    );
    assert_eq!(list_databases_command.target_db, "admin");
    assert_eq!(list_databases_command.read_pref, None);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_with_filter() {
    let filter = doc! {"something" : "something else"};

    let list_databases_op = ListDatabases::new(Some(filter.clone()), false);
    let list_databases_command = list_databases_op
        .build(&StreamDescription::new_testing())
        .unwrap();
    assert_eq!(
        list_databases_command.body,
        doc! {
            "listDatabases": 1,
            "nameOnly": false,
            "filter": Bson::Document(filter)
        }
    );
    assert_eq!(list_databases_command.target_db, "admin");
    assert_eq!(list_databases_command.read_pref, None);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let list_databases_op = ListDatabases::empty();
    let total_size = 251658240;

    let databases: Vec<Document> = vec![
        doc! {
           "name" : "admin",
           "sizeOnDisk" : 83886080,
           "empty" : false
        },
        doc! {
           "name" : "local",
           "sizeOnDisk" : 83886080,
           "empty" : false
        },
        doc! {
           "name" : "test",
           "sizeOnDisk" : 83886080,
           "empty" : false
        },
    ];

    let expected_values: Vec<Document> = databases.clone();

    let response = CommandResponse::with_document(doc! {
       "databases" : bson_util::to_bson_array(&databases),
       "totalSize" : total_size,
       "ok" : 1
    });

    let actual_values = list_databases_op
        .handle_response(response)
        .expect("supposed to succeed");

    assert_eq!(actual_values, expected_values);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_response_no_databases() {
    let list_databases_op = ListDatabases::empty();

    let response = CommandResponse::with_document(doc! {
       "ok" : 1
    });

    let result = list_databases_op.handle_response(response);
    match result.as_ref().map_err(|e| e.as_ref()) {
        Err(ErrorKind::ResponseError { .. }) => {}
        other => panic!("expected response error, but got {:?}", other),
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn op_selection_criteria() {
    let list_databases_op = ListDatabases::empty();
    assert_eq!(
        *list_databases_op
            .selection_criteria()
            .unwrap()
            .as_read_pref()
            .unwrap(),
        ReadPreference::Primary
    );
}
