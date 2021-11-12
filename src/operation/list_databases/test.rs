use bson::RawDocumentBuf;

use crate::{
    bson::{doc, Bson, Document},
    bson_util,
    cmap::StreamDescription,
    error::ErrorKind,
    operation::{test::handle_response_test, ListDatabases, Operation},
    options::ListDatabasesOptions,
    selection_criteria::ReadPreference,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let mut list_databases_op = ListDatabases::empty();
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
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_with_name_only() {
    let name_only = true;

    let mut list_databases_op = ListDatabases::new(None, name_only, None);
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
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_with_filter() {
    let filter = doc! {"something" : "something else"};

    let mut list_databases_op = ListDatabases::new(Some(filter.clone()), false, None);
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
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_with_options() {
    let options = ListDatabasesOptions::builder()
        .authorized_databases(true)
        .build();

    let mut list_databases_op = ListDatabases::new(None, false, Some(options));
    let list_databases_command = list_databases_op
        .build(&StreamDescription::new_testing())
        .unwrap();
    assert_eq!(
        list_databases_command.body,
        doc! {
            "listDatabases": 1,
            "nameOnly": false,
            "authorizedDatabases": true
        }
    );
    assert_eq!(list_databases_command.target_db, "admin");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let total_size = 251658240;

    let databases = vec![
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

    let raw_databases = databases
        .iter()
        .map(|d| RawDocumentBuf::from_document(&d).unwrap())
        .collect::<Vec<_>>();

    let actual_values = handle_response_test(
        &ListDatabases::empty(),
        doc! {
           "databases" : databases,
           "totalSize" : total_size,
           "ok" : 1
        },
    )
    .expect("should succeed");

    assert_eq!(actual_values, raw_databases);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_response_no_databases() {
    let result = handle_response_test(&ListDatabases::empty(), doc! { "ok": 1 });
    match result.map_err(|e| *e.kind) {
        Err(ErrorKind::InvalidResponse { .. }) => {}
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
