use bson::RawDocumentBuf;

use crate::{
    bson::{doc, Bson},
    cmap::StreamDescription,
    error::ErrorKind,
    operation::{test::handle_response_test, ListDatabases, Operation},
    client::action::list_databases,
    selection_criteria::ReadPreference, db::options::ListDatabasesOptions,
};

#[test]
fn build() {
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

#[test]
fn build_with_name_only() {
    let name_only = true;

    let mut list_databases_op = ListDatabases::new(name_only, None);
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

#[test]
fn build_with_filter() {
    let filter = doc! {"something" : "something else"};

    let mut list_databases_op = ListDatabases::new(false, Some(ListDatabasesOptions {
        filter: Some(filter.clone()),
        ..Default::default()
    }));
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

#[test]
fn build_with_options() {
    let options = list_databases::Options {
        authorized_databases: Some(true),
        ..Default::default()
    };

    let mut list_databases_op = ListDatabases::new(false, Some(options));
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

#[test]
fn handle_success() {
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
        .map(|d| RawDocumentBuf::from_document(d).unwrap())
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

#[test]
fn handle_response_no_databases() {
    let result = handle_response_test(&ListDatabases::empty(), doc! { "ok": 1 });
    match result.map_err(|e| *e.kind) {
        Err(ErrorKind::InvalidResponse { .. }) => {}
        other => panic!("expected response error, but got {:?}", other),
    }
}

#[test]
fn op_selection_criteria() {
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
