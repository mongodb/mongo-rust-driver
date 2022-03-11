use crate::{
    bson::{doc, Document},
    bson_util,
    cmap::StreamDescription,
    operation::{test::handle_response_test, ListCollections, Operation},
    options::ListCollectionsOptions,
};

fn build_test(db_name: &str, mut list_collections: ListCollections, mut expected_body: Document) {
    let mut cmd = list_collections
        .build(&StreamDescription::new_testing())
        .expect("build should succeed");
    assert_eq!(cmd.name, "listCollections");
    assert_eq!(cmd.target_db, db_name);

    bson_util::sort_document(&mut cmd.body);
    bson_util::sort_document(&mut expected_body);

    assert_eq!(cmd.body, expected_body);
}

#[test]
fn build() {
    let list_collections = ListCollections::new("test_db".to_string(), None, false, None);
    let expected_body = doc! {
        "listCollections": 1,
        "nameOnly": false,
    };
    build_test("test_db", list_collections, expected_body);

    let filter = doc! { "x": 1 };
    let list_collections =
        ListCollections::new("test_db".to_string(), Some(filter.clone()), false, None);
    let expected_body = doc! {
        "listCollections": 1,
        "nameOnly": false,
        "filter": filter
    };
    build_test("test_db", list_collections, expected_body);
}

#[test]
fn build_name_only() {
    let list_collections = ListCollections::new("test_db".to_string(), None, true, None);
    build_test(
        "test_db",
        list_collections,
        doc! {
            "listCollections": 1,
            "nameOnly": true
        },
    );

    let list_collections = ListCollections::new("test_db".to_string(), None, false, None);
    build_test(
        "test_db",
        list_collections,
        doc! {
            "listCollections": 1,
            "nameOnly": false
        },
    );

    // flip nameOnly if filter has non-name fields
    let filter = doc! { "x": 3 };
    let list_collections =
        ListCollections::new("test_db".to_string(), Some(filter.clone()), true, None);
    build_test(
        "test_db",
        list_collections,
        doc! {
            "listCollections": 1,
            "filter": filter,
            "nameOnly": false,
        },
    );

    // don't flip if filter is name only.
    let filter = doc! { "name": "cat" };
    let list_collections =
        ListCollections::new("test_db".to_string(), Some(filter.clone()), true, None);
    build_test(
        "test_db",
        list_collections,
        doc! {
            "listCollections": 1,
            "filter": filter,
            "nameOnly": true,
        },
    );
}

#[test]
fn build_batch_size() {
    let list_collections = ListCollections::new("test_db".to_string(), None, true, None);
    build_test(
        "test_db",
        list_collections,
        doc! {
            "listCollections": 1,
            "nameOnly": true,
        },
    );

    let options = ListCollectionsOptions::builder().batch_size(123).build();
    let list_collections = ListCollections::new("test_db".to_string(), None, true, Some(options));
    build_test(
        "test_db",
        list_collections,
        doc! {
            "listCollections": 1,
            "nameOnly": true,
            "cursor": {
                "batchSize": 123
            }
        },
    );
}

#[test]
fn op_selection_criteria() {
    assert!(ListCollections::empty()
        .selection_criteria()
        .expect("should have criteria")
        .is_read_pref_primary());
}

#[test]
fn handle_invalid_response() {
    let list_collections = ListCollections::empty();

    let garbled = doc! { "asdfasf": "ASdfasdf" };
    handle_response_test(&list_collections, garbled).expect_err("garbled response should fail");

    let missing_cursor_field = doc! {
        "cursor": {
            "ns": "test.test",
            "firstBatch": [],
        }
    };
    handle_response_test(&list_collections, missing_cursor_field)
        .expect_err("missing cursor field should fail");
}
