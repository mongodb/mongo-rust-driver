use crate::{
    bson::{doc, Document},
    bson_util,
    cmap::{CommandResponse, StreamDescription},
    operation::{ListCollections, Operation},
    options::{ListCollectionsOptions, StreamAddress},
    Namespace,
};

fn build_test(db_name: &str, list_collections: ListCollections, mut expected_body: Document) {
    let mut cmd = list_collections
        .build(&StreamDescription::new_testing())
        .expect("build should succeed");
    assert_eq!(cmd.name, "listCollections");
    assert_eq!(cmd.target_db, db_name);

    bson_util::sort_document(&mut cmd.body);
    bson_util::sort_document(&mut expected_body);

    assert_eq!(cmd.body, expected_body);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_name_only() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_batch_size() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn op_selection_criteria() {
    assert!(ListCollections::empty()
        .selection_criteria()
        .expect("should have criteria")
        .is_read_pref_primary());
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };

    let list_collections = ListCollections::new("test_db".to_string(), None, false, None);

    let first_batch = vec![doc! {
        "name" : "test",
        "type" : "collection",
        "options" : {

        },
        "info" : {
            "readOnly" : false,
            "uuid" : "c977f2fc-9573-4a05-b219-5a39dbca14c8"
        },
        "idIndex" : {
            "v" : 2,
            "key" : {
                "_id" : 1
            },
            "name" : "_id_",
            "ns" : "test.test"
        }
    }];

    let response = doc! {
        "cursor": {
            "id": 123,
            "ns": format!("{}.{}", ns.db, ns.coll),
            "firstBatch": bson_util::to_bson_array(&first_batch),
        },
        "ok": 1.0
    };

    let cursor_spec = list_collections
        .handle_response(
            CommandResponse::with_document_and_address(StreamAddress::default(), response.clone()),
            &Default::default(),
        )
        .expect("handle should succeed");

    assert_eq!(cursor_spec.address(), &StreamAddress::default());
    assert_eq!(cursor_spec.id(), 123);
    assert_eq!(cursor_spec.batch_size(), None);
    assert_eq!(cursor_spec.max_time(), None);
    assert_eq!(
        cursor_spec
            .initial_buffer
            .into_iter()
            .collect::<Vec<Document>>(),
        first_batch
    );

    let list_collections = ListCollections::new(
        "test_db".to_string(),
        None,
        false,
        Some(ListCollectionsOptions::builder().batch_size(123).build()),
    );
    let cursor_spec = list_collections
        .handle_response(
            CommandResponse::with_document_and_address(StreamAddress::default(), response),
            &Default::default(),
        )
        .expect("handle should succeed");

    assert_eq!(cursor_spec.address(), &StreamAddress::default());
    assert_eq!(cursor_spec.id(), 123);
    assert_eq!(cursor_spec.batch_size(), Some(123));
    assert_eq!(cursor_spec.max_time(), None);
    assert_eq!(
        cursor_spec
            .initial_buffer
            .into_iter()
            .collect::<Vec<Document>>(),
        first_batch
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_invalid_response() {
    let list_collections = ListCollections::empty();

    let garbled = doc! { "asdfasf": "ASdfasdf" };
    assert!(list_collections
        .handle_response(CommandResponse::with_document(garbled), &Default::default())
        .is_err());

    let missing_cursor_field = doc! {
        "cursor": {
            "ns": "test.test",
            "firstBatch": [],
        }
    };
    assert!(list_collections
        .handle_response(
            CommandResponse::with_document(missing_cursor_field),
            &Default::default()
        )
        .is_err());
}
