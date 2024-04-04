use std::{fmt::Debug, time::Duration};

use bson::{rawdoc, RawDocumentBuf};
use futures::stream::{StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use semver::VersionReq;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[allow(deprecated)]
use crate::test::EventClient;
use crate::{
    bson::{doc, to_document, Bson, Document},
    error::{ErrorKind, Result, WriteFailure},
    options::{
        Acknowledgment,
        CollectionOptions,
        DeleteOptions,
        DropCollectionOptions,
        FindOneAndDeleteOptions,
        FindOptions,
        Hint,
        IndexOptions,
        ReadConcern,
        ReadPreference,
        SelectionCriteria,
        WriteConcern,
    },
    results::DeleteResult,
    test::{
        get_client_options,
        log_uncaptured,
        util::{event_buffer::EventBuffer, TestClient},
    },
    Client,
    Collection,
    IndexModel,
    Namespace,
};

#[tokio::test]
#[function_name::named]
async fn insert_err_details() {
    let client = TestClient::new().await;
    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;
    if client.server_version_lt(4, 0) || !client.is_replica_set() {
        log_uncaptured("skipping insert_err_details due to test configuration");
        return;
    }
    client
        .database("admin")
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "data": {
            "failCommands": ["insert"],
            "writeConcernError": {
                "code": 100,
                "codeName": "UnsatisfiableWriteConcern",
                "errmsg": "Not enough data-bearing nodes",
                "errInfo": {
                "writeConcern": {
                    "w": 2,
                    "wtimeout": 0,
                    "provenance": "clientSupplied"
                }
                }
            }
            },
            "mode": { "times": 1 }
        })
        .await
        .unwrap();

    let wc_error_result = coll.insert_one(doc! { "test": 1 }).await;
    match *wc_error_result.unwrap_err().kind {
        ErrorKind::Write(WriteFailure::WriteConcernError(ref wc_error)) => {
            match &wc_error.details {
                Some(doc) => {
                    let result = doc.get_document("writeConcern");
                    match result {
                        Ok(write_concern_doc) => {
                            assert!(write_concern_doc.contains_key("provenance"));
                        }
                        Err(e) => panic!("{:?}", e),
                    }
                }
                None => panic!("expected details field"),
            }
        }
        ref e => panic!("expected write concern error, got {:?}", e),
    }
}

#[tokio::test]
#[function_name::named]
async fn count() {
    let client = TestClient::new().await;
    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    assert_eq!(coll.estimated_document_count().await.unwrap(), 0);

    let _ = coll.insert_one(doc! { "x": 1 }).await.unwrap();
    assert_eq!(coll.estimated_document_count().await.unwrap(), 1);

    let result = coll
        .insert_many((1..4).map(|i| doc! { "x": i }).collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(result.inserted_ids.len(), 3);
    assert_eq!(coll.estimated_document_count().await.unwrap(), 4);
}

#[tokio::test]
#[function_name::named]
async fn find() {
    let client = TestClient::new().await;
    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    let result = coll
        .insert_many((0i32..5).map(|i| doc! { "x": i }).collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(result.inserted_ids.len(), 5);

    let mut cursor = coll.find(doc! {}).await.unwrap().enumerate();

    while let Some((i, result)) = cursor.next().await {
        let doc = result.unwrap();
        assert!(i <= 4, "expected 4 result, got {}", i);
        assert_eq!(doc.len(), 2);
        assert!(doc.contains_key("_id"));
        assert_eq!(doc.get("x"), Some(&Bson::Int32(i as i32)));
    }
}

#[tokio::test]
#[function_name::named]
async fn update() {
    let client = TestClient::new().await;
    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    let result = coll
        .insert_many((0i32..5).map(|_| doc! { "x": 3 }).collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(result.inserted_ids.len(), 5);

    let update_one_results = coll
        .update_one(doc! {"x": 3}, doc! {"$set": { "x": 5 }})
        .await
        .unwrap();
    assert_eq!(update_one_results.modified_count, 1);
    assert!(update_one_results.upserted_id.is_none());

    let update_many_results = coll
        .update_many(doc! {"x": 3}, doc! {"$set": { "x": 4}})
        .await
        .unwrap();
    assert_eq!(update_many_results.modified_count, 4);
    assert!(update_many_results.upserted_id.is_none());

    let upsert_results = coll
        .update_one(doc! {"b": 7}, doc! {"$set": { "b": 7 }})
        .upsert(true)
        .await
        .unwrap();
    assert_eq!(upsert_results.modified_count, 0);
    assert!(upsert_results.upserted_id.is_some());
}

#[tokio::test]
#[function_name::named]
async fn delete() {
    let client = TestClient::new().await;
    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    let result = coll
        .insert_many((0i32..5).map(|_| doc! { "x": 3 }).collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(result.inserted_ids.len(), 5);

    let delete_one_result = coll.delete_one(doc! {"x": 3}).await.unwrap();
    assert_eq!(delete_one_result.deleted_count, 1);

    assert_eq!(coll.count_documents(doc! {"x": 3}).await.unwrap(), 4);
    let delete_many_result = coll.delete_many(doc! {"x": 3}).await.unwrap();
    assert_eq!(delete_many_result.deleted_count, 4);
    assert_eq!(coll.count_documents(doc! {"x": 3 }).await.unwrap(), 0);
}

#[tokio::test]
#[function_name::named]
async fn aggregate_out() {
    let client = TestClient::new().await;
    let db = client.database(function_name!());
    let coll = db.collection(function_name!());

    coll.drop().await.unwrap();

    let result = coll
        .insert_many((0i32..5).map(|n| doc! { "x": n }).collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(result.inserted_ids.len(), 5);

    let out_coll = db.collection::<Document>(&format!("{}_1", function_name!()));
    let pipeline = vec![
        doc! {
            "$match": {
                "x": { "$gt": 1 },
            }
        },
        doc! {"$out": out_coll.name()},
    ];
    out_coll.drop().await.unwrap();

    coll.aggregate(pipeline.clone()).await.unwrap();
    assert!(db
        .list_collection_names()
        .await
        .unwrap()
        .into_iter()
        .any(|name| name.as_str() == out_coll.name()));
    out_coll.drop().await.unwrap();

    // check that even with a batch size of 0, a new collection is created.
    coll.aggregate(pipeline).batch_size(0).await.unwrap();
    assert!(db
        .list_collection_names()
        .await
        .unwrap()
        .into_iter()
        .any(|name| name.as_str() == out_coll.name()));
}

#[allow(deprecated)]
fn kill_cursors_sent(client: &EventClient) -> bool {
    !client
        .events
        .get_command_started_events(&["killCursors"])
        .is_empty()
}

#[tokio::test]
#[function_name::named]
async fn kill_cursors_on_drop() {
    let client = TestClient::new().await;
    let db = client.database(function_name!());
    let coll = db.collection(function_name!());

    coll.drop().await.unwrap();

    coll.insert_many(vec![doc! { "x": 1 }, doc! { "x": 2 }])
        .await
        .unwrap();

    #[allow(deprecated)]
    let event_client = EventClient::new().await;
    let coll = event_client
        .database(function_name!())
        .collection::<Document>(function_name!());

    let cursor = coll.find(doc! {}).batch_size(1).await.unwrap();

    assert!(!kill_cursors_sent(&event_client));

    std::mem::drop(cursor);

    // The `Drop` implementation for `Cursor' spawns a back tasks that emits certain events. If the
    // task hasn't been scheduled yet, we may not see the event here. To account for this, we wait
    // for a small amount of time before checking.
    tokio::time::sleep(Duration::from_millis(250)).await;

    assert!(kill_cursors_sent(&event_client));
}

#[tokio::test]
#[function_name::named]
async fn no_kill_cursors_on_exhausted() {
    let client = TestClient::new().await;
    let db = client.database(function_name!());
    let coll = db.collection(function_name!());

    coll.drop().await.unwrap();

    coll.insert_many(vec![doc! { "x": 1 }, doc! { "x": 2 }])
        .await
        .unwrap();

    #[allow(deprecated)]
    let event_client = EventClient::new().await;
    let coll = event_client
        .database(function_name!())
        .collection::<Document>(function_name!());

    let cursor = coll.find(doc! {}).await.unwrap();

    assert!(!kill_cursors_sent(&event_client));

    std::mem::drop(cursor);

    // wait for any tasks to get spawned from `Cursor`'s `Drop`.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!kill_cursors_sent(&event_client));
}

#[allow(clippy::unreadable_literal)]
static LARGE_DOC: Lazy<Document> = Lazy::new(|| {
    doc! {
        "text": "the quick brown fox jumped over the lazy sheep dog",
        "in_reply_to_status_id": 22213321312i64,
        "retweet_count": Bson::Null,
        "contributors": Bson::Null,
        "created_at": ";lkasdf;lkasdfl;kasdfl;kasdkl;ffasdkl;fsadkl;fsad;lfk",
        "geo": Bson::Null,
        "source": "web",
        "coordinates": Bson::Null,
        "in_reply_to_screen_name": "sdafsdafsdaffsdafasdfasdfasdfasdfsadf",
        "truncated": false,
        "entities": {
            "user_mentions": [
                {
                    "indices": [
                        0,
                        9
                    ],
                    "screen_name": "sdafsdaff",
                    "name": "sadfsdaffsadf",
                    "id": 1
                }
            ],
            "urls": [],
            "hashtags": []
        },
        "retweeted": false,
        "place": Bson::Null,
        "user": {
            "friends_count": 123,
            "profile_sidebar_fill_color": "7a7a7a",
            "location": "sdafffsadfsdaf sdaf asdf asdf sdfsdfsdafsdaf asdfff sadf",
            "verified": false,
            "follow_request_sent": Bson::Null,
            "favourites_count": 0,
            "profile_sidebar_border_color": "a3a3a3",
            "profile_image_url": "sadfkljajsdlkffajlksdfjklasdfjlkasdljkf asdjklffjlksadfjlksadfjlksdafjlksdaf",
            "geo_enabled": false,
            "created_at": "sadffasdffsdaf",
            "description": "sdffasdfasdfasdfasdf",
            "time_zone": "Mountain Time (US & Canada)",
            "url": "sadfsdafsadf fsdaljk asjdklf lkjasdf lksadklfffsjdklafjlksdfljksadfjlk",
            "screen_name": "adsfffasdf sdlk;fjjdsakfasljkddfjklasdflkjasdlkfjldskjafjlksadf",
            "notifications": Bson::Null,
            "profile_background_color": "303030",
            "listed_count": 1,
            "lang": "en"
        }
    }
});

#[tokio::test]
#[function_name::named]
async fn large_insert() {
    if std::env::consts::OS != "linux" {
        log_uncaptured("skipping large_insert due to unsupported OS");
        return;
    }

    let docs = vec![LARGE_DOC.clone(); 35000];

    let client = TestClient::new().await;
    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;
    assert_eq!(
        coll.insert_many(docs).await.unwrap().inserted_ids.len(),
        35000
    );
}

/// Returns a vector of documents that cannot be sent in one batch (35000 documents).
/// Includes duplicate _id's across different batches.
fn multibatch_documents_with_duplicate_keys() -> Vec<Document> {
    let large_doc = LARGE_DOC.clone();

    let mut docs: Vec<Document> = Vec::new();
    docs.extend(vec![large_doc.clone(); 7498]);

    docs.push(doc! { "_id": 1 });
    docs.push(doc! { "_id": 1 }); // error in first batch, index 7499

    docs.extend(vec![large_doc.clone(); 14999]);
    docs.push(doc! { "_id": 1 }); // error in second batch, index 22499

    docs.extend(vec![large_doc.clone(); 9999]);
    docs.push(doc! { "_id": 1 }); // error in third batch, index 32499

    docs.extend(vec![large_doc; 2500]);

    assert_eq!(docs.len(), 35000);
    docs
}

#[tokio::test]
#[function_name::named]
async fn large_insert_unordered_with_errors() {
    if std::env::consts::OS != "linux" {
        log_uncaptured("skipping large_insert_unordered_with_errors due to unsupported OS");
        return;
    }

    let docs = multibatch_documents_with_duplicate_keys();

    let client = TestClient::new().await;
    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    match *coll
        .insert_many(docs)
        .ordered(false)
        .await
        .expect_err("should get error")
        .kind
    {
        ErrorKind::BulkWrite(ref failure) => {
            let mut write_errors = failure
                .write_errors
                .clone()
                .expect("should have write errors");
            assert_eq!(write_errors.len(), 3);
            write_errors.sort_by(|lhs, rhs| lhs.index.cmp(&rhs.index));

            assert_eq!(write_errors[0].index, 7499);
            assert_eq!(write_errors[1].index, 22499);
            assert_eq!(write_errors[2].index, 32499);
        }
        e => panic!("expected bulk write error, got {:?} instead", e),
    }
}

#[tokio::test]
#[function_name::named]
async fn large_insert_ordered_with_errors() {
    if std::env::consts::OS != "linux" {
        log_uncaptured("skipping large_insert_ordered_with_errors due to unsupported OS");
        return;
    }

    let docs = multibatch_documents_with_duplicate_keys();

    let client = TestClient::new().await;
    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    match *coll
        .insert_many(docs)
        .ordered(true)
        .await
        .expect_err("should get error")
        .kind
    {
        ErrorKind::BulkWrite(ref failure) => {
            let write_errors = failure
                .write_errors
                .clone()
                .expect("should have write errors");
            assert_eq!(write_errors.len(), 1);
            assert_eq!(write_errors[0].index, 7499);
            assert_eq!(
                coll.count_documents(doc! {})
                    .await
                    .expect("count should succeed"),
                7499
            );
        }
        e => panic!("expected bulk write error, got {:?} instead", e),
    }
}

#[tokio::test]
#[function_name::named]
async fn empty_insert() {
    let client = TestClient::new().await;
    let coll = client
        .database(function_name!())
        .collection::<Document>(function_name!());
    match *coll
        .insert_many(Vec::<Document>::new())
        .await
        .expect_err("should get error")
        .kind
    {
        ErrorKind::InvalidArgument { .. } => {}
        e => panic!("expected argument error, got {:?}", e),
    };
}

#[tokio::test]
async fn find_allow_disk_use() {
    let find_opts = FindOptions::builder().allow_disk_use(true).build();
    allow_disk_use_test(find_opts, Some(true)).await;
}

#[tokio::test]
async fn find_do_not_allow_disk_use() {
    let find_opts = FindOptions::builder().allow_disk_use(false).build();
    allow_disk_use_test(find_opts, Some(false)).await;
}

#[tokio::test]
async fn find_allow_disk_use_not_specified() {
    let find_opts = FindOptions::builder().build();
    allow_disk_use_test(find_opts, None).await;
}

#[function_name::named]
async fn allow_disk_use_test(options: FindOptions, expected_value: Option<bool>) {
    #[allow(deprecated)]
    let event_client = EventClient::new().await;
    if event_client.server_version_lt(4, 3) {
        log_uncaptured("skipping allow_disk_use_test due to server version < 4.3");
        return;
    }
    let coll = event_client
        .database(function_name!())
        .collection::<Document>(function_name!());
    coll.find(doc! {}).with_options(options).await.unwrap();

    #[allow(deprecated)]
    let events = event_client.events.get_command_started_events(&["find"]);
    assert_eq!(events.len(), 1);

    let allow_disk_use = events[0].command.get_bool("allowDiskUse").ok();
    assert_eq!(allow_disk_use, expected_value);
}

#[tokio::test]
#[function_name::named]
async fn ns_not_found_suppression() {
    let client = TestClient::new().await;
    let coll = client.get_coll(function_name!(), function_name!());
    coll.drop().await.expect("drop should not fail");
    coll.drop().await.expect("drop should not fail");
}

async fn delete_hint_test(options: Option<DeleteOptions>, name: &str) {
    #[allow(deprecated)]
    let client = EventClient::new().await;
    let coll = client.database(name).collection::<Document>(name);
    let _: Result<DeleteResult> = coll
        .delete_many(doc! {})
        .with_options(options.clone())
        .await;

    #[allow(deprecated)]
    let events = client.events.get_command_started_events(&["delete"]);
    assert_eq!(events.len(), 1);

    let event_hint = events[0].command.get_array("deletes").unwrap()[0]
        .as_document()
        .unwrap()
        .get("hint")
        .cloned()
        .map(|bson| bson::from_bson(bson).unwrap());
    let expected_hint = options.and_then(|options| options.hint);
    assert_eq!(event_hint, expected_hint);
}

#[tokio::test]
#[function_name::named]
async fn delete_hint_keys_specified() {
    let options = DeleteOptions::builder().hint(Hint::Keys(doc! {})).build();
    delete_hint_test(Some(options), function_name!()).await;
}

#[tokio::test]
#[function_name::named]
async fn delete_hint_string_specified() {
    let options = DeleteOptions::builder()
        .hint(Hint::Name(String::new()))
        .build();
    delete_hint_test(Some(options), function_name!()).await;
}

#[tokio::test]
#[function_name::named]
async fn delete_hint_not_specified() {
    delete_hint_test(None, function_name!()).await;
}

async fn find_one_and_delete_hint_test(options: Option<FindOneAndDeleteOptions>, name: &str) {
    #[allow(deprecated)]
    let client = EventClient::new().await;

    let req = VersionReq::parse(">= 4.2").unwrap();
    if options.is_some() && !req.matches(&client.server_version) {
        log_uncaptured("skipping find_one_and_delete_hint_test due to test configuration");
        return;
    }

    let coll = client.database(name).collection(name);
    let _: Result<Option<Document>> = coll
        .find_one_and_delete(doc! {})
        .with_options(options.clone())
        .await;

    #[allow(deprecated)]
    let events = client.events.get_command_started_events(&["findAndModify"]);
    assert_eq!(events.len(), 1);

    let event_hint = events[0]
        .command
        .get("hint")
        .cloned()
        .map(|bson| bson::from_bson(bson).unwrap());
    let expected_hint = options.and_then(|options| options.hint);
    assert_eq!(event_hint, expected_hint);
}

#[tokio::test]
#[function_name::named]
async fn find_one_and_delete_hint_keys_specified() {
    let options = FindOneAndDeleteOptions::builder()
        .hint(Hint::Keys(doc! {}))
        .build();
    find_one_and_delete_hint_test(Some(options), function_name!()).await;
}

#[tokio::test]
#[function_name::named]
async fn find_one_and_delete_hint_string_specified() {
    let options = FindOneAndDeleteOptions::builder()
        .hint(Hint::Name(String::new()))
        .build();
    find_one_and_delete_hint_test(Some(options), function_name!()).await;
}

#[tokio::test]
#[function_name::named]
async fn find_one_and_delete_hint_not_specified() {
    find_one_and_delete_hint_test(None, function_name!()).await;
}

#[tokio::test]
#[function_name::named]
async fn find_one_and_delete_hint_server_version() {
    #[allow(deprecated)]
    let client = EventClient::new().await;
    let coll = client
        .database(function_name!())
        .collection::<Document>("coll");

    let res = coll
        .find_one_and_delete(doc! {})
        .hint(Hint::Name(String::new()))
        .await;

    let req1 = VersionReq::parse("< 4.2").unwrap();
    let req2 = VersionReq::parse("4.2.*").unwrap();
    if req1.matches(&client.server_version) {
        let error = res.expect_err("find one and delete should fail");
        assert!(matches!(*error.kind, ErrorKind::InvalidArgument { .. }));
    } else if req2.matches(&client.server_version) {
        let error = res.expect_err("find one and delete should fail");
        assert!(matches!(*error.kind, ErrorKind::Command { .. }));
    } else {
        assert!(res.is_ok());
    }
}

#[tokio::test]
#[function_name::named]
async fn no_read_preference_to_standalone() {
    #[allow(deprecated)]
    let client = EventClient::new().await;

    if !client.is_standalone() {
        log_uncaptured("skipping no_read_preference_to_standalone due to test topology");
        return;
    }

    client
        .database(function_name!())
        .collection::<Document>(function_name!())
        .find_one(doc! {})
        .selection_criteria(SelectionCriteria::ReadPreference(
            ReadPreference::SecondaryPreferred {
                options: Default::default(),
            },
        ))
        .await
        .unwrap();

    #[allow(deprecated)]
    let mut events = client.events.clone();
    #[allow(deprecated)]
    let command_started = events.get_successful_command_execution("find").0;

    assert!(!command_started.command.contains_key("$readPreference"));
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct UserType {
    x: i32,
    str: String,
}

#[tokio::test]
#[function_name::named]
async fn typed_insert_one() {
    let client = TestClient::new().await;

    let coll = client
        .init_db_and_typed_coll(function_name!(), function_name!())
        .await;
    let insert_data = UserType {
        x: 1,
        str: "a".into(),
    };
    insert_one_and_find(&coll, insert_data).await;

    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    struct OtherType {
        b: bool,
        nums: Vec<i32>,
    }

    let coll = coll.clone_with_type();
    let insert_data = OtherType {
        b: true,
        nums: vec![1, 2, 3],
    };
    insert_one_and_find(&coll, insert_data).await;
}

async fn insert_one_and_find<T>(coll: &Collection<T>, insert_data: T)
where
    T: Serialize + DeserializeOwned + Clone + PartialEq + Debug + Unpin + Send + Sync,
{
    coll.insert_one(insert_data.clone()).await.unwrap();
    let result = coll
        .find_one(to_document(&insert_data).unwrap())
        .await
        .unwrap();
    match result {
        Some(actual) => {
            assert_eq!(actual, insert_data);
        }
        None => panic!("find_one should return a value"),
    }
}

#[tokio::test]
#[function_name::named]
async fn typed_insert_many() {
    let client = TestClient::new().await;
    let coll = client
        .init_db_and_typed_coll(function_name!(), function_name!())
        .await;

    let insert_data = vec![
        UserType {
            x: 2,
            str: "a".into(),
        },
        UserType {
            x: 2,
            str: "b".into(),
        },
    ];
    coll.insert_many(insert_data.clone()).await.unwrap();

    let actual: Vec<UserType> = coll
        .find(doc! { "x": 2 })
        .sort(doc! { "x": 1 })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(actual, insert_data);
}

#[tokio::test]
#[function_name::named]
async fn typed_find_one_and_replace() {
    let client = TestClient::new().await;
    let coll = client
        .init_db_and_typed_coll(function_name!(), function_name!())
        .await;

    let insert_data = UserType {
        x: 1,
        str: "a".into(),
    };
    coll.insert_one(insert_data.clone()).await.unwrap();

    let replacement = UserType {
        x: 2,
        str: "b".into(),
    };
    let result = coll
        .find_one_and_replace(doc! { "x": 1 }, replacement.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, insert_data);

    let result = coll.find_one(doc! { "x": 2 }).await.unwrap().unwrap();
    assert_eq!(result, replacement);
}

#[tokio::test]
#[function_name::named]
async fn typed_replace_one() {
    let client = TestClient::new().await;
    let coll = client
        .init_db_and_typed_coll(function_name!(), function_name!())
        .await;

    let insert_data = UserType {
        x: 1,
        str: "a".into(),
    };
    let replacement = UserType {
        x: 2,
        str: "b".into(),
    };
    coll.insert_one(insert_data).await.unwrap();
    coll.replace_one(doc! { "x": 1 }, replacement.clone())
        .await
        .unwrap();

    let result = coll.find_one(doc! { "x": 2 }).await.unwrap().unwrap();
    assert_eq!(result, replacement);
}

#[tokio::test]
#[function_name::named]
async fn typed_returns() {
    let client = TestClient::new().await;
    let coll = client
        .init_db_and_typed_coll(function_name!(), function_name!())
        .await;

    let insert_data = UserType {
        x: 1,
        str: "a".into(),
    };
    coll.insert_one(insert_data.clone()).await.unwrap();

    let result = coll
        .find_one_and_update(doc! { "x": 1 }, doc! { "$inc": { "x": 1 } })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, insert_data);

    let result = coll
        .find_one_and_delete(doc! { "x": 2 })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        result,
        UserType {
            x: 2,
            str: "a".into()
        }
    );
}

#[tokio::test]
#[function_name::named]
async fn count_documents_with_wc() {
    let mut options = get_client_options().await.clone();
    options.write_concern = WriteConcern::builder()
        .w(Acknowledgment::Majority)
        .journal(true)
        .build()
        .into();

    let client = TestClient::with_options(Some(options)).await;
    let coll = client
        .database(function_name!())
        .collection(function_name!());

    coll.insert_one(doc! {}).await.unwrap();

    coll.count_documents(doc! {})
        .await
        .expect("count_documents should succeed");
}

#[tokio::test]
#[function_name::named]
async fn collection_options_inherited() {
    #[allow(deprecated)]
    let client = EventClient::new().await;

    let read_concern = ReadConcern::majority();
    let selection_criteria = SelectionCriteria::ReadPreference(ReadPreference::Secondary {
        options: Default::default(),
    });

    let options = CollectionOptions::builder()
        .read_concern(read_concern)
        .selection_criteria(selection_criteria)
        .build();
    let coll = client
        .database(function_name!())
        .collection_with_options::<Document>(function_name!(), options);

    coll.find(doc! {}).await.unwrap();
    assert_options_inherited(&client, "find").await;

    coll.find_one(doc! {}).await.unwrap();
    assert_options_inherited(&client, "find").await;

    coll.count_documents(doc! {}).await.unwrap();
    assert_options_inherited(&client, "aggregate").await;
}

#[allow(deprecated)]
async fn assert_options_inherited(client: &EventClient, command_name: &str) {
    let events = client.events.get_command_started_events(&[command_name]);
    let event = events.iter().last().unwrap();
    assert!(event.command.contains_key("readConcern"));
    assert_eq!(
        event.command.contains_key("$readPreference"),
        !client.is_standalone()
    );
}

#[tokio::test]
#[function_name::named]
async fn drop_skip_serializing_none() {
    let client = TestClient::new().await;
    let coll: Collection<Document> = client
        .database(function_name!())
        .collection(function_name!());
    let options = DropCollectionOptions::builder().build();
    assert!(coll.drop().with_options(options).await.is_ok());
}

#[tokio::test]
#[function_name::named]
async fn collection_generic_bounds() {
    #[derive(Deserialize)]
    struct Foo;

    let client = TestClient::new().await;

    // ensure this code successfully compiles
    let coll: Collection<Foo> = client
        .database(function_name!())
        .collection(function_name!());
    let _result: Result<Option<Foo>> = coll.find_one(doc! {}).await;

    #[derive(Serialize)]
    struct Bar;

    // ensure this code successfully compiles
    let coll: Collection<Bar> = client
        .database(function_name!())
        .collection(function_name!());
    let _result = coll.insert_one(Bar {}).await;
}

/// Verify that a cursor with multiple batches whose last batch isn't full
/// iterates without errors.
#[tokio::test]
async fn cursor_batch_size() {
    let client = TestClient::new().await;
    let coll = client
        .init_db_and_coll("cursor_batch_size", "cursor_batch_size")
        .await;

    let doc = Document::new();
    coll.insert_many(vec![&doc; 10]).await.unwrap();

    let opts = FindOptions::builder().batch_size(3).build();
    let cursor_no_session = coll.find(doc! {}).with_options(opts.clone()).await.unwrap();
    let docs: Vec<_> = cursor_no_session.try_collect().await.unwrap();
    assert_eq!(docs.len(), 10);

    // test session cursors
    if client.is_standalone() {
        log_uncaptured("skipping cursor_batch_size due to standalone topology");
        return;
    }
    let mut session = client.start_session().await.unwrap();
    let mut cursor = coll
        .find(doc! {})
        .with_options(opts.clone())
        .session(&mut session)
        .await
        .unwrap();
    let mut docs = Vec::new();
    while let Some(doc) = cursor.next(&mut session).await {
        docs.push(doc.unwrap());
    }
    assert_eq!(docs.len(), 10);

    let mut cursor = coll
        .find(doc! {})
        .with_options(opts)
        .session(&mut session)
        .await
        .unwrap();
    let docs: Vec<_> = cursor.stream(&mut session).try_collect().await.unwrap();
    assert_eq!(docs.len(), 10);
}

/// Test that the driver gracefully handles cases where the server returns invalid UTF-8 in error
/// messages. See SERVER-24007 and related tickets for details.
#[tokio::test]
async fn invalid_utf8_response() {
    let client = TestClient::new().await;
    let coll = client
        .init_db_and_coll("invalid_uft8_handling", "invalid_uft8_handling")
        .await;

    let index_model = IndexModel::builder()
        .keys(doc! {"name": 1})
        .options(IndexOptions::builder().unique(true).build())
        .build();
    coll.create_index(index_model)
        .await
        .expect("creating an index should succeed");

    // a document containing a long string with multi-byte unicode characters. taken from a user
    // repro in RUBY-2560.
    let long_unicode_str_doc = doc! {"name":  "(╯°□°)╯︵ ┻━┻(╯°□°)╯︵ ┻━┻(╯°□°)╯︵ ┻━┻(╯°□°)╯︵ ┻━┻(╯°□°)╯︵ ┻━┻(╯°□°)╯︵ ┻━┻"};
    coll.insert_one(&long_unicode_str_doc)
        .await
        .expect("first insert of document should succeed");

    // test triggering an invalid error message via an insert_one.
    let insert_err = coll
        .insert_one(&long_unicode_str_doc)
        .await
        .expect_err("second insert of document should fail")
        .kind;
    assert_duplicate_key_error_with_utf8_replacement(&insert_err);

    // test triggering an invalid error message via an insert_many.
    let insert_err = coll
        .insert_many([&long_unicode_str_doc])
        .await
        .expect_err("second insert of document should fail")
        .kind;
    assert_duplicate_key_error_with_utf8_replacement(&insert_err);

    // test triggering an invalid error message via an update_one.
    coll.insert_one(doc! {"x": 1})
        .await
        .expect("inserting new document should succeed");

    let update_err = coll
        .update_one(doc! {"x": 1}, doc! {"$set": &long_unicode_str_doc})
        .await
        .expect_err("update setting duplicate key should fail")
        .kind;
    assert_duplicate_key_error_with_utf8_replacement(&update_err);

    // test triggering an invalid error message via an update_many.
    let update_err = coll
        .update_many(doc! {"x": 1}, doc! {"$set": &long_unicode_str_doc})
        .await
        .expect_err("update setting duplicate key should fail")
        .kind;
    assert_duplicate_key_error_with_utf8_replacement(&update_err);

    // test triggering an invalid error message via a replace_one.
    let replace_err = coll
        .replace_one(doc! {"x": 1}, &long_unicode_str_doc)
        .await
        .expect_err("replacement with duplicate key should fail")
        .kind;
    assert_duplicate_key_error_with_utf8_replacement(&replace_err);
}

/// Check that we successfully decoded a duplicate key error and that the error message contains the
/// unicode replacement character, meaning we gracefully handled the invalid UTF-8.
fn assert_duplicate_key_error_with_utf8_replacement(error: &ErrorKind) {
    match error {
        ErrorKind::Write(ref failure) => match failure {
            WriteFailure::WriteError(err) => {
                assert_eq!(err.code, 11000);
                assert!(err.message.contains('�'));
            }
            e => panic!("expected WriteFailure::WriteError, got {:?} instead", e),
        },
        ErrorKind::BulkWrite(ref failure) => match &failure.write_errors {
            Some(write_errors) => {
                assert_eq!(write_errors.len(), 1);
                assert_eq!(write_errors[0].code, 11000);
                assert!(write_errors[0].message.contains('�'));
            }
            None => panic!(
                "expected BulkWriteFailure containing write errors, got {:?} instead",
                failure
            ),
        },
        e => panic!(
            "expected ErrorKind::Write or ErrorKind::BulkWrite, got {:?} instead",
            e
        ),
    }
}

#[test]
fn test_namespace_fromstr() {
    let t: Namespace = "something.somethingelse".parse().unwrap();
    assert_eq!(t.db, "something");
    assert_eq!(t.coll, "somethingelse");
    let t2: Result<Namespace> = "blahblah".parse();
    assert!(t2.is_err());
    let t: Namespace = "something.something.else".parse().unwrap();
    assert_eq!(t.db, "something");
    assert_eq!(t.coll, "something.else");
}

#[tokio::test]
async fn configure_human_readable_serialization() {
    #[derive(Deserialize)]
    struct StringOrBytes(String);

    impl Serialize for StringOrBytes {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            if serializer.is_human_readable() {
                serializer.serialize_str(&self.0)
            } else {
                serializer.serialize_bytes(self.0.as_bytes())
            }
        }
    }

    #[derive(Deserialize, Serialize)]
    struct Data {
        id: u32,
        s: StringOrBytes,
    }

    let client = TestClient::new().await;

    let collection_options = CollectionOptions::builder()
        .human_readable_serialization(false)
        .build();

    let non_human_readable_collection: Collection<Data> = client
        .database("db")
        .collection_with_options("nonhumanreadable", collection_options);
    non_human_readable_collection.drop().await.unwrap();

    non_human_readable_collection
        .insert_one(Data {
            id: 0,
            s: StringOrBytes("non human readable!".into()),
        })
        .await
        .unwrap();

    // The inserted bytes will not deserialize to StringOrBytes properly, so find as a document
    // instead.
    let document_collection = non_human_readable_collection.clone_with_type::<Document>();
    let doc = document_collection
        .find_one(doc! { "id": 0 })
        .await
        .unwrap()
        .unwrap();
    assert!(doc.get_binary_generic("s").is_ok());

    non_human_readable_collection
        .replace_one(
            doc! { "id": 0 },
            Data {
                id: 1,
                s: StringOrBytes("non human readable!".into()),
            },
        )
        .await
        .unwrap();

    let doc = document_collection
        .find_one(doc! { "id": 1 })
        .await
        .unwrap()
        .unwrap();
    assert!(doc.get_binary_generic("s").is_ok());

    let collection_options = CollectionOptions::builder()
        .human_readable_serialization(true)
        .build();

    let human_readable_collection: Collection<Data> = client
        .database("db")
        .collection_with_options("humanreadable", collection_options);
    human_readable_collection.drop().await.unwrap();

    human_readable_collection
        .insert_one(Data {
            id: 0,
            s: StringOrBytes("human readable!".into()),
        })
        .await
        .unwrap();

    // Proper deserialization to a string demonstrates that the data was correctly serialized as a
    // string.
    human_readable_collection
        .find_one(doc! { "id": 0 })
        .await
        .unwrap();

    human_readable_collection
        .replace_one(
            doc! { "id": 0 },
            Data {
                id: 1,
                s: StringOrBytes("human readable!".into()),
            },
        )
        .await
        .unwrap();

    human_readable_collection
        .find_one(doc! { "id": 1 })
        .await
        .unwrap();
}

#[tokio::test]
async fn insert_many_document_sequences() {
    if cfg!(feature = "in-use-encryption-unstable") {
        log_uncaptured(
            "skipping insert_many_document_sequences: auto-encryption does not support document \
             sequences",
        );
        return;
    }

    let buffer = EventBuffer::new();
    let client = Client::test_builder()
        .event_buffer(buffer.clone())
        .build()
        .await;
    #[allow(deprecated)]
    let mut subscriber = buffer.subscribe();

    let max_object_size = client.server_info.max_bson_object_size;
    let max_message_size = client.server_info.max_message_size_bytes;

    let collection = client
        .database("insert_many_document_sequences")
        .collection::<RawDocumentBuf>("insert_many_document_sequences");
    collection.drop().await.unwrap();

    // A payload with > max_bson_object_size bytes but < max_message_size bytes should require only
    // one round trip
    let docs = vec![
        rawdoc! { "s": "a".repeat((max_object_size / 2) as usize) },
        rawdoc! { "s": "b".repeat((max_object_size / 2) as usize) },
    ];
    collection.insert_many(docs).await.unwrap();

    let (started, _) = subscriber
        .wait_for_successful_command_execution(Duration::from_millis(500), "insert")
        .await
        .expect("did not observe successful command events for insert");
    let insert_documents = started.command.get_array("documents").unwrap();
    assert_eq!(insert_documents.len(), 2);

    // Build up a list of documents that exceeds max_message_size
    let mut docs = Vec::new();
    let mut size = 0;
    while size <= max_message_size {
        // Leave some room for key/metadata bytes in document
        let string_length = max_object_size - 500;
        let doc = rawdoc! { "s": "a".repeat(string_length as usize) };
        size += doc.as_bytes().len() as i32;
        docs.push(doc);
    }
    let total_docs = docs.len();
    collection.insert_many(docs).await.unwrap();

    let (first_started, _) = subscriber
        .wait_for_successful_command_execution(Duration::from_millis(500), "insert")
        .await
        .expect("did not observe successful command events for insert");
    let first_batch_len = first_started.command.get_array("documents").unwrap().len();
    assert!(first_batch_len < total_docs);

    let (second_started, _) = subscriber
        .wait_for_successful_command_execution(Duration::from_millis(500), "insert")
        .await
        .expect("did not observe successful command events for insert");
    let second_batch_len = second_started.command.get_array("documents").unwrap().len();
    assert_eq!(first_batch_len + second_batch_len, total_docs);
}
