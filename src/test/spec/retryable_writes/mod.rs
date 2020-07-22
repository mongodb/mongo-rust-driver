mod test_file;

use futures::stream::TryStreamExt;

use test_file::{Result, TestFile};

use crate::{
    bson::{doc, Document},
    concern::{Acknowledgment, ReadConcern, WriteConcern},
    error::ErrorKind,
    options::{CollectionOptions, CreateCollectionOptions, FindOptions, InsertManyOptions},
    test::{assert_matches, run_spec_test, EventClient, TestClient, LOCK},
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_spec_tests() {
    async fn run_test(test_file: TestFile) {
        for test_case in test_file.tests {
            if test_case.operation.name == "bulkWrite" {
                continue;
            }

            let client = EventClient::merge_options(
                test_case.client_options,
                test_case.use_multiple_mongoses,
            )
            .await;

            if let Some(ref run_on) = test_file.run_on {
                let can_run_on = run_on.iter().any(|run_on| run_on.can_run_on(&client));
                if !can_run_on {
                    println!("Skipping {}", test_case.description);
                    continue;
                }
            }

            let mut db_name = test_case.description.replace('$', "%").replace(' ', "_");
            // database names must have fewer than 64 characters
            db_name.truncate(63);
            let coll_name = "coll";

            let write_concern = WriteConcern::builder().w(Acknowledgment::Majority).build();
            let read_concern = ReadConcern::majority();
            let options = CollectionOptions::builder()
                .write_concern(write_concern)
                .read_concern(read_concern)
                .build();
            let coll = client
                .init_db_and_coll_with_options(&db_name, &coll_name, options.clone())
                .await;

            if !test_file.data.is_empty() {
                coll.insert_many(test_file.data.clone(), None)
                    .await
                    .expect(&test_case.description);
            }

            if let Some(ref fail_point) = test_case.fail_point {
                client
                    .database("admin")
                    .run_command(fail_point.clone(), None)
                    .await
                    .unwrap();
            }

            let result = client
                .run_collection_operation(
                    &test_case.operation,
                    &db_name,
                    &coll_name,
                    Some(options.clone()),
                )
                .await;

            if let Some(error) = test_case.outcome.error {
                assert_eq!(
                    error,
                    result.is_err(),
                    "{}: expected error: {}, got {:?}",
                    &test_case.description,
                    error,
                    result
                );
            }

            if let Some(expected_result) = test_case.outcome.result {
                match expected_result {
                    Result::Value(value) => {
                        let description = &test_case.description;
                        let result = result.unwrap().unwrap_or_else(|| {
                            panic!("{:?}: operation should succeed", description)
                        });
                        assert_matches(&result, &value, Some(description));
                    }
                    Result::Labels(expected_labels) => {
                        let error = result.expect_err(&format!(
                            "{:?}: operation should fail",
                            &test_case.description
                        ));

                        let description = &test_case.description;
                        if let Some(contain) = expected_labels.error_labels_contain {
                            contain.iter().for_each(|label| {
                                assert!(
                                    error.labels().contains(label),
                                    "{}: error labels should include {}",
                                    description,
                                    label,
                                );
                            });
                        }

                        if let Some(omit) = expected_labels.error_labels_omit {
                            omit.iter().for_each(|label| {
                                assert!(
                                    !error.labels().contains(label),
                                    "{}: error labels should not include {}",
                                    description,
                                    label,
                                );
                            });
                        }
                    }
                };
            }

            let coll_name = match test_case.outcome.collection.name {
                Some(name) => name,
                None => coll_name.to_string(),
            };

            let coll = client.get_coll_with_options(&db_name, &coll_name, options);
            let options = FindOptions::builder().sort(doc! { "_id": 1 }).build();
            let actual_data: Vec<Document> = coll
                .find(None, options)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(test_case.outcome.collection.data, actual_data);

            if let Some(fail_point) = test_case.fail_point {
                if let Ok("alwaysOn") = fail_point.get_str("mode") {
                    let disable = doc! {
                        "configureFailPoint": fail_point.get_str("configureFailPoint").unwrap(),
                        "mode": "off",
                    };
                    client
                        .database("admin")
                        .run_command(disable, None)
                        .await
                        .unwrap();
                }
            }
        }
    }

    let _guard = LOCK.run_exclusively().await;
    run_spec_test(&["retryable-writes"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn transaction_ids_excluded() {
    let client = EventClient::new().await;

    if !(client.is_replica_set() || client.is_sharded()) {
        return;
    }

    let coll = client.init_db_and_coll(function_name!(), "coll").await;

    let excludes = |command_name: &str| -> bool {
        let (started, _) = client.get_successful_command_execution(command_name);
        !started.command.contains_key("txnNumber")
    };

    coll.update_many(doc! {}, doc! { "$set": doc! { "x": 1 } }, None)
        .await
        .unwrap();
    assert!(excludes("update"));

    coll.delete_many(doc! {}, None).await.unwrap();
    assert!(excludes("delete"));

    coll.aggregate(
        vec![
            doc! { "$match": doc! { "x": 1 } },
            doc! { "$out": "other_coll" },
        ],
        None,
    )
    .await
    .unwrap();
    assert!(excludes("aggregate"));

    let req = semver::VersionReq::parse(">=4.2").unwrap();
    if req.matches(&client.server_version) {
        coll.aggregate(
            vec![
                doc! { "$match": doc! { "x": 1 } },
                doc! { "$merge": "other_coll" },
            ],
            None,
        )
        .await
        .unwrap();
        assert!(excludes("aggregate"));
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn transaction_ids_included() {
    let client = EventClient::new().await;

    if !(client.is_replica_set() || client.is_sharded()) {
        return;
    }

    let coll = client.init_db_and_coll(function_name!(), "coll").await;

    let includes = |command_name: &str| -> bool {
        let (started, _) = client.get_successful_command_execution(command_name);
        started.command.contains_key("txnNumber")
    };

    coll.insert_one(doc! { "x": 1 }, None).await.unwrap();
    assert!(includes("insert"));

    coll.update_one(doc! {}, doc! { "$set": doc! { "x": 1 } }, None)
        .await
        .unwrap();
    assert!(includes("update"));

    coll.replace_one(doc! {}, doc! { "x": 1 }, None)
        .await
        .unwrap();
    assert!(includes("update"));

    coll.delete_one(doc! {}, None).await.unwrap();
    assert!(includes("delete"));

    coll.find_one_and_delete(doc! {}, None).await.unwrap();
    assert!(includes("findAndModify"));

    coll.find_one_and_replace(doc! {}, doc! { "x": 1 }, None)
        .await
        .unwrap();
    assert!(includes("findAndModify"));

    coll.find_one_and_update(doc! {}, doc! { "$set": doc! { "x": 1 } }, None)
        .await
        .unwrap();
    assert!(includes("findAndModify"));

    let options = InsertManyOptions::builder().ordered(true).build();
    coll.insert_many(vec![doc! { "x": 1 }], options)
        .await
        .unwrap();
    assert!(includes("insert"));

    let options = InsertManyOptions::builder().ordered(false).build();
    coll.insert_many(vec![doc! { "x": 1 }], options)
        .await
        .unwrap();
    assert!(includes("insert"));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn mmapv1_error_raised() {
    let client = TestClient::new().await;

    let req = semver::VersionReq::parse("<=4.0").unwrap();
    if !req.matches(&client.server_version) || !client.is_replica_set() {
        return;
    }

    let coll = client.init_db_and_coll(function_name!(), "coll").await;

    let server_status = client
        .database(function_name!())
        .run_command(doc! { "serverStatus": 1 }, None)
        .await
        .unwrap();
    let name = server_status
        .get_document("storageEngine")
        .unwrap()
        .get_str("name")
        .unwrap();
    if name != "mmapv1" {
        return;
    }

    let err = coll.insert_one(doc! { "x": 1 }, None).await.unwrap_err();
    match err.kind.as_ref() {
        ErrorKind::CommandError(err) => {
            assert_eq!(
                err.message,
                "This MongoDB deployment does not support retryable writes. Please add \
                 retryWrites=false to your connection string."
            );
        }
        e => panic!("expected command error, got: {:?}", e),
    }
}
