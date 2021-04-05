mod test_file;

use std::time::Duration;

use futures::stream::TryStreamExt;
use semver::VersionReq;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use test_file::{Result, TestFile};

use crate::{
    bson::{doc, Document},
    error::ErrorKind,
    options::{ClientOptions, FindOptions, InsertManyOptions},
    test::{
        assert_matches,
        run_spec_test,
        util::get_default_name,
        EventClient,
        TestClient,
        CLIENT_OPTIONS,
        LOCK,
    },
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_spec_tests() {
    async fn run_test(test_file: TestFile) {
        for test_case in test_file.tests {
            if test_case.operation.name == "bulkWrite" {
                continue;
            }

            let options = test_case.client_options.map(|mut opts| {
                opts.hosts = CLIENT_OPTIONS.hosts.clone();
                opts
            });
            let client = EventClient::with_additional_options(
                options,
                Some(Duration::from_millis(50)),
                test_case.use_multiple_mongoses,
                None,
            )
            .await;

            if let Some(ref run_on) = test_file.run_on {
                let can_run_on = run_on.iter().any(|run_on| run_on.can_run_on(&client));
                if !can_run_on {
                    println!("Skipping {}", test_case.description);
                    continue;
                }
            }

            let db_name = get_default_name(&test_case.description);
            let coll_name = "coll";

            let coll = client.init_db_and_coll(&db_name, &coll_name).await;

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

            let coll = client.database(&db_name).collection(&coll_name);
            let result = test_case.operation.execute_on_collection(&coll, None).await;

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
                                    error.contains_label(label),
                                    "{}: error labels should include {}",
                                    description,
                                    label,
                                );
                            });
                        }

                        if let Some(omit) = expected_labels.error_labels_omit {
                            omit.iter().for_each(|label| {
                                assert!(
                                    !error.contains_label(label),
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

            let coll = client.get_coll(&db_name, &coll_name);
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

    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test(&["retryable-writes"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn transaction_ids_excluded() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;

    if !(client.is_replica_set() || client.is_sharded()) {
        return;
    }

    let coll = client.init_db_and_coll(function_name!(), "coll").await;

    let excludes_txn_number = |command_name: &str| -> bool {
        let (started, _) = client.get_successful_command_execution(command_name);
        !started.command.contains_key("txnNumber")
    };

    coll.update_many(doc! {}, doc! { "$set": doc! { "x": 1 } }, None)
        .await
        .unwrap();
    assert!(excludes_txn_number("update"));

    coll.delete_many(doc! {}, None).await.unwrap();
    assert!(excludes_txn_number("delete"));

    coll.aggregate(
        vec![
            doc! { "$match": doc! { "x": 1 } },
            doc! { "$out": "other_coll" },
        ],
        None,
    )
    .await
    .unwrap();
    assert!(excludes_txn_number("aggregate"));

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
        assert!(excludes_txn_number("aggregate"));
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn transaction_ids_included() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;

    if !(client.is_replica_set() || client.is_sharded()) {
        return;
    }

    let coll = client.init_db_and_coll(function_name!(), "coll").await;

    let includes_txn_number = |command_name: &str| -> bool {
        let (started, _) = client.get_successful_command_execution(command_name);
        started.command.contains_key("txnNumber")
    };

    coll.insert_one(doc! { "x": 1 }, None).await.unwrap();
    assert!(includes_txn_number("insert"));

    coll.update_one(doc! {}, doc! { "$set": doc! { "x": 1 } }, None)
        .await
        .unwrap();
    assert!(includes_txn_number("update"));

    coll.replace_one(doc! {}, doc! { "x": 1 }, None)
        .await
        .unwrap();
    assert!(includes_txn_number("update"));

    coll.delete_one(doc! {}, None).await.unwrap();
    assert!(includes_txn_number("delete"));

    coll.find_one_and_delete(doc! {}, None).await.unwrap();
    assert!(includes_txn_number("findAndModify"));

    coll.find_one_and_replace(doc! {}, doc! { "x": 1 }, None)
        .await
        .unwrap();
    assert!(includes_txn_number("findAndModify"));

    coll.find_one_and_update(doc! {}, doc! { "$set": doc! { "x": 1 } }, None)
        .await
        .unwrap();
    assert!(includes_txn_number("findAndModify"));

    let options = InsertManyOptions::builder().ordered(true).build();
    coll.insert_many(vec![doc! { "x": 1 }], options)
        .await
        .unwrap();
    assert!(includes_txn_number("insert"));

    let options = InsertManyOptions::builder().ordered(false).build();
    coll.insert_many(vec![doc! { "x": 1 }], options)
        .await
        .unwrap();
    assert!(includes_txn_number("insert"));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn mmapv1_error_raised() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

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
    match &err.kind {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn label_not_added_first_read_error() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    label_not_added(false).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn label_not_added_second_read_error() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    label_not_added(true).await;
}

#[function_name::named]
async fn label_not_added(retry_reads: bool) {
    let options = ClientOptions::builder().retry_reads(retry_reads).build();
    let client = TestClient::with_additional_options(Some(options), false).await;

    // Configuring a failpoint is only supported on 4.0+ replica sets and 4.1.5+ sharded clusters.
    let req = VersionReq::parse(">=4.0").unwrap();
    let sharded_req = VersionReq::parse(">=4.1.5").unwrap();
    if client.is_sharded() && !sharded_req.matches(&client.server_version)
        || !req.matches(&client.server_version)
    {
        return;
    }

    let coll = client
        .init_db_and_coll(&format!("{}{}", function_name!(), retry_reads), "coll")
        .await;

    let failpoint = doc! {
        "configureFailPoint": "failCommand",
        "mode": { "times": if retry_reads { 2 } else { 1 } },
        "data": {
            "failCommands": ["find"],
            "errorCode": 11600
        }
    };
    client
        .database("admin")
        .run_command(failpoint, None)
        .await
        .unwrap();

    let err = coll.find(doc! {}, None).await.unwrap_err();

    assert!(!err.contains_label("RetryableWriteError"));
}
