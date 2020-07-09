mod test_file;

use futures::stream::TryStreamExt;

use test_file::{Result, TestFile};

use crate::{
    bson::{doc, Document},
    error::ErrorKind,
    options::FindOptions,
    test::{assert_matches, run_spec_test, EventClient, LOCK},
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

            let db_name = test_case.description.replace('$', "%").replace(' ', "_");
            let coll_name = "coll";

            let coll = client.init_db_and_coll(&db_name, &coll_name).await;
            coll.insert_many(test_file.data.clone(), None)
                .await
                .expect(&test_case.description);

            if let Some(ref fail_point) = test_case.fail_point {
                client
                    .database("admin")
                    .run_command(fail_point.clone(), None)
                    .await
                    .unwrap();
            }

            let result = client
                .run_collection_operation(&test_case.operation, &db_name, &coll_name, None)
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
                        let labels = match error.kind.as_ref() {
                            ErrorKind::CommandError(error) => &error.labels,
                            e => panic!("expected command error, got {:?}", e),
                        };

                        if let Some(contain) = expected_labels.error_labels_contain {
                            contain.iter().for_each(|label| {
                                assert!(
                                    labels.contains(label),
                                    "error labels should include {}",
                                    label,
                                );
                            });
                        }

                        if let Some(omit) = expected_labels.error_labels_omit {
                            omit.iter().for_each(|label| {
                                assert!(
                                    !labels.contains(label),
                                    "error labels should not include {}",
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

            let coll = client.init_db_and_coll(&db_name, &coll_name).await;
            let options = FindOptions::builder().sort(doc! { "_id": 1 }).build();
            let actual_data: Vec<Document> = coll
                .find(None, options)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(test_case.outcome.collection.data, actual_data);
        }
    }

    let _guard = LOCK.run_exclusively().await;
    run_spec_test(&["retryable-writes"], run_test).await;
}
