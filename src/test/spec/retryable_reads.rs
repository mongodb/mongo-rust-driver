use bson::{Bson, Document};

use crate::test::{
    assert_matches,
    run_spec_test,
    util::{parse_version, AnyTestOperation, EventClient, TestEvent, TestFile},
    CLIENT_OPTIONS,
    LOCK,
};

const SKIPPED_OPERATIONS: &[&str] = &[
    "count",
    "download",
    "download_by_name",
    "listCollectionObjects",
    "listDatabaseObjects",
    "listIndexNames",
    "listIndexes",
    "mapReduce",
    "watch",
];

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    async fn run_test(test_file: TestFile) {
        let has_skipped_op = test_file.tests.iter().any(|test_case| {
            test_case
                .operations
                .iter()
                .any(|op| SKIPPED_OPERATIONS.contains(&op.name.as_str()))
        });
        if has_skipped_op {
            return;
        }

        for test_case in test_file.tests {
            if let Some(skip_reason) = test_case.skip_reason {
                println!("Skipping {}: {}", test_case.description, skip_reason);
                continue;
            }

            let client = match test_case.client_options {
                Some(client_options) => {
                    let mut options = CLIENT_OPTIONS.clone();
                    options.retry_reads = Some(client_options.get_bool("retryReads").unwrap());
                    EventClient::with_options(options).await
                }
                None => EventClient::new().await,
            };

            if let Some(ref run_on) = test_file.run_on {
                let can_run_on = run_on.iter().any(|run_on| {
                    if let Some(ref min_server_version) = run_on.min_server_version {
                        let (major, minor) = parse_version(min_server_version);
                        if client.server_version_lt(major, minor) {
                            return false;
                        }
                    }
                    if let Some(ref max_server_version) = run_on.max_server_version {
                        let (major, minor) = parse_version(max_server_version);
                        if client.server_version_gt(major, minor) {
                            return false;
                        }
                    }
                    if let Some(ref topology) = run_on.topology {
                        if !topology.contains(&client.topology()) {
                            return false;
                        }
                    }
                    true
                });
                if !can_run_on {
                    println!("Skipping {}", test_case.description);
                    continue;
                }
            }

            match test_case.fail_point {
                Some(_) => {
                    let _guard = LOCK.run_exclusively().await;
                }
                _ => {
                    let _guard = LOCK.run_concurrently().await;
                }
            }

            let db_name = match test_file.database_name {
                Some(ref db_name) => db_name.clone(),
                None => test_case.description.replace('$', "%").replace(' ', "_"),
            };

            let coll_name = match test_file.collection_name {
                Some(ref coll_name) => coll_name.clone(),
                None => String::from("coll"),
            };

            let coll = client.init_db_and_coll(&db_name, &coll_name).await;

            if let Some(ref data) = test_file.data {
                let data: Vec<Document> = data
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|doc| doc.as_document().unwrap().clone())
                    .collect();
                if !data.is_empty() {
                    coll.insert_many(data, None)
                        .await
                        .expect(&test_case.description);
                }
            }

            if let Some(fail_point) = test_case.fail_point {
                client
                    .database("admin")
                    .run_command(fail_point, None)
                    .await
                    .unwrap();
            }

            let mut events: Vec<TestEvent> = Vec::new();
            for operation in test_case.operations {
                let test_operation: AnyTestOperation =
                    bson::from_bson(Bson::Document(operation.as_document())).unwrap();
                let result = match operation.object.as_ref().map(String::as_ref) {
                    Some("client") => client.run_client_operation(&test_operation).await,
                    Some("database") => {
                        client
                            .run_database_operation(&test_operation, &db_name)
                            .await
                    }
                    Some("collection") | None => {
                        client
                            .run_collection_operation(&test_operation, &db_name, &coll_name)
                            .await
                    }
                    Some(op_type) => panic!("unsupported operation type {}", op_type),
                };
                let mut operation_events: Vec<TestEvent> = client
                    .collect_events(&test_operation, false)
                    .into_iter()
                    .map(Into::into)
                    .collect();

                if let Some(error) = operation.error {
                    if error {
                        assert!(result.is_err());
                    }
                }
                if let Some(expected_result) = operation.result {
                    assert_eq!(result.unwrap().unwrap(), expected_result);
                }

                events.append(&mut operation_events);
            }

            if let Some(expectations) = test_case.expectations {
                assert_eq!(events.len(), expectations.len());
                for (actual_event, expected_event) in events.iter().zip(expectations.iter()) {
                    assert_matches(actual_event, expected_event, None);
                }
            }
        }
    }

    run_spec_test(&["retryable-reads", "tests"], run_test).await;
}
