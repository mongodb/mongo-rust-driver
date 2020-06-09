use crate::{
    bson::doc,
    test::{
        assert_matches,
        run_spec_test,
        util::EventClient,
        OperationObject,
        TestClient,
        TestData,
        TestEvent,
        TestFile,
        CLIENT_OPTIONS,
        LOCK,
    },
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

            let mut options = CLIENT_OPTIONS.clone();
            if let Some(client_options) = test_case.client_options {
                options.retry_reads = Some(client_options.get_bool("retryReads").unwrap());
            }
            if TestClient::new().await.is_sharded() && test_case.use_multiple_mongoses != Some(true)
            {
                options.hosts = options.hosts.iter().cloned().take(1).collect();
            }
            let client = EventClient::with_options(options).await;

            if let Some(ref run_on) = test_file.run_on {
                let can_run_on = run_on.iter().any(|run_on| run_on.can_run_on(&client));
                if !can_run_on {
                    println!("Skipping {}", test_case.description);
                    continue;
                }
            }

            let db_name = match test_file.database_name {
                Some(ref db_name) => db_name.clone(),
                None => test_case.description.replace('$', "%").replace(' ', "_"),
            };

            let coll_name = match test_file.collection_name {
                Some(ref coll_name) => coll_name.clone(),
                None => "coll".to_string(),
            };

            let coll = client.init_db_and_coll(&db_name, &coll_name).await;

            if let Some(ref data) = test_file.data {
                match data {
                    TestData::Single(data) => {
                        if !data.is_empty() {
                            coll.insert_many(data.clone(), None)
                                .await
                                .expect(&test_case.description);
                        }
                    }
                    TestData::Many(_) => panic!("{}: invalid data format", &test_case.description),
                }
            }

            if let Some(ref fail_point) = test_case.fail_point {
                client
                    .database("admin")
                    .run_command(fail_point.clone(), None)
                    .await
                    .unwrap();
            }

            let mut events: Vec<TestEvent> = Vec::new();
            for operation in test_case.operations {
                let result = match operation.object {
                    OperationObject::Client => client.run_client_operation(&operation).await,
                    OperationObject::Database => {
                        client.run_database_operation(&operation, &db_name).await
                    }
                    OperationObject::Collection => {
                        client
                            .run_collection_operation(&operation, &db_name, &coll_name)
                            .await
                    }
                    OperationObject::GridfsBucket => {
                        panic!("unsupported operation: {}", operation.name)
                    }
                };
                let mut operation_events: Vec<TestEvent> = client
                    .collect_events(&operation)
                    .into_iter()
                    .map(Into::into)
                    .collect();

                if let Some(error) = operation.error {
                    assert_eq!(
                        result.is_err(),
                        error,
                        "{}: expected error: {}, got {:?}",
                        test_case.description,
                        error,
                        result
                    );
                }
                if let Some(expected_result) = operation.result {
                    let description = &test_case.description;
                    let result = result
                        .unwrap()
                        .unwrap_or_else(|| panic!("{:?}: operation should succeed", description));
                    assert_matches(&expected_result, &result, Some(description));
                }

                events.append(&mut operation_events);
            }

            if let Some(expectations) = test_case.expectations {
                assert_eq!(events.len(), expectations.len());
                for (actual_event, expected_event) in events.iter().zip(expectations.iter()) {
                    assert_matches(actual_event, expected_event, None);
                }
            }

            if test_case.fail_point.is_some() {
                client
                    .database("admin")
                    .run_command(
                        doc! {
                            "configureFailPoint": "failCommand",
                            "mode": "off"
                        },
                        None,
                    )
                    .await
                    .unwrap();
            }
        }
    }

    let _guard = LOCK.run_exclusively().await;
    run_spec_test(&["retryable-reads"], run_test).await;
}
