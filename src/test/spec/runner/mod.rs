mod operation;
mod test_event;
mod test_file;

use std::time::Duration;

use futures::stream::TryStreamExt;

use crate::{
    bson::{doc, Document},
    operation::RunCommand,
    options::FindOptions,
    test::{assert_matches, util::EventClient, TestClient, CLIENT_OPTIONS},
};

pub use self::{
    operation::AnyTestOperation,
    test_event::TestEvent,
    test_file::{OperationObject, TestData, TestFile},
};

const SKIPPED_OPERATIONS: &[&str] = &[
    "bulkWrite",
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

pub async fn run_v2_test(test_file: TestFile) {
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
        if TestClient::new().await.is_sharded() && test_case.use_multiple_mongoses != Some(true) {
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

        if test_case
            .description
            .contains("Aggregate with $listLocalSessions")
        {
            let mut session = client
                .start_implicit_session_with_timeout(Duration::from_secs(60 * 60))
                .await;
            let op = RunCommand::new(db_name.clone(), doc! { "ping": 1 }, None).unwrap();
            client
                .execute_operation_with_session(op, &mut session)
                .await
                .unwrap();
        }

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
                Some(OperationObject::Client) => client.run_client_operation(&operation).await,
                Some(OperationObject::Database) => {
                    client.run_database_operation(&operation, &db_name).await
                }
                Some(OperationObject::Collection) | None => {
                    client
                        .run_collection_operation(
                            &operation,
                            &db_name,
                            &coll_name,
                            operation.collection_options.clone(),
                        )
                        .await
                }
                Some(OperationObject::GridfsBucket) => {
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
                assert_matches(&result, &expected_result, Some(description));
            }

            events.append(&mut operation_events);
        }

        if let Some(expectations) = test_case.expectations {
            assert_eq!(events.len(), expectations.len());
            for (actual_event, expected_event) in events.iter().zip(expectations.iter()) {
                assert_matches(actual_event, expected_event, None);
            }
        }

        if let Some(outcome) = test_case.outcome {
            let coll_name = match outcome.collection.name {
                Some(name) => name,
                None => coll_name,
            };
            let coll = client.database(&db_name).collection(&coll_name);
            let options = FindOptions::builder().sort(doc! { "_id": 1 }).build();
            let actual_data: Vec<Document> = coll
                .find(None, options)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(outcome.collection.data, actual_data);
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
