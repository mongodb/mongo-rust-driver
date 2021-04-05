mod event;
mod operation;

use std::convert::Into;

use semver::VersionReq;
use serde::Deserialize;
use tokio::sync::RwLockWriteGuard;

use self::{event::TestEvent, operation::*};
use crate::{
    bson::{Bson, Document},
    options::ClientOptions,
    test::{assert_matches, util::TestClient, EventClient, CLIENT_OPTIONS, LOCK},
};

#[derive(Deserialize)]
struct TestFile {
    data: Vec<Document>,
    collection_name: String,
    database_name: String,
    tests: Vec<TestCase>,
}

#[derive(Deserialize)]
struct TestCase {
    description: String,
    #[serde(rename = "ignore_if_server_version_greater_than", default)]
    max_version: Option<String>,
    #[serde(rename = "ignore_if_server_version_less_than", default)]
    min_version: Option<String>,
    operation: Document,
    expectations: Vec<TestEvent>,
}

async fn run_command_monitoring_test(test_file: TestFile) {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    let client = TestClient::new().await;

    let skipped_tests = vec![
        // uses old count
        "A successful command",
        "A failed command event",
        "A successful command with a non-primary read preference",
        // bulk write not implemented
        "A successful mixed bulk write",
        "A successful unordered bulk write with an unacknowledged write concern",
        // We can't pass this test since it relies on old OP_QUERY behavior (SPEC-1519)
        "A successful find event with a getmore and the server kills the cursor",
    ];

    for test_case in test_file.tests {
        if skipped_tests.iter().any(|st| st == &test_case.description) {
            println!("Skipping {}", test_case.description);
            continue;
        }

        if let Some(ref max_version) = test_case.max_version {
            let req = VersionReq::parse(&format!("<= {}", &max_version)).unwrap();
            if !req.matches(&client.server_version) {
                println!("Skipping {}", test_case.description);
                continue;
            }
        }

        if let Some(ref min_version) = test_case.min_version {
            let req = VersionReq::parse(&format!(">= {}", &min_version)).unwrap();
            if !req.matches(&client.server_version) {
                println!("Skipping {}", test_case.description);
                continue;
            }
        }

        println!("Running {}", test_case.description);

        let operation: AnyTestOperation =
            bson::from_bson(Bson::Document(test_case.operation)).unwrap();

        client
            .init_db_and_coll(&test_file.database_name, &test_file.collection_name)
            .await
            .insert_many(test_file.data.clone(), None)
            .await
            .expect("insert many error");

        let options = ClientOptions::builder()
            .retry_writes(false)
            .hosts(CLIENT_OPTIONS.hosts.clone())
            .build();
        let client =
            EventClient::with_additional_options(Some(options), None, Some(false), None).await;

        let events: Vec<TestEvent> = client
            .run_operation_with_events(
                operation,
                &test_file.database_name,
                &test_file.collection_name,
            )
            .await
            .into_iter()
            .map(Into::into)
            .collect();

        assert_eq!(events.len(), test_case.expectations.len());
        for (actual_event, expected_event) in events.iter().zip(test_case.expectations.iter()) {
            assert_matches(actual_event, expected_event, None);
        }
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_monitoring() {
    crate::test::run_spec_test(&["command-monitoring"], run_command_monitoring_test).await;
}
