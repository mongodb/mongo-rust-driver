mod entity;
mod matcher;
mod operation;
mod test_event;
mod test_file;
mod test_runner;

use std::{convert::TryFrom, ffi::OsStr, fs::read_dir, path::PathBuf, time::Duration};

use futures::{future::FutureExt, stream::TryStreamExt};
use semver::Version;
use tokio::sync::RwLockWriteGuard;

use crate::{
    bson::{doc, Document},
    options::{CollectionOptions, FindOptions, ReadConcern, ReadPreference, SelectionCriteria},
    test::{run_single_test, run_spec_test, LOCK},
    RUNTIME,
};

pub use self::{
    entity::{ClientEntity, Entity, SessionEntity, TestCursor},
    matcher::{events_match, results_match},
    operation::{Operation, OperationObject},
    test_event::{ExpectedCmapEvent, ExpectedCommandEvent, ExpectedEvent, ObserveEvent},
    test_file::{
        merge_uri_options,
        CollectionData,
        ExpectError,
        ExpectedEventType,
        TestFile,
        TestFileEntity,
        Topology,
    },
    test_runner::{EntityMap, TestRunner},
};

use self::{operation::Expectation, test_file::TestCase};

static SPEC_VERSIONS: &[Version] = &[
    Version::new(1, 0, 0),
    Version::new(1, 1, 0),
    Version::new(1, 4, 0),
    Version::new(1, 5, 0),
];

const SKIPPED_OPERATIONS: &[&str] = &[
    "bulkWrite",
    "count",
    "download",
    "download_by_name",
    "listCollectionObjects",
    "listDatabaseObjects",
    "mapReduce",
    "watch",
];

pub async fn run_unified_format_test(test_file: TestFile) {
    run_unified_format_test_filtered(test_file, |_| true).await
}

pub async fn run_unified_format_test_filtered(
    test_file: TestFile,
    pred: impl Fn(&TestCase) -> bool,
) {
    let version_matches = SPEC_VERSIONS.iter().any(|req| {
        if req.major != test_file.schema_version.major {
            return false;
        }
        if req.minor < test_file.schema_version.minor {
            return false;
        }
        // patch versions do not affect the test file format
        true
    });
    assert!(
        version_matches,
        "Test runner not compatible with specification version {}",
        &test_file.schema_version
    );

    let mut test_runner = TestRunner::new().await;

    if let Some(requirements) = test_file.run_on_requirements {
        let mut can_run_on = false;
        for requirement in requirements {
            if requirement.can_run_on(&test_runner.internal_client).await {
                can_run_on = true;
            }
        }
        if !can_run_on {
            println!("Client topology not compatible with test");
            return;
        }
    }

    for test_case in test_file.tests {
        if let Some(skip_reason) = test_case.skip_reason {
            println!("Skipping {}: {}", &test_case.description, skip_reason);
            continue;
        }

        if test_case
            .operations
            .iter()
            .any(|op| SKIPPED_OPERATIONS.contains(&op.name.as_str()))
        {
            println!("Skipping {}", &test_case.description);
            continue;
        }

        if !pred(&test_case) {
            println!("Skipping {}", test_case.description);
            continue;
        }

        println!("Running {}", &test_case.description);

        if let Some(requirements) = test_case.run_on_requirements {
            let mut can_run_on = false;
            for requirement in requirements {
                if requirement.can_run_on(&test_runner.internal_client).await {
                    can_run_on = true;
                }
            }
            if !can_run_on {
                println!(
                    "{}: client topology not compatible with test",
                    &test_case.description
                );
                return;
            }
        }

        if let Some(ref initial_data) = test_file.initial_data {
            for data in initial_data {
                test_runner.insert_initial_data(data).await;
            }
        }

        if let Some(ref create_entities) = test_file.create_entities {
            test_runner.populate_entity_map(create_entities).await;
        }

        for operation in test_case.operations {
            test_runner.sync_workers().await;
            match operation.object {
                OperationObject::TestRunner => {
                    operation
                        .execute_test_runner_operation(&mut test_runner)
                        .await;
                }
                OperationObject::Entity(ref id) => {
                    let result = operation
                        .execute_entity_operation(id, &mut test_runner)
                        .await;

                    match &operation.expectation {
                        Expectation::Result {
                            expected_value,
                            save_as_entity,
                        } => {
                            let opt_entity = result.unwrap_or_else(|e| {
                                panic!(
                                    "{} should succeed, but failed with the following error: {}",
                                    operation.name, e
                                )
                            });
                            if expected_value.is_some() || save_as_entity.is_some() {
                                let entity = opt_entity.unwrap_or_else(|| {
                                    panic!("{} did not return an entity", operation.name)
                                });
                                if let Some(expected_bson) = expected_value {
                                    if let Entity::Bson(actual) = &entity {
                                        if let Err(e) = results_match(
                                            Some(actual),
                                            expected_bson,
                                            operation.returns_root_documents(),
                                            Some(&test_runner.entities),
                                        ) {
                                            panic!(
                                                "result mismatch, expected = {:#?}  actual = \
                                                 {:#?}\nmismatch detail: {}",
                                                expected_bson, actual, e
                                            );
                                        }
                                    } else {
                                        panic!(
                                            "Incorrect entity type returned from {}, expected BSON",
                                            operation.name
                                        );
                                    }
                                }
                                if let Some(id) = save_as_entity {
                                    test_runner.insert_entity(id, entity);
                                }
                            }
                        }
                        Expectation::Error(expect_error) => {
                            let error = result
                                .expect_err(&format!("{} should return an error", operation.name));
                            expect_error.verify_result(error);
                        }
                        Expectation::Ignore => (),
                    }
                }
            }
            // This test (in src/test/spec/json/sessions/server-support.json) runs two
            // operations with implicit sessions in sequence and then checks to see if they
            // used the same lsid. We delay for one second to ensure that the
            // implicit session used in the first operation is returned to the pool before
            // the second operation is executed.
            if test_case.description == "Server supports implicit sessions" {
                RUNTIME.delay_for(Duration::from_secs(1)).await;
            }
        }

        if let Some(ref events) = test_case.expect_events {
            for expected in events {
                let entity = test_runner.entities.get(&expected.client).unwrap();
                let client = entity.as_client();
                client.sync_workers().await;
                let event_type = expected
                    .event_type
                    .unwrap_or(test_file::ExpectedEventType::Command);

                let actual_events: Vec<_> =
                    client.get_filtered_events(event_type).into_iter().collect();

                let expected_events = &expected.events;

                assert_eq!(
                    actual_events.len(),
                    expected_events.len(),
                    "actual:\n{:#?}\nexpected:\n{:#?}",
                    actual_events,
                    expected_events
                );

                for (actual, expected) in actual_events.iter().zip(expected_events) {
                    if let Err(e) = events_match(actual, expected, Some(&test_runner.entities)) {
                        panic!(
                            "event mismatch: expected = {:#?}, actual = {:#?}\nall \
                             expected:\n{:#?}\nall actual:\n{:#?}\nmismatch detail: {}",
                            expected, actual, expected_events, actual_events, e,
                        );
                    }
                }
            }
        }

        test_runner.fail_point_guards.clear();

        if let Some(ref outcome) = test_case.outcome {
            for expected_data in outcome {
                let db_name = &expected_data.database_name;
                let coll_name = &expected_data.collection_name;

                let selection_criteria = SelectionCriteria::ReadPreference(ReadPreference::Primary);
                let read_concern = ReadConcern::local();

                let options = CollectionOptions::builder()
                    .selection_criteria(selection_criteria)
                    .read_concern(read_concern)
                    .build();
                let collection = test_runner
                    .internal_client
                    .get_coll_with_options(db_name, coll_name, options);

                let options = FindOptions::builder().sort(doc! { "_id": 1 }).build();
                let actual_data: Vec<Document> = collection
                    .find(doc! {}, options)
                    .await
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();

                assert_eq!(expected_data.documents, actual_data);
            }
        }

        println!("{} succeeded", &test_case.description);
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn test_examples() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;
    run_spec_test(
        &["unified-test-format", "examples"],
        run_unified_format_test,
    )
    .await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn valid_fail() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "src",
        "test",
        "spec",
        "json",
        "unified-test-format",
        "valid-fail",
    ]
    .iter()
    .collect();

    for entry in read_dir(&path).unwrap() {
        let test_file_path = PathBuf::from(entry.unwrap().file_name());
        let path = path.join(&test_file_path);
        let path_display = path.display().to_string();

        std::panic::AssertUnwindSafe(run_single_test(path, &run_unified_format_test))
            .catch_unwind()
            .await
            .expect_err(&format!("tests from {} should have failed", path_display));
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn valid_pass() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;
    run_spec_test(
        &["unified-test-format", "valid-pass"],
        run_unified_format_test,
    )
    .await;
}

const SKIPPED_INVALID_TESTS: &[&str] = &[
    // Event types are validated at test execution time, not parse time.
    "expectedEventsForClient-events_conflicts_with_cmap_eventType.json",
    "expectedEventsForClient-events_conflicts_with_command_eventType.json",
    "expectedEventsForClient-events_conflicts_with_default_eventType.json",
];

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn invalid() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "src",
        "test",
        "spec",
        "json",
        "unified-test-format",
        "invalid",
    ]
    .iter()
    .collect();

    for entry in read_dir(&path).unwrap() {
        let test_file = entry.unwrap();
        if !test_file.file_type().unwrap().is_file() {
            continue;
        }
        let test_file_path = PathBuf::from(test_file.file_name());
        if test_file_path.extension().and_then(OsStr::to_str) != Some("json") {
            continue;
        }
        let test_file_str = test_file_path.as_os_str().to_str().unwrap();
        if SKIPPED_INVALID_TESTS
            .iter()
            .any(|skip| *skip == test_file_str)
        {
            println!("Skipping {}", test_file_str);
            continue;
        }
        let path = path.join(&test_file_path);
        let path_display = path.display().to_string();

        let json: serde_json::Value =
            serde_json::from_reader(std::fs::File::open(path.as_path()).unwrap()).unwrap();
        let result: Result<TestFile, _> = bson::from_bson(
            bson::Bson::try_from(json).unwrap_or_else(|_| panic!("{}", path_display)),
        );
        if let Ok(test_file) = result {
            panic!(
                "{}: should be invalid, parsed to:\n{:#?}",
                path_display, test_file
            );
        }
    }
}
