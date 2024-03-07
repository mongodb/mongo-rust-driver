// NOTE: Please run a patch against drivers-atlas-testing when making changes within this file. The
// DRIVER_REPOSITORY and DRIVER_REVISION fields for the Rust axis in drivers-atlas-testing's
// evergreen config file can be updated to test against your branch.

mod json_models;

use std::{
    env,
    fs::File,
    io::{BufWriter, Write},
    panic::AssertUnwindSafe,
    path::PathBuf,
};

use futures::FutureExt;
use serde_json::Value;
use time::OffsetDateTime;

use crate::{
    bson::{doc, Bson},
    test::{
        log_uncaptured,
        spec::unified_runner::{entity::Entity, test_file::TestFile, test_runner::TestRunner},
    },
};

use json_models::{Events, Results};

use super::spec::unified_runner::EntityMap;

#[test]
#[ignore]
fn get_exe_name() {
    let mut file = File::create("exe_name.txt").expect("Failed to create file");
    let exe_name = env::current_exe()
        .expect("Failed to determine name of test executable")
        .into_os_string()
        .into_string()
        .expect("Failed to convert OS string to string");
    write!(&mut file, "{}", exe_name).expect("Failed to write executable name to file");
}

#[tokio::test]
async fn workload_executor() {
    if env::var("ATLAS_PLANNED_MAINTENANCE_TESTING").is_err() {
        // This test should only be run from the workload-executor script.
        log_uncaptured(
            "Skipping workload_executor due to being run outside of planned maintenance testing",
        );
        return;
    }

    let connection_string =
        env::var("WORKLOAD_EXECUTOR_CONNECTION_STRING").expect("No connection string specified");

    let workload_string = env::var("WORKLOAD_EXECUTOR_WORKLOAD").expect("No workload specified");
    let workload =
        serde_json::from_str(&workload_string).expect("Error converting workload to JSON");

    let mut test_runner = TestRunner::new_with_connection_string(&connection_string).await;

    let execution_errors = execute_workload(&mut test_runner, workload).await;
    let mut entities = test_runner.entities.write().await;
    write_json(&mut entities, execution_errors);
}

async fn execute_workload(test_runner: &mut TestRunner, workload: Value) -> Vec<Bson> {
    let mut execution_errors: Vec<Bson> = vec![];

    let test_file: TestFile = serde_json::from_value(workload).unwrap();
    let description = test_file.description.clone();

    log_uncaptured("Running planned maintenance tests");

    if AssertUnwindSafe(test_runner.run_test(test_file, None, None))
        .catch_unwind()
        .await
        .is_err()
    {
        execution_errors.push(
            doc! {
                "error": format!("Unexpected error occurred while running {}", description),
                "time": OffsetDateTime::now_utc().unix_timestamp(),
            }
            .into(),
        )
    }

    log_uncaptured("Planned maintenance tests completed");

    execution_errors
}

fn write_json(entities: &mut EntityMap, mut errors: Vec<Bson>) {
    log_uncaptured("Writing planned maintenance test results to files");

    let mut events = Events::new_empty();
    if let Some(Entity::Bson(Bson::Array(mut operation_errors))) = entities.remove("errors") {
        errors.append(&mut operation_errors);
    }
    events.errors = errors;
    if let Some(Entity::Bson(Bson::Array(failures))) = entities.remove("failures") {
        events.failures = failures;
    }

    let mut results = Results::new_empty();
    results.num_errors = events.errors.len().into();
    results.num_failures = events.failures.len().into();
    if let Some(Entity::Bson(Bson::Int64(iterations))) = entities.remove("iterations") {
        results.num_iterations = iterations.into();
    }
    if let Some(Entity::Bson(Bson::Int64(successes))) = entities.remove("successes") {
        results.num_successes = successes.into();
    }

    let path =
        env::var("WORKLOAD_EXECUTOR_WORKING_DIRECTORY").expect("No working directory specified");

    let mut events_path = PathBuf::from(&path);
    events_path.push("events.json");
    let mut writer =
        BufWriter::new(File::create(events_path).expect("Failed to create events.json"));

    let mut json_string = serde_json::to_string(&events).unwrap();
    // Pop the final "}" from the string as we still need to insert the events k/v pair.
    json_string.pop();
    write!(&mut writer, "{}", json_string).unwrap();
    // The events key is expected to be present regardless of whether storeEventsAsEntities was
    // defined.
    write!(&mut writer, ",\"events\":[").unwrap();
    if let Some(entity) = entities.get("events") {
        let event_list_entity = entity.as_event_list().to_owned();
        let client = entities
            .get(&event_list_entity.client_id)
            .unwrap()
            .as_client();
        let names: Vec<&str> = event_list_entity
            .event_names
            .iter()
            .map(String::as_ref)
            .collect();
        client.write_events_list_to_file(&names, &mut writer);
    }
    write!(&mut writer, "]}}").unwrap();

    let mut results_path = PathBuf::from(&path);
    results_path.push("results.json");
    let file = File::create(results_path).expect("Failed to create results.json");
    serde_json::to_writer(file, &results)
        .expect("Failed to convert results to JSON and write to file");

    log_uncaptured("Writing planned maintenance test results to files completed");
}
