use std::{collections::HashMap, sync::Arc, time::Duration};

use approx::abs_diff_eq;
use bson::Document;
use semver::VersionReq;
use serde::Deserialize;
use tokio::sync::RwLockWriteGuard;

use crate::{
    options::StreamAddress,
    runtime::AsyncJoinHandle,
    sdam::{description::topology::server_selection, Server},
    selection_criteria::ReadPreference,
    test::{
        run_spec_test,
        EventClient,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
        CLIENT_OPTIONS,
        LOCK,
    },
    RUNTIME,
};

use super::TestTopologyDescription;

#[derive(Debug, Deserialize)]
struct TestFile {
    description: String,
    topology_description: TestTopologyDescription,
    mocked_topology_state: Vec<TestServer>,
    iterations: u32,
    outcome: TestOutcome,
}

#[derive(Debug, Deserialize)]
struct TestOutcome {
    tolerance: f64,
    expected_frequencies: HashMap<StreamAddress, f64>,
}

#[derive(Debug, Deserialize)]
struct TestServer {
    address: StreamAddress,
    operation_count: u32,
}

async fn run_test(test_file: TestFile) {
    println!("Running {}", test_file.description);

    let mut tallies: HashMap<StreamAddress, u32> = HashMap::new();

    let servers: HashMap<StreamAddress, Arc<Server>> = test_file
        .mocked_topology_state
        .into_iter()
        .map(|desc| {
            (
                desc.address.clone(),
                Arc::new(Server::new_mocked(desc.address, desc.operation_count)),
            )
        })
        .collect();

    let topology_description = test_file
        .topology_description
        .into_topology_description(None)
        .unwrap();

    let read_pref = ReadPreference::Nearest {
        options: Default::default(),
    }
    .into();

    for _ in 0..test_file.iterations {
        let selection =
            server_selection::attempt_to_select_server(&read_pref, &topology_description, &servers)
                .expect("selection should not fail")
                .expect("a server should have been selected");
        *tallies.entry(selection.address.clone()).or_insert(0) += 1;
    }

    for (address, expected_frequency) in test_file.outcome.expected_frequencies {
        let actual_frequency =
            tallies.get(&address).cloned().unwrap_or(0) as f64 / (test_file.iterations as f64);

        let epsilon = if expected_frequency != 1.0 && expected_frequency != 0.0 {
            test_file.outcome.tolerance
        } else {
            f64::EPSILON
        };

        assert!(
            abs_diff_eq!(actual_frequency, expected_frequency, epsilon = epsilon),
            "{}: for server {} expected frequency = {}, actual = {}",
            test_file.description,
            address,
            expected_frequency,
            actual_frequency
        );
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn select_in_window() {
    run_spec_test(&["server-selection", "in_window"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn load_balancing_test() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut setup_client_options = CLIENT_OPTIONS.clone();
    setup_client_options.hosts.drain(1..);
    setup_client_options.direct_connection = Some(true);
    let setup_client = TestClient::with_options(Some(setup_client_options)).await;

    let version = VersionReq::parse(">= 4.2.9").unwrap();
    // blockConnection failpoint option only supported in 4.2.9+.
    if !version.matches(&setup_client.server_version) {
        println!(
            "skipping load_balancing_test test due to server not supporting blockConnection option"
        );
        return;
    }

    if !setup_client.is_sharded() {
        println!("skipping load_balancing_test test due to topology not being sharded");
        return;
    }

    if CLIENT_OPTIONS.hosts.len() != 2 {
        println!("skipping load_balancing_test test due to topology not having 2 mongoses");
        return;
    }

    let options = FailCommandOptions::builder()
        .block_connection(Duration::from_millis(500))
        .build();
    let failpoint = FailPoint::fail_command(&["find"], FailPointMode::AlwaysOn, options);

    let fp_guard = setup_client
        .enable_failpoint(failpoint, None)
        .await
        .expect("enabling failpoint should succeed");

    let mut client = EventClient::new().await;

    /// min_share is the lower bound for the % of times the the less selected server
    /// was selected. max_share is the upper bound.
    async fn do_test(client: &mut EventClient, min_share: f64, max_share: f64) {
        client.clear_cached_events();

        let mut handles: Vec<AsyncJoinHandle<()>> = Vec::new();
        for _ in 0..10 {
            let collection = client
                .database("load_balancing_test")
                .collection::<Document>("load_balancing_test");
            handles.push(
                RUNTIME
                    .spawn(async move {
                        for _ in 0..10 {
                            let _ = collection.find_one(None, None).await;
                        }
                    })
                    .unwrap(),
            )
        }

        futures::future::join_all(handles).await;

        let mut tallies: HashMap<StreamAddress, u32> = HashMap::new();
        for event in client.get_command_started_events(&["find"]) {
            *tallies.entry(event.connection.address.clone()).or_insert(0) += 1;
        }

        assert_eq!(tallies.len(), 2);
        let mut counts: Vec<_> = tallies.values().collect();
        counts.sort();

        // verify that the lesser picked server (slower one) was picked less than 25% of the time.
        let share_of_selections = (*counts[0] as f64) / ((*counts[0] + *counts[1]) as f64);
        assert!(share_of_selections <= max_share);
        assert!(share_of_selections >= min_share);
    }

    do_test(&mut client, 0.05, 0.25).await;

    // disable failpoint and rerun, should be close to even split
    drop(fp_guard);
    do_test(&mut client, 0.40, 0.50).await;
}
