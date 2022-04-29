use std::{collections::HashMap, sync::Arc, time::Duration};

use approx::abs_diff_eq;
use bson::{doc, Document};
use semver::VersionReq;
use serde::Deserialize;
use tokio::sync::RwLockWriteGuard;

use crate::{
    coll::options::FindOptions,
    error::Result,
    options::ServerAddress,
    runtime,
    runtime::AsyncJoinHandle,
    sdam::{description::topology::server_selection, Server},
    selection_criteria::{ReadPreference, SelectionCriteria},
    test::{
        log_uncaptured,
        run_spec_test,
        CmapEvent,
        Event,
        EventHandler,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
        CLIENT_OPTIONS,
        LOCK,
    },
    ServerInfo,
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
    expected_frequencies: HashMap<ServerAddress, f64>,
}

#[derive(Debug, Deserialize)]
struct TestServer {
    address: ServerAddress,
    operation_count: u32,
}

async fn run_test(test_file: TestFile) {
    println!("Running {}", test_file.description);

    let mut tallies: HashMap<ServerAddress, u32> = HashMap::new();

    let servers: HashMap<ServerAddress, Arc<Server>> = test_file
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
        .into_topology_description(None);

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

    if setup_client_options.load_balanced.unwrap_or(false) {
        log_uncaptured("skipping load_balancing_test test due to load-balanced topology");
        return;
    }

    if setup_client_options.credential.is_some() {
        log_uncaptured("skipping load_balancing_test test due to auth being enabled");
        return;
    }

    setup_client_options.hosts.drain(1..);
    setup_client_options.direct_connection = Some(true);
    let setup_client = TestClient::with_options(Some(setup_client_options)).await;

    let version = VersionReq::parse(">= 4.2.9").unwrap();
    // blockConnection failpoint option only supported in 4.2.9+.
    if !version.matches(&setup_client.server_version) {
        log_uncaptured(
            "skipping load_balancing_test test due to server not supporting blockConnection option",
        );
        return;
    }

    if !setup_client.is_sharded() {
        log_uncaptured("skipping load_balancing_test test due to topology not being sharded");
        return;
    }

    if CLIENT_OPTIONS.hosts.len() != 2 {
        log_uncaptured("skipping load_balancing_test test due to topology not having 2 mongoses");
        return;
    }

    // seed the collection with a document so the find commands do some work
    setup_client
        .database("load_balancing_test")
        .collection("load_balancing_test")
        .insert_one(doc! {}, None)
        .await
        .unwrap();

    /// min_share is the lower bound for the % of times the the less selected server
    /// was selected. max_share is the upper bound.
    async fn do_test(
        client: &TestClient,
        handler: &mut EventHandler,
        min_share: f64,
        max_share: f64,
        iterations: usize,
    ) {
        handler.clear_cached_events();

        let mut handles: Vec<AsyncJoinHandle<Result<()>>> = Vec::new();
        for _ in 0..10 {
            let collection = client
                .database("load_balancing_test")
                .collection::<Document>("load_balancing_test");
            handles.push(runtime::spawn(async move {
                for _ in 0..iterations {
                    collection.find_one(None, None).await?;
                }
                Ok(())
            }))
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let mut tallies: HashMap<ServerAddress, u32> = HashMap::new();
        for event in handler.get_command_started_events(&["find"]) {
            *tallies.entry(event.connection.address.clone()).or_insert(0) += 1;
        }

        assert_eq!(tallies.len(), 2);
        let mut counts: Vec<_> = tallies.values().collect();
        counts.sort();

        let share_of_selections = (*counts[0] as f64) / ((*counts[0] + *counts[1]) as f64);
        assert!(
            share_of_selections <= max_share,
            "expected no more than {}% of selections, instead got {}%",
            (max_share * 100.0) as u32,
            (share_of_selections * 100.0) as u32
        );
        assert!(
            share_of_selections >= min_share,
            "expected at least {}% of selections, instead got {}%",
            (min_share * 100.0) as u32,
            (share_of_selections * 100.0) as u32
        );
    }

    let mut handler = EventHandler::new();
    let mut subscriber = handler.subscribe();
    let mut options = CLIENT_OPTIONS.clone();
    let max_pool_size = 10;
    let hosts = options.hosts.clone();
    options.local_threshold = Duration::from_secs(30).into();
    options.max_pool_size = Some(max_pool_size);
    options.min_pool_size = Some(max_pool_size);
    let client = TestClient::with_handler(Some(Arc::new(handler.clone())), options).await;

    // wait for both servers pools to be saturated.
    for address in hosts {
        let selector = Arc::new(move |sd: &ServerInfo| sd.address() == &address);
        for _ in 0..max_pool_size {
            let client = client.clone();
            let selector = selector.clone();
            runtime::execute(async move {
                let options = FindOptions::builder()
                    .selection_criteria(SelectionCriteria::Predicate(selector))
                    .build();
                client
                    .database("load_balancing_test")
                    .collection::<Document>("load_balancing_test")
                    .find(doc! { "$where": "sleep(500) && true" }, options)
                    .await
                    .unwrap();
            });
        }
    }
    let mut conns = 0;
    while conns < max_pool_size * 2 {
        subscriber
            .wait_for_event(Duration::from_secs(30), |event| {
                matches!(event, Event::Cmap(CmapEvent::ConnectionReady(_)))
            })
            .await
            .expect("timed out waiting for both pools to be saturated");
        conns += 1;
    }
    drop(subscriber);

    // enable a failpoint on one of the mongoses to slow it down
    let options = FailCommandOptions::builder()
        .block_connection(Duration::from_millis(500))
        .build();
    let failpoint = FailPoint::fail_command(&["find"], FailPointMode::AlwaysOn, options);

    let slow_host = CLIENT_OPTIONS.hosts[0].clone();
    let criteria = SelectionCriteria::Predicate(Arc::new(move |si| si.address() == &slow_host));
    let fp_guard = setup_client
        .enable_failpoint(failpoint, criteria)
        .await
        .expect("enabling failpoint should succeed");

    // verify that the lesser picked server (slower one) was picked less than 25% of the time.
    do_test(&client, &mut handler, 0.05, 0.25, 10).await;

    // disable failpoint and rerun, should be back to even split
    drop(fp_guard);
    do_test(&client, &mut handler, 0.40, 0.50, 100).await;
}
