use std::time::Duration;

use serde::Deserialize;

use crate::{
    selection_criteria::{ReadPreference, ReadPreferenceOptions, TagSet},
    test::run_spec_test,
};

use super::{TestServerDescription, TestTopologyDescription};

#[derive(Debug, Deserialize)]
struct TestFile {
    #[serde(rename = "heartbeatFrequencyMS")]
    heartbeat_frequency_ms: Option<u64>,
    topology_description: TestTopologyDescription,
    read_preference: TestReadPreference,
    suitable_servers: Option<Vec<TestServerDescription>>,
    in_latency_window: Option<Vec<TestServerDescription>>,
    error: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct TestReadPreference {
    pub mode: Option<String>,
    pub tag_sets: Option<Vec<TagSet>>,
    #[serde(rename = "maxStalenessSeconds")]
    pub max_staleness_seconds: Option<u64>,
}

fn convert_read_preference(test_read_pref: TestReadPreference) -> Option<ReadPreference> {
    let max_staleness = test_read_pref
        .max_staleness_seconds
        .map(Duration::from_secs);
    let mut options = ReadPreferenceOptions::builder().build();
    options.tag_sets = test_read_pref.tag_sets;
    options.max_staleness = max_staleness;

    let read_pref = match &test_read_pref.mode.as_ref()?[..] {
        "Primary" => ReadPreference::Primary,
        "Secondary" => ReadPreference::Secondary { options },
        "PrimaryPreferred" => ReadPreference::PrimaryPreferred { options },
        "SecondaryPreferred" => ReadPreference::SecondaryPreferred { options },
        "Nearest" => ReadPreference::Nearest { options },
        m => panic!("invalid read preference mode: {}", m),
    };

    Some(read_pref)
}

macro_rules! get_sorted_addresses {
    ($servers:expr) => {{
        let mut servers: Vec<_> = $servers.iter().map(|s| s.address.to_string()).collect();
        servers.sort_unstable();
        servers
    }};
}

async fn run_test(test_file: TestFile) {
    let read_pref = match convert_read_preference(test_file.read_preference) {
        Some(read_pref) => read_pref,
        None => return,
    };

    let topology = match test_file
        .topology_description
        .into_topology_description(test_file.heartbeat_frequency_ms.map(Duration::from_millis))
    {
        Some(t) => t,
        None => return,
    };

    if let Some(ref expected_suitable_servers) = test_file.suitable_servers {
        let mut actual_servers: Vec<_> = topology.suitable_servers(&read_pref).unwrap();

        assert_eq!(
            get_sorted_addresses!(expected_suitable_servers),
            get_sorted_addresses!(&actual_servers),
        );

        if let Some(ref expected_in_latency_window) = test_file.in_latency_window {
            topology.retain_servers_within_latency_window(&mut actual_servers);

            assert_eq!(
                get_sorted_addresses!(expected_in_latency_window),
                get_sorted_addresses!(&actual_servers)
            );
        }
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn server_selection_replica_set_no_primary() {
    run_spec_test(
        &[
            "server-selection",
            "server_selection",
            "ReplicaSetNoPrimary",
            "read",
        ],
        run_test,
    )
    .await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn server_selection_replica_set_with_primary() {
    run_spec_test(
        &[
            "server-selection",
            "server_selection",
            "ReplicaSetWithPrimary",
            "read",
        ],
        run_test,
    )
    .await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn server_selection_sharded() {
    run_spec_test(
        &["server-selection", "server_selection", "Sharded", "read"],
        run_test,
    )
    .await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn server_selection_single() {
    run_spec_test(
        &["server-selection", "server_selection", "Single", "read"],
        run_test,
    )
    .await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn server_selection_unknown() {
    run_spec_test(
        &["server-selection", "server_selection", "Unknown", "read"],
        run_test,
    )
    .await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn max_staleness_replica_set_no_primary() {
    run_spec_test(&["max-staleness", "ReplicaSetNoPrimary"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn max_staleness_replica_set_with_primary() {
    run_spec_test(&["max-staleness", "ReplicaSetWithPrimary"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn max_staleness_sharded() {
    run_spec_test(&["max-staleness", "Sharded"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn max_staleness_single() {
    run_spec_test(&["max-staleness", "Single"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn max_staleness_unknown() {
    run_spec_test(&["max-staleness", "Unknown"], run_test).await;
}
