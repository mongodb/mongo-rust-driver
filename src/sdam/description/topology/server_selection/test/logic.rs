use std::time::Duration;

use serde::Deserialize;

use crate::{
    selection_criteria::{ReadPreference, ReadPreferenceOptions, TagSet},
    test::run_spec_test,
};

use super::{TestServerDescription, TestTopologyDescription};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestFile {
    #[serde(rename = "heartbeatFrequencyMS")]
    heartbeat_frequency_ms: Option<u64>,
    topology_description: TestTopologyDescription,
    read_preference: TestReadPreference,
    suitable_servers: Option<Vec<TestServerDescription>>,
    in_latency_window: Option<Vec<TestServerDescription>>,
    error: Option<bool>,
    #[serde(rename = "operation")]
    // don't need this since we don't have separate server selection functions for reads/writes
    _operation: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TestReadPreference {
    pub mode: Option<String>,
    pub tag_sets: Option<Vec<TagSet>>,
    #[serde(rename = "maxStalenessSeconds")]
    pub max_staleness_seconds: Option<u64>,
}

impl From<TestReadPreference> for ReadPreference {
    fn from(test_read_pref: TestReadPreference) -> Self {
        let max_staleness = test_read_pref
            .max_staleness_seconds
            .map(Duration::from_secs);
        let options = ReadPreferenceOptions::builder()
            .tag_sets(test_read_pref.tag_sets)
            .max_staleness(max_staleness)
            .build();

        match &test_read_pref.mode.as_deref() {
            Some("Primary") | None => ReadPreference::Primary,
            Some("Secondary") => ReadPreference::Secondary { options },
            Some("PrimaryPreferred") => ReadPreference::PrimaryPreferred { options },
            Some("SecondaryPreferred") => ReadPreference::SecondaryPreferred { options },
            Some("Nearest") => ReadPreference::Nearest { options },
            Some(m) => panic!("invalid read preference mode: {}", m),
        }
    }
}

macro_rules! get_sorted_addresses {
    ($servers:expr) => {{
        let mut servers: Vec<_> = $servers.iter().map(|s| s.address.to_string()).collect();
        servers.sort_unstable();
        servers
    }};
}

async fn run_test(test_file: TestFile) {
    if let Some(ref expected_suitable_servers) = test_file.suitable_servers {
        let topology = test_file
            .topology_description
            .into_topology_description(test_file.heartbeat_frequency_ms.map(Duration::from_millis));

        let rp = test_file.read_preference.into();
        let mut actual_servers: Vec<_> = topology.suitable_servers(&rp).unwrap();

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
    } else if test_file.error == Some(true) {
        // skip on sync to avoid compilation conflicts with the sync version of
        // ClientOptions::parse.
        #[cfg(not(any(feature = "sync", feature = "tokio-sync")))]
        {
            let mut options = Vec::new();
            if let Some(ref mode) = test_file.read_preference.mode {
                options.push(format!("readPreference={}", mode));
            }
            if let Some(max_staleness_seconds) = test_file.read_preference.max_staleness_seconds {
                options.push(format!("maxStalenessSeconds={}", max_staleness_seconds));
            }
            if let Some(heartbeat_freq) = test_file.heartbeat_frequency_ms {
                options.push(format!("heartbeatFrequencyMS={}", heartbeat_freq));
            }

            let uri_str = format!("mongodb://localhost:27017/?{}", options.join("&"));
            crate::client::options::ClientOptions::parse(uri_str)
                .await
                .err()
                .unwrap_or_else(|| {
                    panic!(
                        "expected client construction to fail with read preference {:#?}",
                        test_file.read_preference
                    )
                });
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
async fn server_selection_load_balanced() {
    run_spec_test(
        &[
            "server-selection",
            "server_selection",
            "LoadBalanced",
            "read",
        ],
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
