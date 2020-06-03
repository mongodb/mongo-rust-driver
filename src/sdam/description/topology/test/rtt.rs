use std::collections::HashMap;

use serde::Deserialize;

use crate::{
    sdam::description::{
        server::ServerDescription,
        topology::{test::f64_ms_as_duration, TopologyDescription, TopologyType},
    },
    test::run_spec_test,
};

#[derive(Debug, Deserialize)]
pub struct TestFile {
    pub avg_rtt_ms: AverageRtt,
    pub new_rtt_ms: f64,
    pub new_avg_rtt: f64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum AverageRtt {
    F(f64),
    I(i32),
    S(String),
}

async fn run_test(test_file: TestFile) {
    let avg_rtt_ms = match test_file.avg_rtt_ms {
        AverageRtt::F(f) => Some(f),
        AverageRtt::S(ref s) if s == "NULL" => None,
        AverageRtt::I(i) => Some(i as f64),
        AverageRtt::S(ref s) => panic!("invalid average round trip time: {}", s),
    };

    // The address is not used, so it doesn't matter.
    let mut old_server_desc = ServerDescription::new(Default::default(), None);
    let mut new_server_desc = old_server_desc.clone();

    old_server_desc.average_round_trip_time = avg_rtt_ms.map(f64_ms_as_duration);
    new_server_desc.average_round_trip_time = Some(f64_ms_as_duration(test_file.new_rtt_ms));

    let topology = TopologyDescription {
        single_seed: false,
        topology_type: TopologyType::ReplicaSetNoPrimary,
        set_name: None,
        max_set_version: None,
        max_election_id: None,
        compatibility_error: None,
        session_support_status: Default::default(),
        cluster_time: None,
        local_threshold: None,
        heartbeat_freq: None,
        servers: {
            let mut servers = HashMap::new();
            servers.insert(Default::default(), old_server_desc);

            servers
        },
    };

    topology.update_round_trip_time(&mut new_server_desc);

    assert_eq!(
        new_server_desc.average_round_trip_time,
        Some(f64_ms_as_duration(test_file.new_avg_rtt))
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn server_selection_rtt() {
    run_spec_test(&["server-selection", "rtt"], run_test).await;
}
