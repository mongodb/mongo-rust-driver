use std::collections::HashMap;

use super::{retain_suitable_servers, retain_within_latency_window, DEFAULT_LOCAL_THRESHOLD};
use crate::{
    read_preference::ReadPreference,
    topology::{ServerDescription, ServerType, TopologyType},
};

#[derive(Debug, Deserialize)]
pub struct TestFile {
    pub topology_description: TopologyDescription,
    pub read_preference: TestReadPreference,
    pub suitable_servers: Vec<TestServerDescription>,
    pub in_latency_window: Vec<TestServerDescription>,
}

#[derive(Debug, Deserialize)]
pub struct TopologyDescription {
    #[serde(rename = "type")]
    pub topology_type: TopologyType,
    pub servers: Vec<TestServerDescription>,
}

#[derive(Debug, Deserialize)]
pub struct TestServerDescription {
    pub address: String,
    pub avg_rtt_ms: f64,
    #[serde(rename = "type")]
    pub server_type: TestServerType,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub enum TestServerType {
    Standalone,
    Mongos,
    RSPrimary,
    RSSecondary,
    RSArbiter,
    RSOther,
    RSGhost,
    Unknown,
    PossiblePrimary,
}

impl TestServerType {
    fn into_server_type(self) -> Option<ServerType> {
        match self {
            TestServerType::Standalone => Some(ServerType::Standalone),
            TestServerType::Mongos => Some(ServerType::Mongos),
            TestServerType::RSPrimary => Some(ServerType::RSPrimary),
            TestServerType::RSSecondary => Some(ServerType::RSSecondary),
            TestServerType::RSArbiter => Some(ServerType::RSArbiter),
            TestServerType::RSOther => Some(ServerType::RSOther),
            TestServerType::RSGhost => Some(ServerType::RSGhost),
            TestServerType::Unknown => Some(ServerType::Unknown),
            TestServerType::PossiblePrimary => None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct TestReadPreference {
    pub mode: String,
    pub tag_sets: Option<Vec<HashMap<String, String>>>,
}

fn convert_read_preference(test_read_pref: TestReadPreference) -> ReadPreference {
    match &test_read_pref.mode[..] {
        "Primary" => ReadPreference::Primary,
        "Secondary" => ReadPreference::Secondary {
            tag_sets: test_read_pref.tag_sets,
            max_staleness: None,
        },
        "PrimaryPreferred" => ReadPreference::PrimaryPreferred {
            tag_sets: test_read_pref.tag_sets,
            max_staleness: None,
        },
        "SecondaryPreferred" => ReadPreference::SecondaryPreferred {
            tag_sets: test_read_pref.tag_sets,
            max_staleness: None,
        },
        "Nearest" => ReadPreference::Nearest {
            tag_sets: test_read_pref.tag_sets,
            max_staleness: None,
        },
        _ => panic!("invalid read preference: {:?}", test_read_pref),
    }
}

fn convert_server_description(
    test_server_desc: TestServerDescription,
) -> Option<ServerDescription> {
    let mut server_desc = ServerDescription::new(&test_server_desc.address, None);
    server_desc.round_trip_time = Some(test_server_desc.avg_rtt_ms);
    server_desc.server_type = test_server_desc.server_type.into_server_type()?;
    server_desc.tags = test_server_desc.tags.unwrap_or_else(HashMap::new);
    Some(server_desc)
}

fn get_sorted_addresses(servers: &[ServerDescription]) -> Vec<String> {
    let mut servers: Vec<_> = servers.iter().map(|s| s.address.to_string()).collect();
    servers.sort_unstable();
    servers
}

fn run_test(test_file: TestFile) {
    let servers: Vec<_> = test_file
        .topology_description
        .servers
        .into_iter()
        .map(convert_server_description)
        .collect();

    if servers.iter().any(|s| s.is_none()) {
        return;
    }

    let mut servers = servers.into_iter().map(Option::unwrap).collect();
    let read_pref = convert_read_preference(test_file.read_preference);
    let topology_type = test_file.topology_description.topology_type;

    let mut suitable_servers: Vec<_> = test_file
        .suitable_servers
        .into_iter()
        .map(|s| s.address)
        .collect();
    suitable_servers.sort_unstable();

    let mut in_latency_window: Vec<_> = test_file
        .in_latency_window
        .into_iter()
        .map(|s| s.address)
        .collect();
    in_latency_window.sort_unstable();

    retain_suitable_servers(&mut servers, topology_type, Some(&read_pref));
    assert_eq!(get_sorted_addresses(&servers), suitable_servers);

    retain_within_latency_window(&mut servers, *DEFAULT_LOCAL_THRESHOLD);
    assert_eq!(get_sorted_addresses(&servers), in_latency_window);
}

#[test]
fn server_selection_replica_set_no_primary() {
    crate::test::run(
        &[
            "server-selection",
            "server_selection",
            "ReplicaSetNoPrimary",
            "read",
        ],
        run_test,
    );
}

#[test]
fn server_selection_replica_set_with_primary() {
    crate::test::run(
        &[
            "server-selection",
            "server_selection",
            "ReplicaSetWithPrimary",
            "read",
        ],
        run_test,
    );
}

#[test]
fn server_selection_sharded() {
    crate::test::run(
        &["server-selection", "server_selection", "Sharded", "read"],
        run_test,
    );
}

#[test]
fn server_selection_single() {
    crate::test::run(
        &["server-selection", "server_selection", "Single", "read"],
        run_test,
    );
}

#[test]
fn server_selection_unknown() {
    crate::test::run(
        &["server-selection", "server_selection", "Unknown", "read"],
        run_test,
    );
}
