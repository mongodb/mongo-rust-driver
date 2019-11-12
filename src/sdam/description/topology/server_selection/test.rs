use serde::Deserialize;

use crate::{
    is_master::{IsMasterCommandResponse, IsMasterReply},
    options::StreamAddress,
    read_preference::{ReadPreference, TagSet},
    sdam::description::{
        server::{ServerDescription, ServerType},
        topology::{test::f64_ms_as_duration, TopologyDescription, TopologyType},
    },
};

#[derive(Debug, Deserialize)]
struct TestFile {
    topology_description: TestTopologyDescription,
    read_preference: TestReadPreference,
    suitable_servers: Vec<TestServerDescription>,
    in_latency_window: Vec<TestServerDescription>,
}

#[derive(Debug, Deserialize)]
struct TestTopologyDescription {
    #[serde(rename = "type")]
    topology_type: TopologyType,
    servers: Vec<TestServerDescription>,
}

#[derive(Debug, Deserialize)]
struct TestServerDescription {
    address: String,
    avg_rtt_ms: f64,
    #[serde(rename = "type")]
    server_type: TestServerType,
    tags: Option<TagSet>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum TestServerType {
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
    pub tag_sets: Option<Vec<TagSet>>,
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

fn is_master_response_from_server_type(server_type: ServerType) -> IsMasterCommandResponse {
    let mut response = IsMasterCommandResponse::default();

    match server_type {
        ServerType::Unknown => {
            response.ok = Some(0.0);
        }
        ServerType::Mongos => {
            response.ok = Some(1.0);
            response.msg = Some("isdbgrid".into());
        }
        ServerType::RSPrimary => {
            response.ok = Some(1.0);
            response.set_name = Some("foo".into());
            response.is_master = Some(true);
        }
        ServerType::RSOther => {
            response.ok = Some(1.0);
            response.set_name = Some("foo".into());
            response.hidden = Some(true);
        }
        ServerType::RSSecondary => {
            response.ok = Some(1.0);
            response.set_name = Some("foo".into());
            response.secondary = Some(true);
        }
        ServerType::RSArbiter => {
            response.ok = Some(1.0);
            response.set_name = Some("foo".into());
            response.arbiter_only = Some(true);
        }
        ServerType::RSGhost => {
            response.ok = Some(1.0);
            response.is_replica_set = Some(true);
        }
        ServerType::Standalone => {
            response.ok = Some(1.0);
        }
    };

    response
}

fn convert_server_description(
    test_server_desc: TestServerDescription,
) -> Option<ServerDescription> {
    let server_type = match test_server_desc.server_type.into_server_type() {
        Some(server_type) => server_type,
        None => return None,
    };

    let mut command_response = is_master_response_from_server_type(server_type);
    command_response.tags = test_server_desc.tags;

    let is_master = IsMasterReply {
        command_response,
        round_trip_time: Some(f64_ms_as_duration(test_server_desc.avg_rtt_ms)),
    };

    let server_desc = ServerDescription::new(
        StreamAddress::parse(&test_server_desc.address).unwrap(),
        Some(Ok(is_master)),
    );

    Some(server_desc)
}

macro_rules! get_sorted_addresses {
    ($servers:expr) => {{
        let mut servers: Vec<_> = $servers.iter().map(|s| s.address.to_string()).collect();
        servers.sort_unstable();
        servers
    }};
}

fn run_test(test_file: TestFile) {
    let servers: Option<Vec<ServerDescription>> = test_file
        .topology_description
        .servers
        .into_iter()
        .map(convert_server_description)
        .collect();

    let servers = match servers {
        Some(servers) => servers,
        None => return,
    };

    let topology = TopologyDescription {
        single_seed: servers.len() == 1,
        topology_type: test_file.topology_description.topology_type,
        set_name: None,
        max_set_version: None,
        max_election_id: None,
        compatibility_error: None,
        logical_session_timeout_minutes: None,
        local_threshold: None,
        servers: servers
            .into_iter()
            .map(|server| (server.address.clone(), server))
            .collect(),
    };

    let read_pref = convert_read_preference(test_file.read_preference);

    let mut suitable_servers: Vec<_> = topology.suitable_servers(&read_pref);
    suitable_servers.sort_unstable_by_key(|server| (&server.address.hostname, server.address.port));

    assert_eq!(
        get_sorted_addresses!(&test_file.suitable_servers),
        get_sorted_addresses!(&suitable_servers),
    );

    topology.retain_servers_within_latency_window(&mut suitable_servers);

    assert_eq!(
        get_sorted_addresses!(&test_file.in_latency_window),
        get_sorted_addresses!(&suitable_servers)
    );
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
