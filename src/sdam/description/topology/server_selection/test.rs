use std::time::Duration;

use chrono::{naive::NaiveDateTime, offset::Utc, DateTime};
use serde::Deserialize;

use crate::{
    bson::DateTime as BsonDateTime,
    is_master::{IsMasterCommandResponse, IsMasterReply, LastWrite},
    options::StreamAddress,
    sdam::description::{
        server::{ServerDescription, ServerType},
        topology::{test::f64_ms_as_duration, TopologyDescription, TopologyType},
    },
    selection_criteria::{ReadPreference, ReadPreferenceOptions, TagSet},
    test::run_spec_test,
};

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
struct TestTopologyDescription {
    #[serde(rename = "type")]
    topology_type: TopologyType,
    servers: Vec<TestServerDescription>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestServerDescription {
    address: String,
    #[serde(rename = "avg_rtt_ms")]
    avg_rtt_ms: Option<f64>,
    #[serde(rename = "type")]
    server_type: TestServerType,
    tags: Option<TagSet>,
    last_update_time: Option<i32>,
    last_write: Option<LastWriteDate>,
    max_wire_version: Option<i32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LastWriteDate {
    last_write_date: i64,
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
    pub mode: Option<String>,
    pub tag_sets: Option<Vec<TagSet>>,
    #[serde(rename = "maxStalenessSeconds")]
    pub max_staleness_seconds: Option<u64>,
}

fn convert_read_preference(test_read_pref: TestReadPreference) -> Option<ReadPreference> {
    let max_staleness = test_read_pref
        .max_staleness_seconds
        .map(Duration::from_secs);

    let read_pref = match &test_read_pref.mode.as_ref()?[..] {
        "Primary" => ReadPreference::Primary,
        "Secondary" => ReadPreference::Secondary {
            options: ReadPreferenceOptions::builder()
                .tag_sets(test_read_pref.tag_sets)
                .max_staleness(max_staleness.clone())
                .build(),
        },
        "PrimaryPreferred" => ReadPreference::PrimaryPreferred {
            options: ReadPreferenceOptions::builder()
                .tag_sets(test_read_pref.tag_sets)
                .max_staleness(max_staleness.clone())
                .build(),
        },
        "SecondaryPreferred" => ReadPreference::SecondaryPreferred {
            options: ReadPreferenceOptions::builder()
                .tag_sets(test_read_pref.tag_sets)
                .max_staleness(max_staleness.clone())
                .build(),
        },
        "Nearest" => ReadPreference::Nearest {
            options: ReadPreferenceOptions::builder()
                .tag_sets(test_read_pref.tag_sets)
                .max_staleness(max_staleness.clone())
                .build(),
        },
        _ => panic!("invalid read preference: {:?}", test_read_pref),
    };

    Some(read_pref)
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

fn utc_datetime_from_millis(millis: i64) -> BsonDateTime {
    let seconds_portion = millis / 1000;
    let nanos_portion = (millis % 1000) * 1_000_000;

    let naive_datetime = NaiveDateTime::from_timestamp(seconds_portion, nanos_portion as u32);
    let datetime = DateTime::from_utc(naive_datetime, Utc);

    BsonDateTime(datetime)
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
    command_response.last_write = test_server_desc.last_write.map(|last_write| LastWrite {
        last_write_date: utc_datetime_from_millis(last_write.last_write_date),
    });

    let is_master = IsMasterReply {
        command_response,
        round_trip_time: test_server_desc.avg_rtt_ms.map(f64_ms_as_duration),
        cluster_time: None,
    };

    let mut server_desc = ServerDescription::new(
        StreamAddress::parse(&test_server_desc.address).unwrap(),
        Some(Ok(is_master)),
    );
    server_desc.last_update_time = test_server_desc
        .last_update_time
        .map(|i| utc_datetime_from_millis(i as i64));

    Some(server_desc)
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

    let servers: Option<Vec<ServerDescription>> = test_file
        .topology_description
        .servers
        .into_iter()
        // The driver doesn't support server versions low enough not to support max staleness, so we
        // just manually filter them out here.
        .filter(|server| server.max_wire_version.map(|version| version >= 5).unwrap_or(true))
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
        session_support_status: Default::default(),
        cluster_time: None,
        local_threshold: None,
        heartbeat_freq: test_file.heartbeat_frequency_ms.map(Duration::from_millis),
        servers: servers
            .into_iter()
            .map(|server| (server.address.clone(), server))
            .collect(),
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
