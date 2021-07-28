use std::time::Duration;

use bson::{doc, DateTime};
use serde::Deserialize;

use crate::{
    is_master::{IsMasterCommandResponse, IsMasterReply, LastWrite},
    options::ServerAddress,
    sdam::{
        description::topology::{test::f64_ms_as_duration, TopologyType},
        ServerDescription,
        ServerType,
        TopologyDescription,
    },
    selection_criteria::TagSet,
};

mod in_window;
mod logic;

#[derive(Debug, Deserialize)]
struct TestTopologyDescription {
    #[serde(rename = "type")]
    topology_type: TopologyType,
    servers: Vec<TestServerDescription>,
}

impl TestTopologyDescription {
    fn into_topology_description(
        self,
        heartbeat_frequency: Option<Duration>,
    ) -> Option<TopologyDescription> {
        let servers: Option<Vec<ServerDescription>> = self
            .servers
            .into_iter()
        // The driver doesn't support server versions low enough not to support max staleness, so we
        // just manually filter them out here.
            .filter(|server| server.max_wire_version.map(|version| version >= 5).unwrap_or(true))
            .map(|sd| sd.into_server_description())
            .collect();

        let servers = match servers {
            Some(servers) => servers,
            None => return None,
        };

        TopologyDescription {
            single_seed: servers.len() == 1,
            topology_type: self.topology_type,
            set_name: None,
            max_set_version: None,
            max_election_id: None,
            compatibility_error: None,
            session_support_status: Default::default(),
            transaction_support_status: Default::default(),
            cluster_time: None,
            local_threshold: None,
            heartbeat_freq: heartbeat_frequency,
            servers: servers
                .into_iter()
                .map(|server| (server.address.clone(), server))
                .collect(),
        }
        .into()
    }
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

impl TestServerDescription {
    fn into_server_description(self) -> Option<ServerDescription> {
        let server_type = match self.server_type.into_server_type() {
            Some(server_type) => server_type,
            None => return None,
        };

        let server_address = ServerAddress::parse(self.address).ok()?;
        let tags = self.tags;
        let last_write = self.last_write;
        let avg_rtt_ms = self.avg_rtt_ms;
        let reply = is_master_response_from_server_type(server_type).map(|mut command_response| {
            command_response.tags = tags;
            command_response.last_write = last_write.map(|last_write| LastWrite {
                last_write_date: DateTime::from_millis(last_write.last_write_date),
            });
            Ok(IsMasterReply {
                server_address: server_address.clone(),
                command_response,
                round_trip_time: avg_rtt_ms.map(f64_ms_as_duration),
                cluster_time: None,
            })
        });

        let mut server_desc = ServerDescription::new(server_address, reply);
        server_desc.last_update_time = self
            .last_update_time
            .map(|i| DateTime::from_millis(i.into()));

        Some(server_desc)
    }
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
    #[serde(rename = "RSPrimary")]
    RsPrimary,
    #[serde(rename = "RSSecondary")]
    RsSecondary,
    #[serde(rename = "RSArbiter")]
    RsArbiter,
    #[serde(rename = "RSOther")]
    RsOther,
    #[serde(rename = "RSGhost")]
    RsGhost,
    Unknown,
    PossiblePrimary,
}

impl TestServerType {
    fn into_server_type(self) -> Option<ServerType> {
        match self {
            TestServerType::Standalone => Some(ServerType::Standalone),
            TestServerType::Mongos => Some(ServerType::Mongos),
            TestServerType::RsPrimary => Some(ServerType::RsPrimary),
            TestServerType::RsSecondary => Some(ServerType::RsSecondary),
            TestServerType::RsArbiter => Some(ServerType::RsArbiter),
            TestServerType::RsOther => Some(ServerType::RsOther),
            TestServerType::RsGhost => Some(ServerType::RsGhost),
            TestServerType::Unknown => Some(ServerType::Unknown),
            TestServerType::PossiblePrimary => None,
        }
    }
}

fn is_master_response_from_server_type(server_type: ServerType) -> Option<IsMasterCommandResponse> {
    let mut response = IsMasterCommandResponse::default();

    match server_type {
        ServerType::Unknown => {
            return None;
        }
        ServerType::Mongos => {
            response.msg = Some("isdbgrid".into());
        }
        ServerType::RsPrimary => {
            response.set_name = Some("foo".into());
            response.is_writable_primary = Some(true);
        }
        ServerType::RsOther => {
            response.set_name = Some("foo".into());
            response.hidden = Some(true);
        }
        ServerType::RsSecondary => {
            response.set_name = Some("foo".into());
            response.secondary = Some(true);
        }
        ServerType::RsArbiter => {
            response.set_name = Some("foo".into());
            response.arbiter_only = Some(true);
        }
        ServerType::RsGhost => {
            response.is_replica_set = Some(true);
        }
        ServerType::Standalone | ServerType::LoadBalancer => {}
    };

    Some(response)
}
