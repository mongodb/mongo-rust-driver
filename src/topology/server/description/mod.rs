#[cfg(test)]
mod test;

use std::collections::HashMap;

use bson::{oid::ObjectId, TimeStamp, UtcDateTime};

use crate::{
    command_responses::IsMasterCommandResponse,
    error::Result,
    pool::IsMasterReply,
    read_preference::{ReadPreference, TagSet},
    topology::TopologyType,
};

const ROUND_TRIP_TIME_WEIGHT_FACTOR: f64 = 0.2;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize)]
pub enum ServerType {
    Standalone,
    Mongos,
    RSPrimary,
    RSSecondary,
    RSArbiter,
    RSOther,
    RSGhost,
    Unknown,
}

impl ServerType {
    pub(crate) fn from_ismaster_response(is_master: &IsMasterCommandResponse) -> ServerType {
        #[cfg_attr(feature = "cargo-clippy", allow(clippy::if_same_then_else))]
        {
            if is_master.ok != Some(1.0) {
                ServerType::Unknown
            } else if is_master.msg.as_ref().map(|s| &s[..]) == Some("isdbgrid") {
                ServerType::Mongos
            } else if is_master.set_name.is_some() {
                if is_master.is_master.unwrap_or(false) {
                    ServerType::RSPrimary
                } else if is_master.hidden.unwrap_or(false) {
                    ServerType::RSOther
                } else if is_master.secondary.unwrap_or(false) {
                    ServerType::RSSecondary
                } else if is_master.arbiter_only.unwrap_or(false) {
                    ServerType::RSArbiter
                } else {
                    ServerType::RSOther
                }
            } else if is_master.is_replica_set.unwrap_or(false) {
                ServerType::RSGhost
            } else {
                ServerType::Standalone
            }
        }
    }

    pub(crate) fn can_auth(self) -> bool {
        match self {
            ServerType::Standalone
            | ServerType::RSPrimary
            | ServerType::RSSecondary
            | ServerType::Mongos => true,
            _ => false,
        }
    }
}

impl Default for ServerType {
    fn default() -> Self {
        ServerType::Unknown
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct OpTime {
    ts: TimeStamp,
    t: i32,
}

#[derive(Debug, Default, Clone)]
pub struct ServerDescription {
    pub address: String,
    pub server_type: ServerType,
    pub error: Option<String>,
    pub round_trip_time: Option<f64>,
    pub last_write_date: Option<UtcDateTime>,
    pub op_time: Option<OpTime>,
    pub min_wire_version: i32,
    pub max_wire_version: i32,
    pub me: Option<String>,
    pub hosts: Vec<String>,
    pub passives: Vec<String>,
    pub arbiters: Vec<String>,
    pub tags: HashMap<String, String>,
    pub set_name: Option<String>,
    pub set_version: Option<i32>,
    pub election_id: Option<ObjectId>,
    pub primary: Option<String>,
    pub last_update_time: Option<UtcDateTime>,
    pub logical_session_timeout_minutes: Option<i64>,
}

impl ServerDescription {
    pub fn new(address: &str, is_master_reply: Option<Result<IsMasterReply>>) -> Self {
        let mut description = Self {
            address: address.to_string(),
            ..Default::default()
        };

        let is_master_reply = match is_master_reply {
            Some(r) => r,
            None => return description,
        };

        let (is_master, round_trip_time) = match is_master_reply {
            Ok(IsMasterReply {
                command_response,
                round_trip_time,
            }) => (command_response, round_trip_time),
            Err(e) => {
                description.error = Some(format!("{}", e));
                return description;
            }
        };

        description.round_trip_time = Some(round_trip_time as f64);

        description.server_type = ServerType::from_ismaster_response(&is_master);

        description.logical_session_timeout_minutes = is_master.logical_session_timeout_minutes;

        if let Some(last_write) = is_master.last_write {
            description.op_time = Some(last_write.op_time);
            description.last_write_date = Some(last_write.last_write_date);
        }

        description.min_wire_version = is_master.min_wire_version.unwrap_or(0);
        description.max_wire_version = is_master.max_wire_version.unwrap_or(0);

        description.me = is_master.me.map(|s| s.to_lowercase());

        if let Some(hosts) = is_master.hosts {
            description.hosts = hosts.into_iter().map(|s| s.to_lowercase()).collect();
        }

        if let Some(passives) = is_master.passives {
            description.passives = passives.into_iter().map(|s| s.to_lowercase()).collect();
        }

        if let Some(arbiters) = is_master.arbiters {
            description.arbiters = arbiters.into_iter().map(|s| s.to_lowercase()).collect();
        }

        if let Some(tags) = is_master.tags {
            description.tags = tags;
        }

        description.set_version = is_master.set_version;
        description.set_name = is_master.set_name;
        description.election_id = is_master.election_id;
        description.primary = is_master.primary;
        description
    }

    pub fn update_round_trip_time(&mut self, old_round_trip_time: Option<f64>) {
        if let Some(new_rtt) = self.round_trip_time {
            if let Some(old_rtt) = old_round_trip_time {
                self.round_trip_time = Some(
                    ROUND_TRIP_TIME_WEIGHT_FACTOR * new_rtt
                        + (1.0 - ROUND_TRIP_TIME_WEIGHT_FACTOR) * old_rtt,
                );
            }
        }
    }

    pub fn matches(&self, topology_type: TopologyType, read_pref: Option<&ReadPreference>) -> bool {
        self.matches_mode(topology_type, read_pref.unwrap_or(&ReadPreference::Primary))
    }

    fn matches_mode(&self, topology_type: TopologyType, mode: &ReadPreference) -> bool {
        match mode {
            ReadPreference::Primary => self.server_type == ServerType::RSPrimary,
            ReadPreference::Secondary { tag_sets, .. } => {
                self.server_type == ServerType::RSSecondary
                    && self.matches_tag_sets(tag_sets.as_ref().map(|v| v.as_slice()))
            }
            ReadPreference::PrimaryPreferred { tag_sets, .. } => {
                self.server_type == ServerType::RSPrimary
                    || (topology_type == TopologyType::ReplicaSetNoPrimary
                        && self.server_type == ServerType::RSSecondary)
                        && self.matches_tag_sets(tag_sets.as_ref().map(|v| v.as_slice()))
            }
            ReadPreference::SecondaryPreferred { tag_sets, .. } => {
                self.server_type == ServerType::RSPrimary
                    || (self.server_type == ServerType::RSSecondary
                        && self.matches_tag_sets(tag_sets.as_ref().map(|v| v.as_slice())))
            }
            ReadPreference::Nearest { tag_sets, .. } => {
                self.matches_tag_sets(tag_sets.as_ref().map(|v| v.as_slice()))
            }
        }
    }

    fn matches_tag_sets(&self, tag_sets: Option<&[TagSet]>) -> bool {
        let tag_sets = match tag_sets {
            Some(tags) => tags,
            None => return true,
        };

        if tag_sets.is_empty() {
            return true;
        }

        tag_sets.iter().any(|tag_set| {
            tag_set
                .iter()
                .all(|(tag_name, tag_value)| self.tags.get(tag_name) == Some(tag_value))
        })
    }
}
