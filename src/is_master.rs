use std::time::Duration;

use bson::{oid::ObjectId, TimeStamp, UtcDateTime};
use serde::Deserialize;

use crate::{read_preference::TagSet, sdam::ServerType};

#[derive(Debug, Clone)]
pub(crate) struct IsMasterReply {
    pub command_response: IsMasterCommandResponse,
    pub round_trip_time: Option<Duration>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct IsMasterCommandResponse {
    #[serde(rename = "ismaster")]
    pub is_master: Option<bool>,
    pub ok: Option<f32>,
    pub hosts: Option<Vec<String>>,
    pub passives: Option<Vec<String>>,
    pub arbiters: Option<Vec<String>>,
    pub msg: Option<String>,
    pub me: Option<String>,
    pub set_version: Option<i32>,
    pub set_name: Option<String>,
    pub hidden: Option<bool>,
    pub secondary: Option<bool>,
    pub arbiter_only: Option<bool>,
    #[serde(rename = "isreplicaset")]
    pub is_replica_set: Option<bool>,
    pub logical_session_timeout_minutes: Option<i64>,
    pub last_write: Option<LastWrite>,
    pub min_wire_version: Option<i32>,
    pub max_wire_version: Option<i32>,
    pub tags: Option<TagSet>,
    pub election_id: Option<ObjectId>,
    pub primary: Option<String>,
    pub sasl_supported_mechs: Option<Vec<String>>,
}

impl PartialEq for IsMasterCommandResponse {
    fn eq(&self, other: &Self) -> bool {
        self.server_type() == other.server_type()
            && self.min_wire_version == other.min_wire_version
            && self.max_wire_version == other.max_wire_version
            && self.me == other.me
            && self.hosts == other.hosts
            && self.passives == other.passives
            && self.arbiters == other.arbiters
            && self.tags == other.tags
            && self.set_name == other.set_name
            && self.set_version == other.set_version
            && self.election_id == other.election_id
            && self.primary == other.primary
            && self.logical_session_timeout_minutes == other.logical_session_timeout_minutes
    }
}

impl IsMasterCommandResponse {
    pub(crate) fn server_type(&self) -> ServerType {
        if self.ok != Some(1.0) {
            ServerType::Unknown
        } else if self.msg.as_ref().map(String::as_str) == Some("isdbgrid") {
            ServerType::Mongos
        } else if self.set_name.is_some() {
            if let Some(true) = self.hidden {
                ServerType::RSOther
            } else if let Some(true) = self.is_master {
                ServerType::RSPrimary
            } else if let Some(true) = self.secondary {
                ServerType::RSSecondary
            } else if let Some(true) = self.arbiter_only {
                ServerType::RSArbiter
            } else {
                ServerType::RSOther
            }
        } else if let Some(true) = self.is_replica_set {
            ServerType::RSGhost
        } else {
            ServerType::Standalone
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LastWrite {
    pub op_time: OpTime,
    pub last_write_date: UtcDateTime,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct OpTime {
    ts: TimeStamp,
    t: i32,
}
