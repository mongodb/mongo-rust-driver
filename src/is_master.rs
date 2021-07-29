use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::{
    bson::{doc, oid::ObjectId, DateTime, Document, Timestamp},
    client::{
        options::{ServerAddress, ServerApi},
        ClusterTime,
    },
    cmap::{Command, Connection},
    error::{ErrorKind, Result},
    sdam::ServerType,
    selection_criteria::TagSet,
};

/// Construct an isMaster command.
pub(crate) fn is_master_command(api: Option<&ServerApi>) -> Command {
    let command_name = if api.is_some() { "hello" } else { "isMaster" };
    let mut command = Command::new(
        command_name.into(),
        "admin".into(),
        doc! { command_name: 1 },
    );
    if let Some(server_api) = api {
        command.set_server_api(server_api);
    }
    command
}

/// Run the given isMaster command.
///
/// If the given command is not an isMaster, this function will return an error.
pub(crate) async fn run_is_master(
    command: Command,
    conn: &mut Connection,
) -> Result<IsMasterReply> {
    if !command.name.eq_ignore_ascii_case("ismaster") && !command.name.eq_ignore_ascii_case("hello")
    {
        return Err(ErrorKind::Internal {
            message: format!("invalid ismaster command: {}", command.name),
        }
        .into());
    }
    let start_time = Instant::now();
    let response = conn.send_command(command, None).await?;
    let end_time = Instant::now();

    let server_address = response.source_address().clone();
    let basic_response = response.into_document_response()?;
    basic_response.validate()?;
    let cluster_time = basic_response.cluster_time().cloned();
    let command_response: IsMasterCommandResponse = basic_response.body()?;

    Ok(IsMasterReply {
        server_address,
        command_response,
        round_trip_time: Some(end_time.duration_since(start_time)),
        cluster_time,
    })
}

#[derive(Debug, Clone)]
pub(crate) struct IsMasterReply {
    pub server_address: ServerAddress,
    pub command_response: IsMasterCommandResponse,
    pub round_trip_time: Option<Duration>,
    pub cluster_time: Option<ClusterTime>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct IsMasterCommandResponse {
    pub is_writable_primary: Option<bool>,
    #[serde(rename = "ismaster")]
    pub is_master: Option<bool>,
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
    pub speculative_authenticate: Option<Document>,
    pub max_bson_object_size: i64,
    pub max_write_batch_size: i64,
    pub service_id: Option<ObjectId>,
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
            && self.max_bson_object_size == other.max_bson_object_size
            && self.max_write_batch_size == other.max_write_batch_size
            && self.service_id == other.service_id
    }
}

impl IsMasterCommandResponse {
    pub(crate) fn server_type(&self) -> ServerType {
        if self.msg.as_deref() == Some("isdbgrid") {
            ServerType::Mongos
        } else if self.set_name.is_some() {
            if let Some(true) = self.hidden {
                ServerType::RsOther
            } else if let Some(true) = self.is_writable_primary {
                ServerType::RsPrimary
            } else if let Some(true) = self.is_master {
                ServerType::RsPrimary
            } else if let Some(true) = self.secondary {
                ServerType::RsSecondary
            } else if let Some(true) = self.arbiter_only {
                ServerType::RsArbiter
            } else {
                ServerType::RsOther
            }
        } else if let Some(true) = self.is_replica_set {
            ServerType::RsGhost
        } else {
            ServerType::Standalone
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LastWrite {
    pub last_write_date: DateTime,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct OpTime {
    ts: Timestamp,
    t: i32,
}
