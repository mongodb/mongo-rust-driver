use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use crate::{
    bson::{doc, oid::ObjectId, DateTime, Document, Timestamp},
    client::{
        options::{ServerAddress, ServerApi},
        ClusterTime,
    },
    cmap::{Command, Connection},
    error::Result,
    event::sdam::{
        SdamEventHandler,
        ServerHeartbeatFailedEvent,
        ServerHeartbeatStartedEvent,
        ServerHeartbeatSucceededEvent,
    },
    sdam::{ServerType, WeakTopology},
    selection_criteria::TagSet,
    compression::Compressor,
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

pub(crate) async fn run_is_master(
    conn: &mut Connection,
    command: Command,
    topology: Option<&WeakTopology>,
    handler: &Option<Arc<dyn SdamEventHandler>>,
) -> Result<IsMasterReply> {
    emit_event(topology, handler, |handler| {
        let event = ServerHeartbeatStartedEvent {
            server_address: conn.address.clone(),
        };
        handler.handle_server_heartbeat_started_event(event);
    });

    let start_time = Instant::now();
    let response_result = conn.send_command(command, None).await;
    let end_time = Instant::now();

    let round_trip_time = end_time.duration_since(start_time);

    match response_result.and_then(|raw_response| {
        let is_master_reply = raw_response.to_is_master_response(round_trip_time)?;
        Ok((raw_response, is_master_reply))
    }) {
        Ok((raw_response, is_master_reply)) => {
            emit_event(topology, handler, |handler| {
                let mut reply = raw_response
                    .body::<Document>()
                    .unwrap_or_else(|e| doc! { "deserialization error": e.to_string() });
                // if this isMaster call is part of a handshake, remove speculative authentication
                // information before publishing an event
                reply.remove("speculativeAuthenticate");
                let event = ServerHeartbeatSucceededEvent {
                    duration: round_trip_time,
                    reply,
                    server_address: conn.address.clone(),
                };
                handler.handle_server_heartbeat_succeeded_event(event);
            });
            Ok(is_master_reply)
        }
        Err(err) => {
            emit_event(topology, handler, |handler| {
                let event = ServerHeartbeatFailedEvent {
                    duration: round_trip_time,
                    failure: err.clone(),
                    server_address: conn.address.clone(),
                };
                handler.handle_server_heartbeat_failed_event(event);
            });
            Err(err)
        }
    }
}

fn emit_event<F>(
    topology: Option<&WeakTopology>,
    handler: &Option<Arc<dyn SdamEventHandler>>,
    emit: F,
) where
    F: FnOnce(&Arc<dyn SdamEventHandler>),
{
    if let Some(handler) = handler {
        if let Some(topology) = topology {
            if topology.is_alive() {
                emit(handler);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IsMasterReply {
    pub server_address: ServerAddress,
    pub command_response: IsMasterCommandResponse,
    pub round_trip_time: Duration,
    pub cluster_time: Option<ClusterTime>,
}

/// The response to a `hello` command.
///
/// See the documentation [here](https://docs.mongodb.com/manual/reference/command/hello/) for more details.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct IsMasterCommandResponse {
    /// Whether the server is writable. If true, this instance is a primary in a replica set, a
    /// mongos instance, or a standalone mongod.
    pub is_writable_primary: Option<bool>,
    #[serde(rename = "ismaster")]

    /// Legacy name for `is_writable_primary` field.
    pub is_master: Option<bool>,

    /// The list of all hosts.
    pub hosts: Option<Vec<String>>,

    /// The list of all passives in a replica set.
    pub passives: Option<Vec<String>>,

    /// The list of all arbiters in a replica set.
    pub arbiters: Option<Vec<String>>,

    /// An optional message. This contains the value "isdbgrid" when returned from a mongos.
    pub msg: Option<String>,

    /// The address of the server that returned this `IsMasterCommandResponse`.
    pub me: Option<String>,

    #[serde(rename = "compression")]
    /// The list of compatible compressors that the server returned.
    pub compressors: Option<Vec<Compressor>>,

    /// The current replica set config version.
    pub set_version: Option<i32>,

    /// The name of the current replica set.
    pub set_name: Option<String>,

    /// Whether the server is hidden.
    pub hidden: Option<bool>,

    /// Whether the server is a secondary.
    pub secondary: Option<bool>,

    /// Whether the server is an arbiter.
    pub arbiter_only: Option<bool>,
    #[serde(rename = "isreplicaset")]

    /// Whether the server is a replica set.
    pub is_replica_set: Option<bool>,

    /// The time in minutes that a session remains active after its most recent use.
    pub logical_session_timeout_minutes: Option<i64>,

    /// Optime and date information for the server's most recent write operation.
    pub last_write: Option<LastWrite>,

    /// The minimum wire version that the server supports.
    pub min_wire_version: Option<i32>,

    /// The maximum wire version that the server supports.
    pub max_wire_version: Option<i32>,

    /// User-defined tags for a replica set member.
    pub tags: Option<TagSet>,

    /// A unique identifier for each election.
    pub election_id: Option<ObjectId>,

    /// The address of current primary member of the replica set.
    pub primary: Option<String>,

    /// A list of SASL mechanisms used to create the user's credential(s).
    pub sasl_supported_mechs: Option<Vec<String>>,

    /// The reply to speculative authentication done in the authentication handshake.
    pub speculative_authenticate: Option<Document>,

    /// The maximum permitted size of a BSON object in bytes.
    pub max_bson_object_size: i64,

    /// The maximum number of write operations permitted in a write batch.
    pub max_write_batch_size: i64,

    /// If the connection is to a load balancer, the id of the selected backend.
    pub service_id: Option<ObjectId>,

    /// For internal use.
    pub topology_version: Option<Document>,
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

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LastWrite {
    pub last_write_date: DateTime,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct OpTime {
    ts: Timestamp,
    t: i32,
}
