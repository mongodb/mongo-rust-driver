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
    sdam::{ServerType, Topology},
    selection_criteria::TagSet,
};

/// The legacy version of the `hello` command which was deprecated in 5.0.
/// To limit usages of the legacy name in the codebase, this constant should be used
/// wherever possible.
pub(crate) const LEGACY_HELLO_COMMAND_NAME: &str = "isMaster";
pub(crate) const LEGACY_HELLO_COMMAND_NAME_LOWERCASE: &str = "ismaster";

/// Construct a hello or legacy hello command, depending on the circumstances.
///
/// If an API version is provided, `hello` will be used.
/// If the server indicated `helloOk: true`, then `hello` will also be used.
/// Otherwise, legacy hello will be used, and if it's unknown whether the server supports hello,
/// the command also will contain `helloOk: true`.
pub(crate) fn hello_command(api: Option<&ServerApi>, hello_ok: Option<bool>) -> Command {
    let (command, command_name) = if api.is_some() || matches!(hello_ok, Some(true)) {
        (doc! { "hello": 1 }, "hello")
    } else {
        let mut cmd = doc! { LEGACY_HELLO_COMMAND_NAME: 1 };
        if hello_ok.is_none() {
            cmd.insert("helloOk", true);
        }
        (cmd, LEGACY_HELLO_COMMAND_NAME)
    };
    let mut command = Command::new(command_name.into(), "admin".into(), command);
    if let Some(server_api) = api {
        command.set_server_api(server_api);
    }
    command
}

/// Execute a hello or legacy hello command, emiting events if a reference to the topology and a
/// handler are provided.
///
/// A strong reference to the topology is used here to ensure it is still in scope and has not yet
/// emitted a `TopologyClosedEvent`.
pub(crate) async fn run_hello(
    conn: &mut Connection,
    command: Command,
    topology: Option<&Topology>,
    handler: &Option<Arc<dyn SdamEventHandler>>,
) -> Result<HelloReply> {
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
        let hello_reply = raw_response.to_hello_reply(round_trip_time)?;
        Ok((raw_response, hello_reply))
    }) {
        Ok((raw_response, hello_reply)) => {
            emit_event(topology, handler, |handler| {
                let mut reply = raw_response
                    .body::<Document>()
                    .unwrap_or_else(|e| doc! { "deserialization error": e.to_string() });
                // if this hello call is part of a handshake, remove speculative authentication
                // information before publishing an event
                reply.remove("speculativeAuthenticate");
                let event = ServerHeartbeatSucceededEvent {
                    duration: round_trip_time,
                    reply,
                    server_address: conn.address.clone(),
                };
                handler.handle_server_heartbeat_succeeded_event(event);
            });
            Ok(hello_reply)
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

fn emit_event<F>(topology: Option<&Topology>, handler: &Option<Arc<dyn SdamEventHandler>>, emit: F)
where
    F: FnOnce(&Arc<dyn SdamEventHandler>),
{
    if let Some(handler) = handler {
        if topology.is_some() {
            emit(handler);
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HelloReply {
    pub server_address: ServerAddress,
    pub command_response: HelloCommandResponse,
    pub round_trip_time: Duration,
    pub cluster_time: Option<ClusterTime>,
}

/// The response to a `hello` command.
///
/// See the documentation [here](https://docs.mongodb.com/manual/reference/command/hello/) for more details.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HelloCommandResponse {
    /// Whether the server is writable. If true, this instance is a primary in a replica set, a
    /// mongos instance, or a standalone mongod.
    pub is_writable_primary: Option<bool>,

    #[serde(rename = "ismaster")]
    /// Legacy name for `is_writable_primary` field.
    pub is_master: Option<bool>,

    /// Whether or not the server supports using the `hello` command for monitoring instead
    /// of the legacy hello command.
    pub hello_ok: Option<bool>,

    /// The list of all hosts.
    pub hosts: Option<Vec<String>>,

    /// The list of all passives in a replica set.
    pub passives: Option<Vec<String>>,

    /// The list of all arbiters in a replica set.
    pub arbiters: Option<Vec<String>>,

    /// An optional message. This contains the value "isdbgrid" when returned from a mongos.
    pub msg: Option<String>,

    /// The address of the server that returned this `HelloCommandResponse`.
    pub me: Option<String>,

    #[serde(rename = "compression")]
    /// The list of compatible compressors that the server returned.
    pub compressors: Option<Vec<String>>,

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

impl PartialEq for HelloCommandResponse {
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

impl HelloCommandResponse {
    pub(crate) fn server_type(&self) -> ServerType {
        if self.msg.as_deref() == Some("isdbgrid") {
            ServerType::Mongos
        } else if self.set_name.is_some() {
            if self.hidden == Some(true) {
                ServerType::RsOther
            } else if self.is_writable_primary == Some(true) || self.is_master == Some(true) {
                ServerType::RsPrimary
            } else if self.secondary == Some(true) {
                ServerType::RsSecondary
            } else if self.arbiter_only == Some(true) {
                ServerType::RsArbiter
            } else {
                ServerType::RsOther
            }
        } else if self.is_replica_set == Some(true) {
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
