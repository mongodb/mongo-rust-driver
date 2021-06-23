use std::{collections::HashMap, sync::Arc, time::Duration};

use bson::Document;
use serde::Deserialize;
use tokio::sync::RwLockReadGuard;

use crate::{
    bson::{doc, oid::ObjectId},
    client::Client,
    error::{BulkWriteFailure, CommandError, Error, ErrorKind},
    is_master::{IsMasterCommandResponse, IsMasterReply, LastWrite},
    options::{ClientOptions, ReadPreference, SelectionCriteria, ServerAddress},
    sdam::{
        description::{
            server::{ServerDescription, ServerType},
            topology::TopologyType,
        },
        HandshakePhase,
        Topology,
    },
    selection_criteria::TagSet,
    test::{run_spec_test, TestClient, CLIENT_OPTIONS, LOCK},
};

#[derive(Debug, Deserialize)]
pub struct TestFile {
    description: String,
    uri: String,
    phases: Vec<Phase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Phase {
    description: Option<String>,
    #[serde(default)]
    responses: Vec<Response>,
    #[serde(default)]
    application_errors: Vec<ApplicationError>,
    outcome: Outcome,
}

#[derive(Debug, Deserialize)]
pub struct Response(String, TestIsMasterCommandResponse);

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TestIsMasterCommandResponse {
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
    pub speculative_authenticate: Option<Document>,
    pub max_bson_object_size: Option<i64>,
    pub max_write_batch_size: Option<i64>,
}

impl From<TestIsMasterCommandResponse> for IsMasterCommandResponse {
    fn from(test: TestIsMasterCommandResponse) -> Self {
        IsMasterCommandResponse {
            is_master: test.is_master,
            ok: test.ok,
            hosts: test.hosts,
            passives: test.passives,
            arbiters: test.arbiters,
            msg: test.msg,
            me: test.me,
            set_version: test.set_version,
            set_name: test.set_name,
            hidden: test.hidden,
            secondary: test.secondary,
            arbiter_only: test.arbiter_only,
            is_replica_set: test.is_replica_set,
            logical_session_timeout_minutes: test.logical_session_timeout_minutes,
            last_write: test.last_write,
            min_wire_version: test.min_wire_version,
            max_wire_version: test.max_wire_version,
            tags: test.tags,
            election_id: test.election_id,
            primary: test.primary,
            sasl_supported_mechs: test.sasl_supported_mechs,
            speculative_authenticate: test.speculative_authenticate,
            max_bson_object_size: test.max_bson_object_size.unwrap_or(1234),
            max_write_batch_size: test.max_write_batch_size.unwrap_or(1234),
        }
    }
}

impl PartialEq for TestIsMasterCommandResponse {
    fn eq(&self, other: &Self) -> bool {
        IsMasterCommandResponse::from(self.clone()) == other.clone().into()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationError {
    address: ServerAddress,
    generation: Option<u32>,
    max_wire_version: i32,
    when: ErrorHandshakePhase,
    #[serde(rename = "type")]
    error_type: ErrorType,
    response: Option<ServerError>,
}

impl ApplicationError {
    fn to_error(&self) -> Error {
        match self.error_type {
            ErrorType::Command => self.response.clone().unwrap().into(),
            ErrorType::Network => {
                ErrorKind::Io(Arc::new(std::io::ErrorKind::UnexpectedEof.into())).into()
            }
            ErrorType::Timeout => {
                ErrorKind::Io(Arc::new(std::io::ErrorKind::TimedOut.into())).into()
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ErrorHandshakePhase {
    BeforeHandshakeCompletes,
    AfterHandshakeCompletes,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ErrorType {
    Command,
    Network,
    Timeout,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum ServerError {
    CommandError(CommandError),
    WriteError(BulkWriteFailure),
}

impl From<ServerError> for Error {
    fn from(server_error: ServerError) -> Self {
        match server_error {
            ServerError::CommandError(command_error) => ErrorKind::Command(command_error).into(),
            ServerError::WriteError(bwf) => ErrorKind::BulkWrite(bwf).into(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Outcome {
    topology_type: TopologyType,
    set_name: Option<String>,
    servers: HashMap<String, Server>,
    logical_session_timeout_minutes: Option<i32>,
    compatible: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Server {
    #[serde(rename = "type")]
    server_type: String,
    set_name: Option<String>,
    set_version: Option<i32>,
    election_id: Option<ObjectId>,
    logical_session_timeout_minutes: Option<i32>,
    min_wire_version: Option<i32>,
    max_wire_version: Option<i32>,
}

fn server_type_from_str(s: &str) -> Option<ServerType> {
    let t = match s {
        "Standalone" => ServerType::Standalone,
        "Mongos" => ServerType::Mongos,
        "RSPrimary" => ServerType::RsPrimary,
        "RSSecondary" => ServerType::RsSecondary,
        "RSArbiter" => ServerType::RsArbiter,
        "RSOther" => ServerType::RsOther,
        "RSGhost" => ServerType::RsGhost,
        "Unknown" | "PossiblePrimary" => ServerType::Unknown,
        _ => return None,
    };

    Some(t)
}

async fn run_test(test_file: TestFile) {
    let options = ClientOptions::parse_uri(&test_file.uri, None)
        .await
        .expect(&test_file.description);

    let test_description = &test_file.description;

    // TODO: RUST-360 unskip tests that rely on topology version
    if test_description.contains("topologyVersion") {
        println!("Skipping {} (RUST-360)", test_description);
        return;
    }

    let topology = Topology::new_mocked(options.clone());
    let mut servers = topology.get_servers().await;

    for (i, phase) in test_file.phases.into_iter().enumerate() {
        for Response(address, command_response) in phase.responses {
            let is_master_reply = if command_response == Default::default() {
                Err("dummy error".to_string())
            } else {
                Ok(IsMasterReply {
                    command_response: command_response.into(),
                    round_trip_time: Some(Duration::from_millis(1234)), // Doesn't matter for tests.
                    cluster_time: None,
                })
            };

            let address = ServerAddress::parse(&address).unwrap_or_else(|_| {
                panic!(
                    "{}: couldn't parse address \"{:?}\"",
                    test_description.as_str(),
                    address
                )
            });

            // only update server if we have strong reference to it like the monitors do
            if let Some(server) = servers.get(&address).and_then(|s| s.upgrade()) {
                let new_sd = ServerDescription::new(address.clone(), Some(is_master_reply));
                if topology.update(&server, new_sd).await {
                    servers = topology.get_servers().await
                }
            }
        }

        for application_error in phase.application_errors {
            // only update server if we have strong reference to it like is done as part of
            // operation execution
            if let Some(server) = servers
                .get(&application_error.address)
                .and_then(|s| s.upgrade())
            {
                let error = application_error.to_error();
                let error_generation = application_error
                    .generation
                    .unwrap_or_else(|| server.pool.generation());
                let handshake_phase = match application_error.when {
                    ErrorHandshakePhase::BeforeHandshakeCompletes => {
                        HandshakePhase::BeforeCompletion {
                            generation: error_generation,
                        }
                    }
                    ErrorHandshakePhase::AfterHandshakeCompletes => {
                        HandshakePhase::AfterCompletion {
                            generation: error_generation,
                            max_wire_version: application_error.max_wire_version,
                        }
                    }
                };

                topology
                    .handle_application_error(error, handshake_phase, &server)
                    .await;
            }
        }

        let topology_description = topology.description().await;
        let phase_description = phase.description.unwrap_or_else(|| format!("{}", i));
        assert_eq!(
            topology_description.topology_type, phase.outcome.topology_type,
            "{}: {}",
            &test_file.description, phase_description
        );

        assert_eq!(
            topology_description.set_name, phase.outcome.set_name,
            "{}: {}",
            &test_file.description, phase_description,
        );

        let expected_timeout = phase
            .outcome
            .logical_session_timeout_minutes
            .map(|mins| Duration::from_secs((mins as u64) * 60));
        assert_eq!(
            topology_description
                .session_support_status
                .logical_session_timeout(),
            expected_timeout,
            "{}: {}",
            &test_file.description,
            phase_description
        );

        if let Some(compatible) = phase.outcome.compatible {
            assert_eq!(
                topology_description.compatibility_error.is_none(),
                compatible,
                "{}: {}",
                &test_file.description,
                phase_description,
            );
        }

        assert_eq!(
            topology_description.servers.len(),
            phase.outcome.servers.len(),
            "{}: {}",
            &test_file.description,
            phase_description
        );

        for (address, server) in phase.outcome.servers {
            let address = ServerAddress::parse(&address).unwrap_or_else(|_| {
                panic!(
                    "{}: couldn't parse address \"{:?}\"",
                    test_description, address
                )
            });
            let actual_server = &topology_description
                .servers
                .get(&address)
                .unwrap_or_else(|| panic!("{} (phase {})", test_description, phase_description));

            let server_type = server_type_from_str(&server.server_type)
                .unwrap_or_else(|| panic!("{} (phase {})", test_description, phase_description));

            assert_eq!(
                actual_server.server_type, server_type,
                "{} (phase {}, address: {})",
                &test_file.description, phase_description, address,
            );

            assert_eq!(
                actual_server.set_name().unwrap_or(None),
                server.set_name,
                "{} (phase {})",
                &test_file.description,
                phase_description
            );

            assert_eq!(
                actual_server.set_version().unwrap_or(None),
                server.set_version,
                "{} (phase {})",
                &test_file.description,
                phase_description
            );

            assert_eq!(
                actual_server.election_id().unwrap_or(None),
                server.election_id,
                "{} (phase {})",
                &test_file.description,
                phase_description
            );

            if let Some(logical_session_timeout_minutes) = server.logical_session_timeout_minutes {
                assert_eq!(
                    actual_server.logical_session_timeout().unwrap(),
                    Some(Duration::from_secs(
                        logical_session_timeout_minutes as u64 * 60
                    )),
                    "{} (phase {})",
                    &test_file.description,
                    phase_description
                );
            }

            if let Some(min_wire_version) = server.min_wire_version {
                assert_eq!(
                    actual_server.min_wire_version().unwrap(),
                    Some(min_wire_version),
                    "{} (phase {})",
                    &test_file.description,
                    phase_description
                );
            }

            if let Some(max_wire_version) = server.max_wire_version {
                assert_eq!(
                    actual_server.max_wire_version().unwrap(),
                    Some(max_wire_version),
                    "{} (phase {})",
                    &test_file.description,
                    phase_description
                );
            }
        }
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn single() {
    run_spec_test(&["server-discovery-and-monitoring", "single"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn rs() {
    run_spec_test(&["server-discovery-and-monitoring", "rs"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn sharded() {
    run_spec_test(&["server-discovery-and-monitoring", "sharded"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn errors() {
    run_spec_test(&["server-discovery-and-monitoring", "errors"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn direct_connection() {
    let _guard: RwLockReadGuard<_> = LOCK.run_concurrently().await;

    let test_client = TestClient::new().await;
    if !test_client.is_replica_set() {
        println!("Skipping due to non-replica set topology");
        return;
    }

    let criteria = SelectionCriteria::ReadPreference(ReadPreference::Secondary {
        options: Default::default(),
    });
    let secondary_address = test_client
        .test_select_server(Some(&criteria))
        .await
        .expect("failed to select secondary");

    let mut secondary_options = CLIENT_OPTIONS.clone();
    secondary_options.hosts = vec![secondary_address];

    let mut direct_false_options = secondary_options.clone();
    direct_false_options.direct_connection = Some(false);
    let direct_false_client =
        Client::with_options(direct_false_options).expect("client construction should succeed");
    direct_false_client
        .database(function_name!())
        .collection(function_name!())
        .insert_one(doc! {}, None)
        .await
        .expect("write should succeed with directConnection=false on secondary");

    let mut direct_true_options = secondary_options.clone();
    direct_true_options.direct_connection = Some(true);
    let direct_true_client =
        Client::with_options(direct_true_options).expect("client construction should succeed");
    let error = direct_true_client
        .database(function_name!())
        .collection(function_name!())
        .insert_one(doc! {}, None)
        .await
        .expect_err("write should fail with directConnection=true on secondary");
    assert!(error.is_not_master());

    let client =
        Client::with_options(secondary_options).expect("client construction should succeed");
    client
        .database(function_name!())
        .collection(function_name!())
        .insert_one(doc! {}, None)
        .await
        .expect("write should succeed with directConnection unspecified");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn pool_cleared_error_does_not_mark_unknown() {
    let address = ServerAddress::parse("a:1234").unwrap();
    let options = ClientOptions::builder()
        .hosts(vec![address.clone()])
        .build();
    let topology = Topology::new_mocked(options);

    // get the one server in the topology
    let server = topology
        .get_servers()
        .await
        .into_iter()
        .next()
        .unwrap()
        .1
        .upgrade()
        .unwrap();

    let heartbeat_response: IsMasterCommandResponse = bson::from_document(doc! {
        "ok": 1,
        "ismaster": true,
        "minWireVersion": 0,
        "maxWireVersion": 6,
        "maxBsonObjectSize": 16_000,
        "maxWriteBatchSize": 10_000,
    })
    .unwrap();

    // discover the node
    topology
        .update(
            &server,
            ServerDescription::new(
                address.clone(),
                Some(Ok(IsMasterReply {
                    command_response: heartbeat_response,
                    round_trip_time: Some(Duration::from_secs(1)),
                    cluster_time: None,
                })),
            ),
        )
        .await;
    assert_eq!(
        topology
            .get_server_description(&address)
            .await
            .unwrap()
            .server_type,
        ServerType::Standalone
    );

    // assert a pool cleared error would have no effect on the topology
    let error: Error = ErrorKind::ConnectionPoolCleared {
        message: "foo".to_string(),
    }
    .into();
    let phase = HandshakePhase::BeforeCompletion {
        generation: server.pool.generation(),
    };
    assert!(
        !topology
            .handle_application_error(error, phase, &server)
            .await
    );
    assert_eq!(
        topology
            .get_server_description(&address)
            .await
            .unwrap()
            .server_type,
        ServerType::Standalone
    );
}
