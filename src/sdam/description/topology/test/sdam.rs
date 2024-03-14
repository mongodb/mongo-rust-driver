use std::{collections::HashMap, sync::Arc, time::Duration};

use bson::Document;
use serde::Deserialize;

use super::TestSdamEvent;

use crate::{
    bson::{doc, oid::ObjectId},
    client::Client,
    cmap::{conn::ConnectionGeneration, PoolGeneration},
    error::{BulkWriteFailure, CommandError, Error, ErrorKind},
    event::sdam::SdamEvent,
    hello::{HelloCommandResponse, HelloReply, LastWrite, LEGACY_HELLO_COMMAND_NAME},
    options::{ClientOptions, ReadPreference, SelectionCriteria, ServerAddress},
    sdam::{
        description::{
            server::{ServerDescription, ServerType},
            topology::TopologyType,
        },
        HandshakePhase,
        Topology,
        TopologyDescription,
        TopologyVersion,
    },
    selection_criteria::TagSet,
    test::{
        get_client_options,
        log_uncaptured,
        run_spec_test,
        Event,
        EventClient,
        EventBuffer,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
    },
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
pub struct Response(String, TestHelloCommandResponse);

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TestHelloCommandResponse {
    pub is_writable_primary: Option<bool>,
    #[serde(rename = "ismaster")]
    pub is_master: Option<bool>,
    pub hello_ok: Option<bool>,
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
    pub service_id: Option<ObjectId>,
    pub topology_version: Option<TopologyVersion>,
}

impl From<TestHelloCommandResponse> for HelloCommandResponse {
    fn from(test: TestHelloCommandResponse) -> Self {
        HelloCommandResponse {
            is_writable_primary: test.is_writable_primary,
            is_master: test.is_master,
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
            max_write_batch_size: Some(test.max_write_batch_size.unwrap_or(1234)),
            service_id: test.service_id,
            topology_version: test.topology_version,
            compressors: None,
            hello_ok: test.hello_ok,
            max_message_size_bytes: 48 * 1024 * 1024,
            connection_id: None,
        }
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
#[serde(untagged)]
pub enum Outcome {
    Description(DescriptionOutcome),
    Events(EventsOutcome),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DescriptionOutcome {
    topology_type: TopologyType,
    set_name: Option<String>,
    servers: HashMap<String, Server>,
    logical_session_timeout_minutes: Option<i32>,
    compatible: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct EventsOutcome {
    events: Vec<TestSdamEvent>,
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
    topology_version: Option<TopologyVersion>,
    pool: Option<Pool>,
}

#[derive(Debug, Deserialize)]
pub struct Pool {
    generation: u32,
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
        "LoadBalancer" => ServerType::LoadBalancer,
        "Unknown" | "PossiblePrimary" => ServerType::Unknown,
        _ => return None,
    };

    Some(t)
}

async fn run_test(test_file: TestFile) {
    let test_description = &test_file.description;

    // TODO: RUST-1081 unskip tests
    let skip_keywords = doc! {
        "election Id": "(RUST-1081)",
        "electionId": "(RUST-1081)",
        "ElectionId": "(RUST-1081)"
    };

    for (keyword, skip_reason) in skip_keywords {
        if test_description.contains(keyword.as_str()) {
            log_uncaptured(format!(
                "Skipping {}: {}",
                test_description,
                skip_reason.as_str().unwrap()
            ));
            return;
        }
    }

    log_uncaptured(format!("Executing {}", test_description));

    let mut options = ClientOptions::parse_uri(&test_file.uri, None)
        .await
        .expect(test_description);

    let handler = EventBuffer::new();
    options.sdam_event_handler = Some(handler.handler());
    options.test_options_mut().disable_monitoring_threads = true;

    let mut event_subscriber = handler.subscribe();
    let mut topology = Topology::new(options.clone()).unwrap();

    for (i, phase) in test_file.phases.into_iter().enumerate() {
        for Response(address, command_response) in phase.responses {
            let address = ServerAddress::parse(&address).unwrap_or_else(|_| {
                panic!(
                    "{}: couldn't parse address \"{:?}\"",
                    test_description.as_str(),
                    address
                )
            });

            let hello_reply = if command_response.ok != Some(1.0) {
                Err(Error::from(ErrorKind::Command(CommandError {
                    code: 1234,
                    code_name: "dummy error".to_string(),
                    message: "dummy".to_string(),
                    topology_version: None,
                })))
            } else if command_response == Default::default() {
                Err(Error::from(ErrorKind::Io(Arc::new(
                    std::io::ErrorKind::BrokenPipe.into(),
                ))))
            } else {
                Ok(HelloReply {
                    server_address: address.clone(),
                    command_response: command_response.into(),
                    cluster_time: None,
                    raw_command_response: Default::default(), // doesn't matter for tests
                })
            };

            match hello_reply {
                Ok(reply) => {
                    let new_sd = ServerDescription::new_from_hello_reply(
                        address.clone(),
                        reply,
                        Duration::from_secs(1),
                    );
                    topology.clone_updater().update(new_sd).await;
                }
                Err(e) => {
                    topology
                        .clone_updater()
                        .handle_monitor_error(address, e)
                        .await;
                }
            }
        }

        for application_error in phase.application_errors {
            if let Some(server) = topology.servers().get(&application_error.address) {
                let error = application_error.to_error();
                let pool_generation = application_error
                    .generation
                    .map(PoolGeneration::Normal)
                    .unwrap_or_else(|| server.pool.generation());
                let conn_generation = application_error
                    .generation
                    .or_else(|| server.pool.generation().as_normal())
                    .unwrap_or(0);
                let conn_generation = ConnectionGeneration::Normal(conn_generation);
                let handshake_phase = match application_error.when {
                    ErrorHandshakePhase::BeforeHandshakeCompletes => HandshakePhase::PreHello {
                        generation: pool_generation,
                    },
                    ErrorHandshakePhase::AfterHandshakeCompletes => {
                        HandshakePhase::AfterCompletion {
                            generation: conn_generation,
                            max_wire_version: application_error.max_wire_version,
                        }
                    }
                };

                topology
                    .handle_application_error(server.address.clone(), error, handshake_phase)
                    .await;
            }
        }

        topology.watch().wait_until_initialized().await;
        let topology_description = topology.description();
        let servers = topology.servers();
        let phase_description = phase.description.unwrap_or_else(|| format!("{}", i));

        match phase.outcome {
            Outcome::Description(outcome) => {
                verify_description_outcome(
                    outcome,
                    topology_description,
                    servers,
                    test_description,
                    phase_description,
                );
            }
            Outcome::Events(EventsOutcome { events: expected }) => {
                let actual = event_subscriber
                    .collect_events(Duration::from_millis(500), |e| matches!(e, Event::Sdam(_)))
                    .await
                    .into_iter()
                    .map(|e| e.unwrap_sdam_event());

                assert_eq!(
                    actual.len(),
                    expected.len(),
                    "{}: {}: event list length mismatch:\n actual: {:#?}, expected: {:#?}",
                    test_description,
                    phase_description,
                    actual,
                    expected
                );
                for (actual, expected) in actual.zip(expected.into_iter()) {
                    assert_eq!(
                        actual, expected,
                        "{}: {}: SDAM events do not match:\n actual: {:#?}, expected: {:#?}",
                        test_description, phase_description, actual, expected
                    );
                }
            }
        }
    }
}

fn verify_description_outcome(
    outcome: DescriptionOutcome,
    topology_description: TopologyDescription,
    servers: HashMap<ServerAddress, Arc<crate::sdam::Server>>,
    test_description: &str,
    phase_description: String,
) {
    assert_eq!(
        topology_description.topology_type, outcome.topology_type,
        "{}: {}",
        test_description, phase_description
    );

    assert_eq!(
        topology_description.set_name, outcome.set_name,
        "{}: {}",
        test_description, phase_description,
    );

    let expected_timeout = outcome
        .logical_session_timeout_minutes
        .map(|mins| Duration::from_secs((mins as u64) * 60));
    assert_eq!(
        topology_description.logical_session_timeout, expected_timeout,
        "{}: {}",
        test_description, phase_description
    );

    if let Some(compatible) = outcome.compatible {
        assert_eq!(
            topology_description.compatibility_error.is_none(),
            compatible,
            "{}: {}",
            test_description,
            phase_description,
        );
    }

    assert_eq!(
        topology_description.servers.len(),
        outcome.servers.len(),
        "{}: {}",
        test_description,
        phase_description
    );

    for (address, server) in outcome.servers {
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
            test_description, phase_description, address,
        );

        assert_eq!(
            actual_server.set_name().unwrap_or(None),
            server.set_name,
            "{} (phase {})",
            test_description,
            phase_description
        );

        assert_eq!(
            actual_server.set_version().unwrap_or(None),
            server.set_version,
            "{} (phase {})",
            test_description,
            phase_description
        );

        assert_eq!(
            actual_server.election_id().unwrap_or(None),
            server.election_id,
            "{} (phase {})",
            test_description,
            phase_description
        );

        if let Some(logical_session_timeout_minutes) = server.logical_session_timeout_minutes {
            assert_eq!(
                actual_server.logical_session_timeout().unwrap(),
                Some(Duration::from_secs(
                    logical_session_timeout_minutes as u64 * 60
                )),
                "{} (phase {})",
                test_description,
                phase_description
            );
        }

        if let Some(min_wire_version) = server.min_wire_version {
            assert_eq!(
                actual_server.min_wire_version().unwrap(),
                Some(min_wire_version),
                "{} (phase {})",
                test_description,
                phase_description
            );
        }

        if let Some(max_wire_version) = server.max_wire_version {
            assert_eq!(
                actual_server.max_wire_version().unwrap(),
                Some(max_wire_version),
                "{} (phase {})",
                test_description,
                phase_description
            );
        }

        if let Some(topology_version) = server.topology_version {
            assert_eq!(
                actual_server.topology_version(),
                Some(topology_version),
                "{} (phase {})",
                test_description,
                phase_description
            )
        }

        if let Some(generation) = server.pool.map(|p| p.generation) {
            assert_eq!(
                servers
                    .get(&address)
                    .unwrap()
                    .pool
                    .generation()
                    .as_normal()
                    .unwrap(),
                generation,
                "{} (phase {})",
                test_description,
                phase_description
            );
        }
    }
}

#[tokio::test]
async fn single() {
    run_spec_test(&["server-discovery-and-monitoring", "single"], run_test).await;
}

#[tokio::test]
async fn rs() {
    run_spec_test(&["server-discovery-and-monitoring", "rs"], run_test).await;
}

#[tokio::test]
async fn sharded() {
    run_spec_test(&["server-discovery-and-monitoring", "sharded"], run_test).await;
}

#[tokio::test]
async fn errors() {
    run_spec_test(&["server-discovery-and-monitoring", "errors"], run_test).await;
}

#[tokio::test]
async fn monitoring() {
    run_spec_test(&["server-discovery-and-monitoring", "monitoring"], run_test).await;
}

#[tokio::test]
async fn load_balanced() {
    run_spec_test(
        &["server-discovery-and-monitoring", "load-balanced"],
        run_test,
    )
    .await;
}

#[tokio::test]
#[function_name::named]
async fn topology_closed_event_last() {
    let event_handler = EventBuffer::new();
    let mut subscriber = event_handler.subscribe();
    let client = EventClient::with_additional_options(
        None,
        Some(Duration::from_millis(50)),
        None,
        event_handler.clone(),
    )
    .await;

    client
        .database(function_name!())
        .collection(function_name!())
        .insert_one(doc! { "x": 1 })
        .await
        .unwrap();
    drop(client);

    subscriber
        .wait_for_event(Duration::from_millis(1000), |event| {
            matches!(event, Event::Sdam(SdamEvent::TopologyClosed(_)))
        })
        .await
        .expect("should see topology closed event");

    // no further SDAM events should be emitted after the TopologyClosedEvent
    let event = subscriber
        .wait_for_event(Duration::from_millis(1000), |event| {
            matches!(event, Event::Sdam(_))
        })
        .await;
    assert!(
        event.is_none(),
        "expected no more SDAM events, got {:#?}",
        event
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_events() {
    let mut options = get_client_options().await.clone();
    options.hosts.drain(1..);
    options.heartbeat_freq = Some(Duration::from_millis(50));
    options.app_name = "heartbeat_events".to_string().into();

    let event_handler = EventBuffer::new();
    let mut subscriber = event_handler.subscribe();

    let client = EventClient::with_additional_options(
        Some(options.clone()),
        Some(Duration::from_millis(50)),
        None,
        event_handler.clone(),
    )
    .await;

    if client.is_load_balanced() {
        log_uncaptured("skipping heartbeat_events tests due to load-balanced topology");
        return;
    }

    subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::Sdam(SdamEvent::ServerHeartbeatStarted(_)))
        })
        .await
        .expect("should see server heartbeat started event");

    subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::Sdam(SdamEvent::ServerHeartbeatSucceeded(_)))
        })
        .await
        .expect("should see server heartbeat succeeded event");

    if !client.supports_fail_command_appname_initial_handshake() {
        return;
    }

    options.app_name = None;
    options.heartbeat_freq = None;
    let fp_client = TestClient::with_options(Some(options)).await;

    let fp_options = FailCommandOptions::builder()
        .error_code(1234)
        .app_name("heartbeat_events".to_string())
        .build();
    let failpoint = FailPoint::fail_command(
        &[LEGACY_HELLO_COMMAND_NAME, "hello"],
        FailPointMode::AlwaysOn,
        fp_options,
    );
    let _fp_guard = fp_client
        .enable_failpoint(failpoint, None)
        .await
        .expect("enabling failpoint should succeed");

    subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::Sdam(SdamEvent::ServerHeartbeatFailed(_)))
        })
        .await
        .expect("should see server heartbeat failed event");
}

#[tokio::test]
#[function_name::named]
async fn direct_connection() {
    let test_client = TestClient::new().await;
    if !test_client.is_replica_set() {
        log_uncaptured("Skipping direct_connection test due to non-replica set topology");
        return;
    }

    let criteria = SelectionCriteria::ReadPreference(ReadPreference::Secondary {
        options: Default::default(),
    });
    let secondary_address = test_client
        .test_select_server(Some(&criteria))
        .await
        .expect("failed to select secondary");

    let mut secondary_options = get_client_options().await.clone();
    secondary_options.hosts = vec![secondary_address];

    let mut direct_false_options = secondary_options.clone();
    direct_false_options.direct_connection = Some(false);
    let direct_false_client =
        Client::with_options(direct_false_options).expect("client construction should succeed");
    direct_false_client
        .database(function_name!())
        .collection(function_name!())
        .insert_one(doc! {})
        .await
        .expect("write should succeed with directConnection=false on secondary");

    let mut direct_true_options = secondary_options.clone();
    direct_true_options.direct_connection = Some(true);
    let direct_true_client =
        Client::with_options(direct_true_options).expect("client construction should succeed");
    let error = direct_true_client
        .database(function_name!())
        .collection(function_name!())
        .insert_one(doc! {})
        .await
        .expect_err("write should fail with directConnection=true on secondary");
    assert!(error.is_notwritableprimary());

    let client =
        Client::with_options(secondary_options).expect("client construction should succeed");
    client
        .database(function_name!())
        .collection(function_name!())
        .insert_one(doc! {})
        .await
        .expect("write should succeed with directConnection unspecified");
}

#[tokio::test(flavor = "multi_thread")]
async fn pool_cleared_error_does_not_mark_unknown() {
    let address = ServerAddress::parse("a:1234").unwrap();
    let mut options = ClientOptions::builder()
        .hosts(vec![address.clone()])
        .build();
    options.test_options_mut().disable_monitoring_threads = true;
    let mut topology = Topology::new(options).unwrap();
    topology.watch().wait_until_initialized().await;

    // get the one server in the topology
    let server = topology.servers().into_values().next().unwrap();

    let heartbeat_response: HelloCommandResponse = bson::from_document(doc! {
        "ok": 1,
        "isWritablePrimary": true,
        "minWireVersion": 0,
        "maxWireVersion": 6,
        "maxBsonObjectSize": 16_000,
        "maxWriteBatchSize": 10_000,
        "maxMessageSizeBytes": 48_000_000,
    })
    .unwrap();

    // discover the node
    topology
        .clone_updater()
        .update(ServerDescription::new_from_hello_reply(
            address.clone(),
            HelloReply {
                server_address: address.clone(),
                command_response: heartbeat_response,
                cluster_time: None,
                raw_command_response: Default::default(),
            },
            Duration::from_secs(1),
        ))
        .await;
    assert_eq!(
        topology
            .watch()
            .server_description(&address)
            .unwrap()
            .server_type,
        ServerType::Standalone
    );

    // assert a pool cleared error would have no effect on the topology
    let error: Error = ErrorKind::ConnectionPoolCleared {
        message: "foo".to_string(),
    }
    .into();
    let phase = HandshakePhase::PreHello {
        generation: server.pool.generation(),
    };
    topology
        .handle_application_error(server.address.clone(), error, phase)
        .await;
    assert_eq!(
        topology
            .watch()
            .server_description(&address)
            .unwrap()
            .server_type,
        ServerType::Standalone
    );
}
