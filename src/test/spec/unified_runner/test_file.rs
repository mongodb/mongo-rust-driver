use std::{borrow::Cow, collections::HashMap, fmt::Write, sync::Arc, time::Duration};

use percent_encoding::NON_ALPHANUMERIC;
use regex::Regex;
use semver::{Version, VersionReq};
use serde::{Deserialize, Deserializer};
use tokio::sync::oneshot;

use super::{results_match, ExpectedEvent, ObserveEvent, Operation};

#[cfg(feature = "tracing-unstable")]
use crate::trace;
use crate::{
    bson::{doc, Bson, Deserializer as BsonDeserializer, Document},
    client::options::{ServerApi, ServerApiVersion},
    concern::{Acknowledgment, ReadConcernLevel},
    error::{ClientBulkWriteError, Error, ErrorKind},
    gridfs::options::GridFsBucketOptions,
    options::{
        ClientOptions,
        CollectionOptions,
        DatabaseOptions,
        HedgedReadOptions,
        ReadConcern,
        ReadPreference,
        SelectionCriteria,
        WriteConcern,
    },
    serde_util,
    test::{Event, Serverless, TestClient, DEFAULT_URI},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TestFile {
    pub(crate) description: String,
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub(crate) schema_version: Version,
    #[serde(default, deserialize_with = "serde_util::deserialize_nonempty_vec")]
    pub(crate) run_on_requirements: Option<Vec<RunOnRequirement>>,
    #[serde(default, deserialize_with = "serde_util::deserialize_nonempty_vec")]
    pub(crate) create_entities: Option<Vec<TestFileEntity>>,
    #[serde(default, deserialize_with = "serde_util::deserialize_nonempty_vec")]
    pub(crate) initial_data: Option<Vec<CollectionData>>,
    pub(crate) tests: Vec<TestCase>,
    // We don't need to use this field, but it needs to be included during deserialization so that
    // we can use the deny_unknown_fields tag.
    #[serde(rename = "_yamlAnchors")]
    _yaml_anchors: Option<Document>,
}

fn deserialize_schema_version<'de, D>(deserializer: D) -> std::result::Result<Version, D::Error>
where
    D: Deserializer<'de>,
{
    let mut schema_version = String::deserialize(deserializer)?;
    // If the schema version does not contain a minor or patch version, append as necessary to
    // ensure the String parses correctly into a semver::Version.
    let count = schema_version.split('.').count();
    if count == 1 {
        schema_version.push_str(".0.0");
    } else if count == 2 {
        schema_version.push_str(".0");
    }
    Version::parse(&schema_version).map_err(|e| serde::de::Error::custom(format!("{}", e)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RunOnRequirement {
    min_server_version: Option<String>,
    max_server_version: Option<String>,
    #[serde(default, deserialize_with = "serde_util::deserialize_nonempty_vec")]
    topologies: Option<Vec<Topology>>,
    server_parameters: Option<Document>,
    serverless: Option<Serverless>,
    auth: Option<bool>,
    csfle: Option<bool>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub(crate) enum Topology {
    Single,
    ReplicaSet,
    #[serde(alias = "sharded-replicaset")]
    Sharded,
    #[serde(rename = "load-balanced")]
    LoadBalanced,
}

impl RunOnRequirement {
    pub(crate) async fn can_run_on(&self, client: &TestClient) -> Result<(), String> {
        if let Some(ref min_version) = self.min_server_version {
            let req = VersionReq::parse(&format!(">= {}", &min_version)).unwrap();
            if !req.matches(&client.server_version) {
                return Err(format!(
                    "min server version {:?}, actual {:?}",
                    min_version, client.server_version
                ));
            }
        }
        if let Some(ref max_version) = self.max_server_version {
            let req = VersionReq::parse(&format!("<= {}", &max_version)).unwrap();
            if !req.matches(&client.server_version) {
                return Err(format!(
                    "max server version {:?}, actual {:?}",
                    max_version, client.server_version
                ));
            }
        }
        if let Some(ref topologies) = self.topologies {
            let client_topology = client.topology();
            if !topologies.contains(&client_topology) {
                return Err(format!(
                    "allowed topologies {:?}, actual: {:?}",
                    topologies, client_topology
                ));
            }
        }
        if let Some(ref actual_server_parameters) = self.server_parameters {
            if results_match(
                Some(&Bson::Document(client.server_parameters.clone())),
                &Bson::Document(actual_server_parameters.clone()),
                false,
                None,
            )
            .is_err()
            {
                return Err(format!(
                    "required server parameters {:?}, actual {:?}",
                    actual_server_parameters, client.server_parameters
                ));
            }
        }
        if let Some(ref serverless) = self.serverless {
            if !serverless.can_run() {
                return Err("requires serverless".to_string());
            }
        }
        if let Some(ref auth) = self.auth {
            if *auth != client.auth_enabled() {
                return Err("requires auth".to_string());
            }
        }
        if let Some(csfle) = &self.csfle {
            if *csfle && std::env::var("CSFLE_LOCAL_KEY").is_err() {
                return Err("requires csfle env".to_string());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum TestFileEntity {
    Client(Client),
    Database(Database),
    Collection(Collection),
    Session(Session),
    Bucket(Bucket),
    Thread(Thread),
    #[cfg(feature = "in-use-encryption-unstable")]
    ClientEncryption(ClientEncryption),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StoreEventsAsEntity {
    pub id: String,
    pub events: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Client {
    pub(crate) id: String,
    pub(crate) uri_options: Option<Document>,
    pub(crate) use_multiple_mongoses: Option<bool>,
    #[serde(default, deserialize_with = "serde_util::deserialize_nonempty_vec")]
    pub(crate) observe_events: Option<Vec<ObserveEvent>>,
    #[serde(default, deserialize_with = "serde_util::deserialize_nonempty_vec")]
    pub(crate) ignore_command_monitoring_events: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) observe_sensitive_commands: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_server_api_test_format")]
    pub(crate) server_api: Option<ServerApi>,
    #[serde(default, deserialize_with = "serde_util::deserialize_nonempty_vec")]
    pub(crate) store_events_as_entities: Option<Vec<StoreEventsAsEntity>>,
    #[cfg(feature = "tracing-unstable")]
    #[serde(default, deserialize_with = "deserialize_tracing_level_map")]
    pub(crate) observe_log_messages: Option<HashMap<String, tracing::Level>>,
}

impl Client {
    pub(crate) fn use_multiple_mongoses(&self) -> bool {
        self.use_multiple_mongoses.unwrap_or(true)
    }
}

pub(crate) fn deserialize_server_api_test_format<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<ServerApi>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct ApiHelper {
        version: ServerApiVersion,
        strict: Option<bool>,
        deprecation_errors: Option<bool>,
    }

    let h = ApiHelper::deserialize(deserializer)?;
    Ok(Some(ServerApi {
        version: h.version,
        strict: h.strict,
        deprecation_errors: h.deprecation_errors,
    }))
}

pub(crate) fn merge_uri_options(
    given_uri: &str,
    uri_options: Option<&Document>,
    use_multiple_hosts: bool,
) -> String {
    let given_uri = if !use_multiple_hosts && !given_uri.starts_with("mongodb+srv") {
        let hosts_regex = Regex::new(r"mongodb://([^/]*)").unwrap();
        let single_host = hosts_regex
            .captures(given_uri)
            .unwrap()
            .get(1)
            .unwrap()
            .as_str()
            .split(',')
            .next()
            .expect("expected URI to contain at least one host, but it had none");
        hosts_regex.replace(given_uri, format!("mongodb://{}", single_host))
    } else {
        Cow::Borrowed(given_uri)
    };

    let uri_options = match uri_options {
        Some(opts) => opts,
        None => return given_uri.to_string(),
    };

    let mut given_uri_parts = given_uri.split('?');

    let mut uri = String::from(given_uri_parts.next().unwrap());
    // A connection string has two slashes before the host list and one slash before the auth db
    // name. If an auth db name is not provided the latter slash might not be present, so it needs
    // to be added manually.
    if uri.chars().filter(|c| *c == '/').count() < 3 {
        uri.push('/');
    }
    uri.push('?');

    if let Some(options) = given_uri_parts.next() {
        let options = options.split('&');
        for option in options {
            let key = option.split('=').next().unwrap();
            // The provided URI options should override any existing options in the connection
            // string.
            if !uri_options.contains_key(key) {
                uri.push_str(option);
                uri.push('&');
            }
        }
    }

    for (key, value) in uri_options {
        let value = value.to_string();
        // to_string() wraps quotations around Bson strings
        let value = value.trim_start_matches('\"').trim_end_matches('\"');
        let _ = write!(
            &mut uri,
            "{}={}&",
            &key,
            percent_encoding::percent_encode(value.as_bytes(), NON_ALPHANUMERIC)
        );
    }

    // remove the trailing '&' from the URI (or '?' if no options are present)
    uri.pop();

    uri
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Database {
    pub(crate) id: String,
    pub(crate) client: String,
    pub(crate) database_name: String,
    pub(crate) database_options: Option<CollectionOrDatabaseOptions>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Collection {
    pub(crate) id: String,
    pub(crate) database: String,
    pub(crate) collection_name: String,
    pub(crate) collection_options: Option<CollectionOrDatabaseOptions>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Session {
    pub(crate) id: String,
    pub(crate) client: String,
    pub(crate) session_options: Option<crate::client::options::SessionOptions>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Bucket {
    pub(crate) id: String,
    pub(crate) database: String,
    pub(crate) bucket_options: Option<GridFsBucketOptions>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Thread {
    pub(crate) id: String,
}

#[cfg(feature = "in-use-encryption-unstable")]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ClientEncryption {
    pub(crate) id: String,
    pub(crate) client_encryption_opts: ClientEncryptionOpts,
}

#[cfg(feature = "in-use-encryption-unstable")]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ClientEncryptionOpts {
    pub(crate) key_vault_client: String,
    pub(crate) key_vault_namespace: crate::Namespace,
    pub(crate) kms_providers: Document,
}

/// Messages used for communicating with test runner "threads".
#[derive(Debug)]
pub(crate) enum ThreadMessage {
    ExecuteOperation(Arc<Operation>),
    Stop(oneshot::Sender<()>),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CollectionOrDatabaseOptions {
    pub(crate) read_concern: Option<ReadConcern>,
    #[serde(rename = "readPreference")]
    pub(crate) selection_criteria: Option<SelectionCriteria>,
    pub(crate) write_concern: Option<WriteConcern>,
}

impl CollectionOrDatabaseOptions {
    pub(crate) fn as_database_options(&self) -> DatabaseOptions {
        DatabaseOptions {
            read_concern: self.read_concern.clone(),
            selection_criteria: self.selection_criteria.clone(),
            write_concern: self.write_concern.clone(),
        }
    }

    pub(crate) fn as_collection_options(&self) -> CollectionOptions {
        CollectionOptions {
            read_concern: self.read_concern.clone(),
            selection_criteria: self.selection_criteria.clone(),
            write_concern: self.write_concern.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CollectionData {
    pub(crate) collection_name: String,
    pub(crate) database_name: String,
    pub(crate) documents: Vec<Document>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TestCase {
    pub(crate) description: String,
    #[serde(default, deserialize_with = "serde_util::deserialize_nonempty_vec")]
    pub(crate) run_on_requirements: Option<Vec<RunOnRequirement>>,
    pub(crate) skip_reason: Option<String>,
    pub(crate) operations: Vec<Operation>,
    #[serde(default, deserialize_with = "serde_util::deserialize_nonempty_vec")]
    pub(crate) expect_events: Option<Vec<ExpectedEvents>>,
    #[cfg(feature = "tracing-unstable")]
    pub(crate) expect_log_messages: Option<Vec<ExpectedMessages>>,
    #[serde(default, deserialize_with = "serde_util::deserialize_nonempty_vec")]
    pub(crate) outcome: Option<Vec<CollectionData>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ExpectedEvents {
    pub(crate) client: String,
    pub(crate) events: Vec<ExpectedEvent>,
    pub(crate) event_type: Option<ExpectedEventType>,
    pub(crate) ignore_extra_events: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum ExpectedEventType {
    Command,
    Cmap,
    // TODO RUST-1055 Remove this when connection usage is serialized.
    #[serde(skip)]
    CmapWithoutConnectionReady,
    Sdam,
}

impl ExpectedEventType {
    pub(crate) fn matches(&self, event: &Event) -> bool {
        match (self, event) {
            (ExpectedEventType::Cmap, Event::Cmap(_)) => true,
            (
                ExpectedEventType::CmapWithoutConnectionReady,
                Event::Cmap(crate::event::cmap::CmapEvent::ConnectionReady(_)),
            ) => false,
            (ExpectedEventType::CmapWithoutConnectionReady, Event::Cmap(_)) => true,
            (ExpectedEventType::Command, Event::Command(_)) => true,
            (ExpectedEventType::Sdam, Event::Sdam(_)) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum EventMatch {
    Exact,
    Prefix,
}

#[cfg(feature = "tracing-unstable")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ExpectedMessages {
    pub(crate) client: String,
    pub(crate) ignore_extra_messages: Option<bool>,
    pub(crate) ignore_messages: Option<Vec<ExpectedMessage>>,
    pub(crate) messages: Vec<ExpectedMessage>,
}

#[cfg(feature = "tracing-unstable")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ExpectedMessage {
    #[serde(default, deserialize_with = "deserialize_option_tracing_level")]
    pub(crate) level: Option<tracing::Level>,
    #[serde(
        default,
        rename = "component",
        deserialize_with = "deserialize_option_tracing_target"
    )]
    pub(crate) target: Option<String>,
    pub(crate) failure_is_redacted: Option<bool>,
    pub(crate) data: Document,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ExpectError {
    #[allow(unused)]
    pub(crate) is_error: Option<bool>,
    pub(crate) is_client_error: Option<bool>,
    pub(crate) error_contains: Option<String>,
    pub(crate) error_code: Option<i32>,
    pub(crate) error_code_name: Option<String>,
    #[serde(default, deserialize_with = "serde_util::deserialize_nonempty_vec")]
    pub(crate) error_labels_contain: Option<Vec<String>>,
    #[serde(default, deserialize_with = "serde_util::deserialize_nonempty_vec")]
    pub(crate) error_labels_omit: Option<Vec<String>>,
    pub(crate) expect_result: Option<Bson>,
    #[serde(default, deserialize_with = "serde_util::deserialize_indexed_map")]
    pub(crate) write_errors: Option<HashMap<usize, Bson>>,
    pub(crate) write_concern_errors: Option<Vec<Bson>>,
}

impl ExpectError {
    pub(crate) fn verify_result(
        &self,
        error: &Error,
        description: impl AsRef<str>,
    ) -> std::result::Result<(), String> {
        let description = description.as_ref();

        if let Some(is_client_error) = self.is_client_error {
            if is_client_error != !error.is_server_error() {
                return Err(format!(
                    "{}: expected client error but got {:?}",
                    description, error
                ));
            }
        }

        if let Some(error_contains) = &self.error_contains {
            match &error.message() {
                Some(msg) if msg.contains(error_contains) => (),
                _ => {
                    return Err(format!(
                        "{}: \"{}\" should include message field",
                        description, error
                    ))
                }
            }
        }

        if let Some(error_code) = self.error_code {
            match &error.code() {
                Some(code) => {
                    if code != &error_code {
                        return Err(format!(
                            "{}: error code {} ({:?}) did not match expected error code {}",
                            description,
                            code,
                            error.code_name(),
                            error_code
                        ));
                    }
                }
                None => {
                    return Err(format!(
                        "{}: {:?} was expected to include code {} but had no code",
                        description, error, error_code
                    ))
                }
            }
        }

        if let Some(expected_code_name) = &self.error_code_name {
            match error.code_name() {
                Some(name) => {
                    if name != expected_code_name {
                        return Err(format!(
                            "{}: error code name \"{}\" did not match expected error code name \
                             \"{}\"",
                            description, name, expected_code_name,
                        ));
                    }
                }
                None => {
                    return Err(format!(
                        "{}: {:?} was expected to include code name \"{}\" but had no code name",
                        description, error, expected_code_name
                    ))
                }
            }
        }

        if let Some(error_labels_contain) = &self.error_labels_contain {
            for label in error_labels_contain {
                if !error.contains_label(label) {
                    return Err(format!(
                        "{}: expected {:?} to contain label \"{}\"",
                        description, error, label
                    ));
                }
            }
        }

        if let Some(error_labels_omit) = &self.error_labels_omit {
            for label in error_labels_omit {
                if error.contains_label(label) {
                    return Err(format!(
                        "{}: expected {:?} to omit label \"{}\"",
                        description, error, label
                    ));
                }
            }
        }

        if let Some(ref expected_result) = self.expect_result {
            let actual_result = match *error.kind {
                ErrorKind::ClientBulkWrite(ClientBulkWriteError {
                    partial_result: Some(ref partial_result),
                    ..
                }) => Some(bson::to_bson(partial_result).map_err(|e| e.to_string())?),
                _ => None,
            };
            results_match(actual_result.as_ref(), expected_result, false, None)?;
        }

        if let Some(ref write_errors) = self.write_errors {
            let actual_write_errors = match *error.kind {
                ErrorKind::ClientBulkWrite(ref bulk_write_error) => &bulk_write_error.write_errors,
                ref other => {
                    return Err(format!(
                        "{}: expected bulk write error, got {:?}",
                        description, other
                    ))
                }
            };

            for (expected_index, expected_error) in write_errors {
                let actual_error = actual_write_errors.get(expected_index).ok_or_else(|| {
                    format!(
                        "{}: expected error for operation at index {}",
                        description, expected_index
                    )
                })?;
                let actual_error = bson::to_bson(&actual_error).map_err(|e| e.to_string())?;
                results_match(Some(&actual_error), expected_error, true, None)?;
            }
        }

        if let Some(ref write_concern_errors) = self.write_concern_errors {
            let actual_write_concern_errors = match *error.kind {
                ErrorKind::ClientBulkWrite(ref bulk_write_error) => {
                    &bulk_write_error.write_concern_errors
                }
                ref other => {
                    return Err(format!(
                        "{}: expected bulk write error, got {:?}",
                        description, other
                    ))
                }
            };

            if actual_write_concern_errors.len() != write_concern_errors.len() {
                return Err(format!(
                    "{}: got {} write errors, expected {}",
                    description,
                    actual_write_concern_errors.len(),
                    write_concern_errors.len()
                ));
            }

            for (actual, expected) in actual_write_concern_errors.iter().zip(write_concern_errors) {
                let actual = bson::to_bson(&actual).map_err(|e| e.to_string())?;
                results_match(Some(&actual), expected, true, None)?;
            }
        }

        Ok(())
    }
}

#[tokio::test]
async fn merged_uri_options() {
    let options = doc! {
        "ssl": true,
        "w": 2,
        "readconcernlevel": "local",
    };
    let uri = merge_uri_options(&DEFAULT_URI, Some(&options), true);
    let options = ClientOptions::parse(&uri).await.unwrap();

    assert!(options.tls_options().is_some());

    let write_concern = WriteConcern::builder().w(Acknowledgment::from(2)).build();
    assert_eq!(options.write_concern.unwrap(), write_concern);

    let read_concern = ReadConcern::local();
    assert_eq!(options.read_concern.unwrap(), read_concern);
}

#[test]
fn deserialize_selection_criteria() {
    let read_preference = doc! {
        "mode": "SecondaryPreferred",
        "maxStalenessSeconds": 100,
        "hedge": { "enabled": true },
    };
    let d = BsonDeserializer::new(read_preference.into());
    let selection_criteria = SelectionCriteria::deserialize(d).unwrap();

    match selection_criteria {
        SelectionCriteria::ReadPreference(read_preference) => match read_preference {
            ReadPreference::SecondaryPreferred {
                options: Some(options),
            } => {
                assert_eq!(options.max_staleness, Some(Duration::from_secs(100)));
                assert_eq!(
                    options.hedge,
                    Some(HedgedReadOptions::builder().enabled(true).build())
                );
            }
            other => panic!(
                "Expected mode SecondaryPreferred with options, got {:?}",
                other
            ),
        },
        SelectionCriteria::Predicate(_) => panic!("Expected read preference, got predicate"),
    }
}

#[test]
fn deserialize_read_concern() {
    let read_concern = doc! {
        "level": "local",
    };
    let d = BsonDeserializer::new(read_concern.into());
    let read_concern = ReadConcern::deserialize(d).unwrap();
    assert!(matches!(read_concern.level, ReadConcernLevel::Local));

    let read_concern = doc! {
        "level": "customlevel",
    };
    let d = BsonDeserializer::new(read_concern.into());
    let read_concern = ReadConcern::deserialize(d).unwrap();
    match read_concern.level {
        ReadConcernLevel::Custom(level) => assert_eq!(level.as_str(), "customlevel"),
        other => panic!("Expected custom read concern, got {:?}", other),
    };
}

#[cfg(feature = "tracing-unstable")]
fn deserialize_tracing_level_map<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<HashMap<String, tracing::Level>>, D::Error>
where
    D: Deserializer<'de>,
{
    let str_map = HashMap::<String, String>::deserialize(deserializer)?;
    let mut level_map = HashMap::new();
    for (key, val) in str_map.iter() {
        let key = log_component_as_tracing_target(key);
        let level = val
            .parse::<tracing::Level>()
            .map_err(|e| serde::de::Error::custom(format!("{}", e)))?;
        level_map.insert(key, level);
    }
    Ok(Some(level_map))
}

#[cfg(feature = "tracing-unstable")]
fn deserialize_option_tracing_target<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer).map(|s| Some(log_component_as_tracing_target(&s)))
}

#[cfg(feature = "tracing-unstable")]
fn log_component_as_tracing_target(component: &String) -> String {
    match component.as_ref() {
        "command" => trace::COMMAND_TRACING_EVENT_TARGET.to_string(),
        "connection" => trace::CONNECTION_TRACING_EVENT_TARGET.to_string(),
        "serverSelection" => trace::SERVER_SELECTION_TRACING_EVENT_TARGET.to_string(),
        "topology" => trace::TOPOLOGY_TRACING_EVENT_TARGET.to_string(),
        _ => panic!("Unknown tracing target: {}", component),
    }
}

#[cfg(feature = "tracing-unstable")]
fn deserialize_option_tracing_level<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<tracing::Level>, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer)?
        .parse::<tracing::Level>()
        .map(Some)
        .map_err(|e| serde::de::Error::custom(format!("{}", e)))
}
