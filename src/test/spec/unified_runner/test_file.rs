use std::{collections::HashMap, sync::Arc, time::Duration};

use pretty_assertions::assert_eq;
use regex::Regex;
use semver::Version;
use serde::{Deserialize, Deserializer};
use tokio::sync::oneshot;

use super::{results_match, ExpectedEvent, ObserveEvent, Operation};

#[cfg(feature = "bson-3")]
use crate::bson_compat::RawDocumentBufExt;
#[cfg(feature = "tracing-unstable")]
use crate::trace;
use crate::{
    bson::{doc, Bson, Deserializer as BsonDeserializer, Document},
    client::options::{ServerApi, ServerApiVersion},
    concern::{Acknowledgment, ReadConcernLevel},
    error::{BulkWriteError, Error, ErrorKind},
    gridfs::options::GridFsBucketOptions,
    options::{
        AuthMechanism,
        ClientOptions,
        CollectionOptions,
        CreateCollectionOptions,
        DatabaseOptions,
        HedgedReadOptions,
        ReadConcern,
        ReadPreference,
        SelectionCriteria,
        WriteConcern,
    },
    serde_util,
    test::{
        auth_enabled,
        get_client_options,
        get_server_parameters,
        get_topology,
        server_version_matches,
        Event,
        Serverless,
        DEFAULT_URI,
    },
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
    Version::parse(&schema_version).map_err(|e| serde::de::Error::custom(format!("{e}")))
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
    csfle: Option<Csfle>,
    auth_mechanism: Option<AuthMechanism>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Csfle {
    Bool(bool),
    #[serde(rename_all = "camelCase")]
    Version {
        #[cfg(feature = "in-use-encryption")]
        min_libmongocrypt_version: String,
    },
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
    pub(crate) async fn can_run_on(&self) -> Result<(), String> {
        if let Some(ref min_version) = self.min_server_version {
            if !server_version_matches(&format!(">= {min_version}")).await {
                return Err(format!("does not match min server version: {min_version}"));
            }
        }
        if let Some(ref max_version) = self.max_server_version {
            if !server_version_matches(&format!("<= {max_version}")).await {
                return Err(format!("does not match max server version: {max_version}"));
            }
        }
        if let Some(ref topologies) = self.topologies {
            let client_topology = get_topology().await;
            if !topologies.contains(client_topology) {
                return Err(format!(
                    "allowed topologies {topologies:?}, actual: {client_topology:?}"
                ));
            }
        }
        if let Some(ref required_server_parameters) = self.server_parameters {
            let actual_server_parameters = get_server_parameters().await;
            if results_match(
                Some(&Bson::Document(actual_server_parameters.clone())),
                &Bson::Document(required_server_parameters.clone()),
                false,
                None,
            )
            .is_err()
            {
                return Err(format!(
                    "required server parameters {required_server_parameters:?}, actual \
                     {actual_server_parameters:?}"
                ));
            }
        }
        if let Some(ref serverless) = self.serverless {
            if !serverless.can_run() {
                return Err("requires serverless".to_string());
            }
        }
        if let Some(ref auth) = self.auth {
            if *auth != auth_enabled().await {
                return Err("requires auth".to_string());
            }
        }
        if let Some(ref csfle) = self.csfle {
            match csfle {
                Csfle::Bool(true) | Csfle::Version { .. }
                    if cfg!(not(feature = "in-use-encryption")) =>
                {
                    return Err(
                        "requires csfle but in-use-encryption feature not enabled".to_string()
                    );
                }
                #[cfg(feature = "in-use-encryption")]
                Csfle::Version {
                    min_libmongocrypt_version,
                } => {
                    if crate::test::mongocrypt_version_lt(min_libmongocrypt_version) {
                        return Err(format!(
                            "requires at least libmongocrypt version {min_libmongocrypt_version} \
                             but using version {},",
                            mongocrypt::version()
                        ));
                    }
                }
                _ => {}
            }
        }
        if let Some(ref auth_mechanism) = self.auth_mechanism {
            let actual_mechanism = get_client_options()
                .await
                .credential
                .as_ref()
                .and_then(|c| c.mechanism.as_ref());
            if !actual_mechanism.is_some_and(|actual_mechanism| actual_mechanism == auth_mechanism)
            {
                return Err(format!("requires {auth_mechanism:?} auth mechanism"));
            }
        }
        Ok(())
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum TestFileEntity {
    Client(Client),
    Database(Database),
    Collection(Collection),
    Session(Session),
    Bucket(Bucket),
    Thread(Thread),
    #[cfg(feature = "in-use-encryption")]
    ClientEncryption(ClientEncryption),
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
    #[cfg(feature = "tracing-unstable")]
    #[serde(default, deserialize_with = "deserialize_tracing_level_map")]
    pub(crate) observe_log_messages: Option<HashMap<String, tracing::Level>>,
    #[cfg(feature = "in-use-encryption")]
    pub(crate) auto_encrypt_opts: Option<AutoEncryptionOpts>,
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
    let direct_connection = uri_options
        .and_then(|options| options.get_bool("directConnection").ok())
        .unwrap_or(false);

    // TODO RUST-1308: use the ConnectionString type to remove hosts
    let uri = if (!use_multiple_hosts || direct_connection) && !given_uri.starts_with("mongodb+srv")
    {
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
        hosts_regex
            .replace(given_uri, format!("mongodb://{single_host}"))
            .to_string()
    } else {
        given_uri.to_string()
    };

    let Some(mut uri_options) = uri_options.cloned() else {
        return uri;
    };

    let (mut uri, existing_options) = match uri.split_once("?") {
        Some((pre_options, options)) => (pre_options.to_string(), Some(options)),
        None => (uri, None),
    };

    if let Some(existing_options) = existing_options {
        for option in existing_options.split("&") {
            let (key, value) = option.split_once("=").unwrap();
            // prefer the option specified by the test
            if !uri_options.contains_key(key) {
                uri_options.insert(key, value);
            }
        }
    }

    if direct_connection {
        uri_options.remove("replicaSet");
    }

    let mut join = '?';
    for (key, value) in uri_options {
        uri.push(join);
        if join == '?' {
            join = '&';
        }
        uri.push_str(&key);
        uri.push('=');

        let value = value.to_string();
        let value = value.trim_start_matches('\"').trim_end_matches('\"');
        uri.push_str(value);
    }

    uri
}

#[cfg(feature = "in-use-encryption")]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AutoEncryptionOpts {
    pub(crate) kms_providers: HashMap<mongocrypt::ctx::KmsProvider, Document>,
    pub(crate) key_vault_namespace: crate::Namespace,
    pub(crate) bypass_auto_encryption: Option<bool>,
    pub(crate) schema_map: Option<HashMap<String, Document>>,
    pub(crate) encrypted_fields_map: Option<HashMap<String, Document>>,
    pub(crate) extra_options: Option<Document>,
    pub(crate) bypass_query_analysis: Option<bool>,
    #[serde(
        default,
        rename = "keyExpirationMS",
        deserialize_with = "serde_util::deserialize_duration_option_from_u64_millis"
    )]
    pub(crate) key_cache_expiration: Option<Duration>,
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

#[cfg(feature = "in-use-encryption")]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ClientEncryption {
    pub(crate) id: String,
    pub(crate) client_encryption_opts: ClientEncryptionOpts,
}

#[cfg(feature = "in-use-encryption")]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ClientEncryptionOpts {
    pub(crate) key_vault_client: String,
    pub(crate) key_vault_namespace: crate::Namespace,
    pub(crate) kms_providers: HashMap<mongocrypt::ctx::KmsProvider, Document>,
    #[serde(
        default,
        rename = "keyExpirationMS",
        deserialize_with = "serde_util::deserialize_duration_option_from_u64_millis"
    )]
    pub(crate) key_cache_expiration: Option<Duration>,
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
    pub(crate) create_options: Option<CreateCollectionOptions>,
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
    pub(crate) error_response: Option<Document>,
}

impl ExpectError {
    pub(crate) fn verify_result(&self, error: &Error, description: impl AsRef<str>) {
        let context = format!(
            "test description: {}\nerror: {:?}\n",
            description.as_ref(),
            error
        );

        if let Some(is_client_error) = self.is_client_error {
            assert_eq!(!error.is_server_error(), is_client_error, "{context}");
        }

        if let Some(error_contains) = &self.error_contains {
            let Some(message) = error.message() else {
                panic!("{context}expected error to have message");
            };
            assert!(message.contains(error_contains), "{context}");
        }

        if let Some(error_code) = self.error_code {
            let Some(actual_code) = error.code() else {
                panic!("{context}expected error to have code");
            };
            assert_eq!(actual_code, error_code, "{context}");
        }

        if let Some(expected_code_name) = &self.error_code_name {
            let Some(actual_code_name) = error.code_name() else {
                panic!("{context}: expected error to have code name");
            };
            assert_eq!(actual_code_name, expected_code_name, "{}", context);
        }

        if let Some(error_labels_contain) = &self.error_labels_contain {
            for label in error_labels_contain {
                assert!(error.contains_label(label), "{}", context);
            }
        }

        if let Some(error_labels_omit) = &self.error_labels_omit {
            for label in error_labels_omit {
                assert!(!error.contains_label(label), "{}", context);
            }
        }

        if let Some(ref expected_result) = self.expect_result {
            match *error.kind {
                ErrorKind::BulkWrite(BulkWriteError {
                    ref partial_result, ..
                }) => {
                    let actual_result = partial_result.as_ref().map(|result| {
                        crate::bson_compat::serialize_to_bson(result).expect(&context)
                    });
                    results_match(actual_result.as_ref(), expected_result, false, None)
                        .expect(&context);
                }
                // Skip this assertion for insert_many tests, as InsertManyError does not report
                // partial results.
                ErrorKind::InsertMany(_) => {}
                _ => panic!("{context}expected error with partial result"),
            }
        }

        if let Some(ref write_errors) = self.write_errors {
            let ErrorKind::BulkWrite(BulkWriteError {
                write_errors: ref actual_write_errors,
                ..
            }) = *error.kind
            else {
                panic!("{context}expected client bulk write error");
            };

            for (expected_index, expected_error) in write_errors {
                let actual_error = actual_write_errors.get(expected_index).expect(&context);
                let actual_error = crate::bson_compat::serialize_to_bson(&actual_error)
                    .map_err(|e| e.to_string())
                    .expect(&context);
                results_match(Some(&actual_error), expected_error, true, None).expect(&context);
            }
        }

        if let Some(ref write_concern_errors) = self.write_concern_errors {
            let ErrorKind::BulkWrite(BulkWriteError {
                write_concern_errors: ref actual_write_concern_errors,
                ..
            }) = *error.kind
            else {
                panic!("{context}expected client bulk write error");
            };

            assert_eq!(
                actual_write_concern_errors.len(),
                write_concern_errors.len(),
                "{context}"
            );

            for (actual, expected) in actual_write_concern_errors.iter().zip(write_concern_errors) {
                let actual = crate::bson_compat::serialize_to_bson(&actual)
                    .map_err(|e| e.to_string())
                    .expect(&context);
                results_match(Some(&actual), expected, true, None).expect(&context);
            }
        }

        if let Some(ref expected) = self.error_response {
            let actual_raw = error.server_response().expect(&context);
            let actual = actual_raw.to_document().expect(&context);
            results_match(Some(&actual.into()), &expected.into(), true, None).expect(&context);
        }
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
                #[allow(deprecated)]
                let hedge = options.hedge;
                assert_eq!(
                    hedge,
                    Some(HedgedReadOptions::builder().enabled(true).build())
                );
            }
            other => panic!("Expected mode SecondaryPreferred with options, got {other:?}"),
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
        other => panic!("Expected custom read concern, got {other:?}"),
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
            .map_err(|e| serde::de::Error::custom(format!("{e}")))?;
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
        _ => panic!("Unknown tracing target: {component}"),
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
        .map_err(|e| serde::de::Error::custom(format!("{e}")))
}
