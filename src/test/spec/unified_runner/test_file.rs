use std::time::Duration;

use semver::{Version, VersionReq};
use serde::{Deserialize, Deserializer};

use super::{Operation, TestEvent};

use crate::{
    bson::{doc, Bson, Deserializer as BsonDeserializer, Document},
    client::options::ServerApi,
    concern::{Acknowledgment, ReadConcernLevel},
    error::Error,
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
    test::{spec::unified_runner::results_match, TestClient, DEFAULT_URI},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestFile {
    pub description: String,
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: Version,
    pub run_on_requirements: Option<Vec<RunOnRequirement>>,
    pub allow_multiple_mongoses: Option<bool>,
    pub create_entities: Option<Vec<TestFileEntity>>,
    pub initial_data: Option<Vec<CollectionData>>,
    pub tests: Vec<TestCase>,
    #[serde(rename = "_yamlAnchors")]
    yaml_anchors: Option<Document>,
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
pub struct RunOnRequirement {
    min_server_version: Option<String>,
    max_server_version: Option<String>,
    topologies: Option<Vec<Topology>>,
    server_parameters: Option<Document>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub enum Topology {
    Single,
    ReplicaSet,
    Sharded,
    #[serde(rename = "sharded-replicaset")]
    ShardedReplicaSet,
}

impl RunOnRequirement {
    pub async fn can_run_on(&self, client: &TestClient) -> bool {
        if let Some(ref min_version) = self.min_server_version {
            let req = VersionReq::parse(&format!(">= {}", &min_version)).unwrap();
            if !req.matches(&client.server_version) {
                return false;
            }
        }
        if let Some(ref max_version) = self.max_server_version {
            let req = VersionReq::parse(&format!("<= {}", &max_version)).unwrap();
            if !req.matches(&client.server_version) {
                return false;
            }
        }
        if let Some(ref topologies) = self.topologies {
            if !topologies.contains(&client.topology().await) {
                return false;
            }
        }
        if let Some(ref actual_server_parameters) = self.server_parameters {
            if !results_match(
                Some(&Bson::Document(client.server_parameters.clone())),
                &Bson::Document(actual_server_parameters.clone()),
                false,
                None,
            ) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum TestFileEntity {
    Client(Client),
    Database(Database),
    Collection(Collection),
    Session(Session),
    Bucket(Bucket),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Client {
    pub id: String,
    #[serde(
        default = "default_uri",
        deserialize_with = "deserialize_uri_options_to_uri_string",
        rename = "uriOptions"
    )]
    pub uri: String,
    pub use_multiple_mongoses: Option<bool>,
    pub observe_events: Option<Vec<String>>,
    pub ignore_command_monitoring_events: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) server_api: Option<ServerApi>,
}

fn default_uri() -> String {
    DEFAULT_URI.clone()
}

fn deserialize_uri_options_to_uri_string<'de, D>(
    deserializer: D,
) -> std::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let uri_options = Document::deserialize(deserializer)?;

    let mut default_uri_parts = DEFAULT_URI.split('?');

    let mut uri = String::from(default_uri_parts.next().unwrap());
    // A connection string has two slashes before the host list and one slash before the auth db
    // name. If an auth db name is not provided the latter slash might not be present, so it needs
    // to be added manually.
    if uri.chars().filter(|c| *c == '/').count() < 3 {
        uri.push('/');
    }
    uri.push('?');

    if let Some(options) = default_uri_parts.next() {
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
        uri.push_str(&format!("{}={}&", &key, value));
    }

    // remove the trailing '&' from the URI (or '?' if no options are present)
    uri.pop();

    Ok(uri)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Database {
    pub id: String,
    pub client: String,
    pub database_name: String,
    pub database_options: Option<CollectionOrDatabaseOptions>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Collection {
    pub id: String,
    pub database: String,
    pub collection_name: String,
    pub collection_options: Option<CollectionOrDatabaseOptions>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Session {
    pub id: String,
    pub client: String,
    pub session_options: Option<Document>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Bucket {
    pub id: String,
    pub database: String,
    pub bucket_options: Option<Document>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Stream {
    pub id: String,
    pub hex_bytes: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CollectionOrDatabaseOptions {
    pub read_concern: Option<ReadConcern>,
    #[serde(rename = "readPreference")]
    pub selection_criteria: Option<SelectionCriteria>,
    pub write_concern: Option<WriteConcern>,
}

impl CollectionOrDatabaseOptions {
    pub fn as_database_options(&self) -> DatabaseOptions {
        DatabaseOptions {
            read_concern: self.read_concern.clone(),
            selection_criteria: self.selection_criteria.clone(),
            write_concern: self.write_concern.clone(),
        }
    }

    pub fn as_collection_options(&self) -> CollectionOptions {
        CollectionOptions {
            read_concern: self.read_concern.clone(),
            selection_criteria: self.selection_criteria.clone(),
            write_concern: self.write_concern.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CollectionData {
    pub collection_name: String,
    pub database_name: String,
    pub documents: Vec<Document>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestCase {
    pub description: String,
    pub run_on_requirements: Option<Vec<RunOnRequirement>>,
    pub skip_reason: Option<String>,
    pub operations: Vec<Operation>,
    pub expect_events: Option<Vec<ExpectedEvents>>,
    pub outcome: Option<Vec<CollectionData>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExpectedEvents {
    pub client: String,
    pub events: Vec<TestEvent>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExpectError {
    pub is_error: Option<bool>,
    pub is_client_error: Option<bool>,
    pub error_contains: Option<String>,
    pub error_code: Option<i32>,
    pub error_code_name: Option<String>,
    pub error_labels_contain: Option<Vec<String>>,
    pub error_labels_omit: Option<Vec<String>>,
    pub expect_result: Option<Bson>,
}

impl ExpectError {
    pub fn verify_result(self, error: Error) {
        if let Some(is_client_error) = self.is_client_error {
            assert_eq!(is_client_error, !error.is_server_error());
        }
        if let Some(error_contains) = self.error_contains {
            match &error.kind.server_message() {
                Some(msg) => assert!(msg.contains(&error_contains)),
                None => panic!("{} should include message field", error),
            }
        }
        if let Some(error_code) = self.error_code {
            match &error.kind.code() {
                Some(code) => assert_eq!(*code, error_code),
                None => panic!("{} should include code", error),
            }
        }
        if let Some(error_code_name) = self.error_code_name {
            match &error.kind.code_name() {
                Some(name) => assert_eq!(&error_code_name, name),
                None => panic!("{} should include code name", error),
            }
        }
        if let Some(error_labels_contain) = self.error_labels_contain {
            for label in error_labels_contain {
                assert!(error.labels().contains(&label));
            }
        }
        if let Some(error_labels_omit) = self.error_labels_omit {
            for label in error_labels_omit {
                assert!(!error.labels().contains(&label));
            }
        }
        if self.expect_result.is_some() {
            // TODO RUST-260: match against partial results
        }
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn deserialize_uri_options() {
    let options = doc! {
        "ssl": true,
        "w": 2,
        "readconcernlevel": "local",
    };
    let d = BsonDeserializer::new(options.into());
    let uri = deserialize_uri_options_to_uri_string(d).unwrap();
    let options = ClientOptions::parse_uri(&uri, None).await.unwrap();

    assert!(options.tls_options().is_some());

    let write_concern = WriteConcern::builder().w(Acknowledgment::from(2)).build();
    assert_eq!(options.write_concern.unwrap(), write_concern);

    let read_concern = ReadConcern::local();
    assert_eq!(options.read_concern.unwrap(), read_concern);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn deserialize_selection_criteria() {
    let read_preference = doc! {
        "mode": "SecondaryPreferred",
        "maxStalenessSeconds": 100,
        "hedge": { "enabled": true },
    };
    let d = BsonDeserializer::new(read_preference.into());
    let selection_criteria = SelectionCriteria::deserialize(d).unwrap();

    match selection_criteria {
        SelectionCriteria::ReadPreference(read_preference) => match read_preference {
            ReadPreference::SecondaryPreferred { options } => {
                assert_eq!(options.max_staleness, Some(Duration::from_secs(100)));
                assert_eq!(options.hedge, Some(HedgedReadOptions::with_enabled(true)));
            }
            other => panic!("Expected mode SecondaryPreferred, got {:?}", other),
        },
        SelectionCriteria::Predicate(_) => panic!("Expected read preference, got predicate"),
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn deserialize_read_concern() {
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
