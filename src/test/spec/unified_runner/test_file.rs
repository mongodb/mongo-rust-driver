#![allow(dead_code)]

use semver::{Version, VersionReq};
use serde::{Deserialize, Deserializer};

use super::TestEvent;

use crate::{
    bson::{Bson, Document},
    error::Error,
    options::{
        ClientOptions,
        CollectionOptions,
        DatabaseOptions,
        ReadConcern,
        ReadPreference,
        SelectionCriteria,
        WriteConcern,
    },
    test::TestClient,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestFile {
    pub description: String,
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: Version,
    pub run_on_requirements: Option<Vec<RunOnRequirement>>,
    pub allow_multiple_mongoses: Option<bool>,
    pub create_entities: Option<Vec<Entity>>,
    pub initial_data: Option<Vec<CollectionData>>,
    pub tests: Vec<TestCase>,
}

fn deserialize_schema_version<'de, D>(deserializer: D) -> std::result::Result<Version, D::Error>
where
    D: Deserializer<'de>,
{
    let mut schema_version = String::deserialize(deserializer)?;
    // if the schema version only contains a major and minor version (e.g. 1.0), append a ".0" to
    // ensure correct parsing into a semver::Version
    if schema_version.split('.').count() == 2 {
        schema_version.push_str(".0");
    }
    Version::parse(&schema_version).map_err(|e| serde::de::Error::custom(format!("{}", e)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunOnRequirement {
    min_server_version: Option<String>,
    max_server_version: Option<String>,
    topology: Option<Vec<Topology>>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
#[serde(untagged)]
pub enum Topology {
    Single,
    ReplicaSet,
    Sharded,
    #[serde(rename = "sharded-replicaset")]
    ShardedReplicaSet,
}

impl RunOnRequirement {
    pub fn can_run_on(&self, client: &TestClient) -> bool {
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
        if let Some(ref topology) = self.topology {
            if !topology.contains(&client.topology()) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Entity {
    Client(Client),
    Database(Database),
    Collection(Collection),
    Session(Session),
    Bucket(Bucket),
    Stream(Stream),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Client {
    pub id: String,
    pub uri_options: Option<ClientOptions>,
    pub use_multiple_mongoses: Option<bool>,
    pub observe_events: Option<Vec<String>>,
    pub ignore_command_monitoring_events: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Database {
    pub id: String,
    pub client: String,
    pub database_name: String,
    pub database_options: Option<CollectionOrDatabaseOptions>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Collection {
    pub id: String,
    pub database: String,
    pub collection_name: String,
    pub collection_options: Option<CollectionOrDatabaseOptions>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Session {
    pub id: String,
    pub client: String,
    pub session_options: Option<Document>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bucket {
    pub id: String,
    pub database: String,
    pub bucket_options: Option<Document>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stream {
    pub id: String,
    pub hex_bytes: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionOrDatabaseOptions {
    // TODO properly implement Deserialize for ReadConcern
    pub read_concern: Option<ReadConcern>,
    pub read_preference: Option<ReadPreference>,
    pub write_concern: Option<WriteConcern>,
}

impl CollectionOrDatabaseOptions {
    pub fn as_database_options(&self) -> DatabaseOptions {
        let mut database_options = DatabaseOptions::builder().build();
        database_options.read_concern = self.read_concern.clone();
        if let Some(ref read_preference) = self.read_preference {
            let selection_criteria = SelectionCriteria::ReadPreference(read_preference.clone());
            database_options.selection_criteria = Some(selection_criteria);
        }
        database_options.write_concern = self.write_concern.clone();
        database_options
    }

    pub fn as_collection_options(&self) -> CollectionOptions {
        let mut collection_options = CollectionOptions::builder().build();
        collection_options.read_concern = self.read_concern.clone();
        if let Some(ref read_preference) = self.read_preference {
            let selection_criteria = SelectionCriteria::ReadPreference(read_preference.clone());
            collection_options.selection_criteria = Some(selection_criteria);
        }
        collection_options.write_concern = self.write_concern.clone();
        collection_options
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionData {
    pub collection_name: String,
    pub database_name: String,
    pub documents: Vec<Document>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestCase {
    pub description: String,
    pub run_on_requirements: Option<Vec<RunOnRequirement>>,
    pub skip_reason: Option<String>,
    pub operations: Vec<Operation>,
    pub expect_events: Option<Vec<ExpectedEvents>>,
    pub outcome: Option<Vec<CollectionData>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    pub name: String,
    pub object: String,
    pub arguments: Option<Document>,
    pub expect_error: Option<ExpectError>,
    pub expect_result: Option<Bson>,
    pub save_result_as_entity: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ExpectedEvents {
    pub client: String,
    pub events: Vec<TestEvent>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpectError {
    pub is_error: Option<bool>,
    pub is_client_error: Option<bool>,
    pub error_contains: Option<String>,
    pub error_code: Option<i32>,
    pub error_labels_contain: Option<Vec<String>>,
    pub error_labels_omit: Option<Vec<String>>,
    pub expected_result: Option<Bson>,
}

impl ExpectError {
    pub fn verify_result(self, error: Error) {
        if let Some(is_client_error) = self.is_client_error {
            assert_eq!(is_client_error, !error.is_server_error());
        }
        if let Some(error_contains) = self.error_contains {
            match &error.kind.code_and_message() {
                Some((_, msg)) => assert!(msg.contains(&error_contains)),
                None => panic!("error should include message field"),
            }
        }
        if let Some(error_code) = self.error_code {
            match &error.kind.code_and_message() {
                Some((code, _)) => assert_eq!(*code, error_code),
                None => panic!("error should include code"),
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
        if self.expected_result.is_some() {
            // TODO RUST-260: match against partial results
        }
    }
}
