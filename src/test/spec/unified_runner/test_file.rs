#![allow(dead_code)]

use semver::{Version, VersionReq};
use serde::{Deserialize, Deserializer};

use crate::{
    bson::{Bson, Document},
    options::{
        ClientOptions,
        CollectionOptions,
        DatabaseOptions,
        ReadConcern,
        ReadPreference,
        SelectionCriteria,
        WriteConcern,
    },
    test::{TestClient, TestEvent},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestFile {
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: Version,
    pub run_on: Option<Vec<RunOn>>,
    pub allow_multiple_mongoses: Option<bool>,
    pub create_entities: Option<Vec<Entity>>,
    pub collection_name: String,
    pub database_name: String,
    pub initial_data: Option<CollectionData>,
    pub tests: Vec<TestCase>,
}

fn deserialize_schema_version<'de, D>(deserializer: D) -> std::result::Result<Version, D::Error>
where
    D: Deserializer<'de>,
{
    let schema_version = String::deserialize(deserializer)?;
    Version::parse(&schema_version).map_err(|e| serde::de::Error::custom(format!("{}", e)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunOn {
    pub min_server_version: Option<String>,
    pub max_server_version: Option<String>,
    pub topology: Option<Vec<String>>,
}

impl RunOn {
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
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Client {
    pub id: String,
    pub uri_options: Option<ClientOptions>,
    pub use_multiple_mongoses: Option<bool>,
    pub observe_events: Option<Vec<Document>>,
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
    pub create_on_server: Option<bool>,
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
pub struct CollectionOrDatabaseOptions {
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
    pub run_on: Option<Vec<RunOn>>,
    pub skip_reason: Option<String>,
    pub operations: Vec<Operation>,
    pub expect_events: Option<Vec<ExpectedEvents>>,
    pub outcome: Option<CollectionData>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    pub name: String,
    pub object: String,
    pub arguments: Option<Document>,
    pub expected_error: Option<ExpectedError>,
    pub expected_result: Option<Bson>,
    pub save_result_as_entity: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ExpectedEvents {
    pub client: String,
    pub events: Vec<TestEvent>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedError {
    #[serde(rename = "type")]
    pub error_type: Option<ErrorType>,
    pub error_contains: Option<String>,
    pub error_code_name: Option<String>,
    pub error_labels_contain: Option<Vec<String>>,
    pub error_labels_omit: Option<Vec<String>>,
    pub expected_result: Option<Bson>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ErrorType {
    Client,
    Server,
}
