use crate::test::{
    util::{parse_version, EventClient},
    TestEvent,
};
use bson::{doc, Bson, Document};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct TestFile {
    #[serde(rename = "runOn")]
    pub run_on: Option<Vec<RunOn>>,
    pub database_name: Option<String>,
    pub collection_name: Option<String>,
    pub bucket_name: Option<String>,
    pub data: Option<TestData>,
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum TestData {
    Single(Vec<Document>),
    Many(HashMap<String, Vec<Document>>),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunOn {
    pub min_server_version: Option<String>,
    pub max_server_version: Option<String>,
    pub topology: Option<Vec<String>>,
}

impl RunOn {
    pub fn can_run_on(&self, client: &EventClient) -> bool {
        if let Some(ref min_server_version) = self.min_server_version {
            let (major, minor) = parse_version(min_server_version);
            if client.server_version_lt(major, minor) {
                return false;
            }
        }
        if let Some(ref max_server_version) = self.max_server_version {
            let (major, minor) = parse_version(max_server_version);
            if client.server_version_gt(major, minor) {
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
pub struct TestCase {
    pub description: String,
    pub client_options: Option<Document>,
    pub use_multiple_mongoses: Option<bool>,
    pub skip_reason: Option<String>,
    pub fail_point: Option<Document>,
    pub operations: Vec<Operation>,
    pub outcome: Option<Document>,
    pub expectations: Option<Vec<TestEvent>>,
}

#[derive(Debug, Deserialize)]
pub struct Operation {
    pub name: String,
    pub object: OperationObject,
    pub arguments: Option<Bson>,
    pub result: Option<Bson>,
    pub error: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperationObject {
    Database,
    Collection,
    Client,
    GridfsBucket,
}

impl Operation {
    pub fn as_document(&self) -> Document {
        let mut doc = Document::new();
        doc.insert("name", self.name.clone());
        if let Some(ref arguments) = self.arguments {
            doc.insert("arguments", arguments);
        }
        doc
    }
}
