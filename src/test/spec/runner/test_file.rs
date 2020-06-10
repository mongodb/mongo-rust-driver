
use std::collections::HashMap;

use semver::VersionReq;
use serde::Deserialize;

use crate::{
    bson::{doc, Document},
    test::{
        util::EventClient,
        AnyTestOperation,
        TestEvent,
    },
};

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
    pub min_version: Option<String>,
    pub max_version: Option<String>,
    pub topology: Option<Vec<String>>,
}

impl RunOn {
    pub fn can_run_on(&self, client: &EventClient) -> bool {
        if let Some(ref min_version) = self.min_version {
            let req = VersionReq::parse(&format!(">= {}", &min_version)).unwrap();
            if !req.matches(&client.server_version) {
                return false;
            }
        }
        if let Some(ref max_version) = self.max_version {
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
pub struct TestCase {
    pub description: String,
    pub client_options: Option<Document>,
    pub use_multiple_mongoses: Option<bool>,
    pub skip_reason: Option<String>,
    pub fail_point: Option<Document>,
    pub operations: Vec<AnyTestOperation>,
    pub outcome: Option<Document>,
    pub expectations: Option<Vec<TestEvent>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperationObject {
    Database,
    Collection,
    Client,
    GridfsBucket,
}
