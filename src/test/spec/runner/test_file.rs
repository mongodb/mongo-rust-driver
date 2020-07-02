use std::collections::HashMap;

use futures::stream::TryStreamExt;
use semver::VersionReq;
use serde::Deserialize;

use crate::{
    bson::{doc, Document},
    options::{ClientOptions, FindOptions},
    test::{util::EventClient, AnyTestOperation, TestEvent},
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
    pub min_server_version: Option<String>,
    pub max_server_version: Option<String>,
    pub topology: Option<Vec<String>>,
}

impl RunOn {
    pub fn can_run_on(&self, client: &EventClient) -> bool {
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
pub struct TestCase {
    pub description: String,
    pub client_options: Option<ClientOptions>,
    pub use_multiple_mongoses: Option<bool>,
    pub skip_reason: Option<String>,
    pub fail_point: Option<Document>,
    pub operations: Vec<AnyTestOperation>,
    pub outcome: Option<Outcome>,
    pub expectations: Option<Vec<TestEvent>>,
}

#[derive(Debug, Deserialize)]
pub struct Outcome {
    pub collection: CollectionOutcome,
}

impl Outcome {
    pub async fn matches_actual(
        self,
        db_name: String,
        coll_name: String,
        client: &EventClient,
    ) -> bool {
        let coll_name = match self.collection.name {
            Some(name) => name,
            None => coll_name,
        };
        let coll = client.database(&db_name).collection(&coll_name);
        let options = FindOptions::builder().sort(doc! { "_id": 1 }).build();
        let actual_data: Vec<Document> = coll
            .find(None, options)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        actual_data == self.collection.data
    }
}

#[derive(Debug, Deserialize)]
pub struct CollectionOutcome {
    pub name: Option<String>,
    pub data: Vec<Document>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperationObject {
    Database,
    Collection,
    Client,
    GridfsBucket,
}
