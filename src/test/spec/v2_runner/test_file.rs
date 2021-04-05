use std::collections::HashMap;

use bson::{doc, from_document};
use futures::TryStreamExt;
use semver::VersionReq;
use serde::{Deserialize, Deserializer};

use crate::{
    bson::Document,
    client::options::ClientOptions,
    options::{FindOptions, SessionOptions},
    test::{EventClient, FailPoint, TestClient},
};

use super::{operation::Operation, test_event::CommandStartedEvent};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestFile {
    #[serde(rename = "runOn")]
    pub run_on: Option<Vec<RunOn>>,
    pub database_name: Option<String>,
    pub collection_name: Option<String>,
    pub bucket_name: Option<String>,
    pub data: Option<TestData>,
    pub tests: Vec<Test>,
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
            if !topology.contains(&client.topology_string()) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum TestData {
    Single(Vec<Document>),
    Many(HashMap<String, Vec<Document>>),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Test {
    pub description: String,
    pub skip_reason: Option<String>,
    pub use_multiple_mongoses: Option<bool>,
    pub client_options: Option<ClientOptions>,
    pub fail_point: Option<FailPoint>,
    pub session_options: Option<HashMap<String, SessionOptions>>,
    pub operations: Vec<Operation>,
    #[serde(default, deserialize_with = "deserialize_command_started_events")]
    pub expectations: Option<Vec<CommandStartedEvent>>,
    pub outcome: Option<Outcome>,
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

fn deserialize_command_started_events<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Vec<CommandStartedEvent>>, D::Error>
where
    D: Deserializer<'de>,
{
    let docs = Vec::<Document>::deserialize(deserializer)?;
    Ok(Some(
        docs.iter()
            .map(|doc| {
                let event = doc.get_document("command_started_event").unwrap();
                from_document(event.clone()).unwrap()
            })
            .collect(),
    ))
}
