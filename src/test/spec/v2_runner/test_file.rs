use std::collections::HashMap;

use bson::{doc, from_document};
use futures::TryStreamExt;
use semver::VersionReq;
use serde::{Deserialize, Deserializer};

use crate::{
    bson::Document,
    options::{FindOptions, ReadPreference, SelectionCriteria, SessionOptions},
    test::{spec::merge_uri_options, EventClient, FailPoint, Serverless, TestClient, DEFAULT_URI},
};

use super::{operation::Operation, test_event::CommandStartedEvent};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TestFile {
    #[serde(rename = "runOn")]
    pub(crate) run_on: Option<Vec<RunOn>>,
    pub(crate) database_name: Option<String>,
    pub(crate) collection_name: Option<String>,
    #[allow(unused)]
    pub(crate) bucket_name: Option<String>,
    pub(crate) data: Option<TestData>,
    pub(crate) tests: Vec<Test>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RunOn {
    pub(crate) min_server_version: Option<String>,
    pub(crate) max_server_version: Option<String>,
    pub(crate) topology: Option<Vec<String>>,
    pub(crate) serverless: Option<Serverless>,
}

impl RunOn {
    pub(crate) fn can_run_on(&self, client: &TestClient) -> bool {
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
        if let Some(ref serverless) = self.serverless {
            if !serverless.can_run() {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum TestData {
    Single(Vec<Document>),
    Many(HashMap<String, Vec<Document>>),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Test {
    pub(crate) description: String,
    pub(crate) skip_reason: Option<String>,
    pub(crate) use_multiple_mongoses: Option<bool>,
    #[serde(
        default,
        deserialize_with = "deserialize_uri_options_to_uri_string_option",
        rename = "clientOptions"
    )]
    pub(crate) client_uri: Option<String>,
    pub(crate) fail_point: Option<FailPoint>,
    pub(crate) session_options: Option<HashMap<String, SessionOptions>>,
    pub(crate) operations: Vec<Operation>,
    #[serde(default, deserialize_with = "deserialize_command_started_events")]
    pub(crate) expectations: Option<Vec<CommandStartedEvent>>,
    pub(crate) outcome: Option<Outcome>,
}

fn deserialize_uri_options_to_uri_string_option<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let uri_options = Document::deserialize(deserializer)?;
    Ok(Some(merge_uri_options(
        &DEFAULT_URI,
        Some(&uri_options),
        true,
    )))
}

#[derive(Debug, Deserialize)]
pub(crate) struct Outcome {
    pub(crate) collection: CollectionOutcome,
}

impl Outcome {
    pub(crate) async fn matches_actual(
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
        let selection_criteria = SelectionCriteria::ReadPreference(ReadPreference::Primary);
        let options = FindOptions::builder()
            .sort(doc! { "_id": 1 })
            .selection_criteria(selection_criteria)
            .build();
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
pub(crate) struct CollectionOutcome {
    pub(crate) name: Option<String>,
    pub(crate) data: Vec<Document>,
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
