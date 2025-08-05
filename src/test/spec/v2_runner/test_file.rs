use std::collections::HashMap;

use crate::{
    bson::{doc, Bson},
    bson_compat::deserialize_from_document,
};
use futures::TryStreamExt;
use serde::{Deserialize, Deserializer};

use crate::{
    bson::Document,
    options::{ReadPreference, SelectionCriteria, SessionOptions},
    test::{
        get_topology,
        log_uncaptured,
        server_version_matches,
        spec::merge_uri_options,
        util::{fail_point::FailPoint, is_expected_type},
        Serverless,
        Topology,
        DEFAULT_URI,
    },
    Client,
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
    #[cfg(feature = "in-use-encryption")]
    pub(crate) json_schema: Option<Document>,
    #[cfg(feature = "in-use-encryption")]
    pub(crate) encrypted_fields: Option<Document>,
    #[cfg(feature = "in-use-encryption")]
    pub(crate) key_vault_data: Option<Vec<Document>>,
    pub(crate) tests: Vec<Test>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RunOn {
    pub(crate) min_server_version: Option<String>,
    pub(crate) max_server_version: Option<String>,
    pub(crate) topology: Option<Vec<Topology>>,
    pub(crate) serverless: Option<Serverless>,
}

impl RunOn {
    pub(crate) async fn can_run_on(&self) -> bool {
        if let Some(ref min_version) = self.min_server_version {
            if !server_version_matches(&format!(">= {min_version}")).await {
                log_uncaptured(format!(
                    "runOn mismatch: required server version >= {min_version}",
                ));
                return false;
            }
        }
        if let Some(ref max_version) = self.max_server_version {
            if !server_version_matches(&format!("<= {max_version}")).await {
                log_uncaptured(format!(
                    "runOn mismatch: required server version <= {max_version}",
                ));
                return false;
            }
        }
        if let Some(ref topology) = self.topology {
            let actual_topology = get_topology().await;
            if !topology.contains(actual_topology) {
                log_uncaptured(format!(
                    "runOn mismatch: required topology in {topology:?}, got {actual_topology:?}"
                ));
                return false;
            }
        }
        if let Some(ref serverless) = self.serverless {
            if !serverless.can_run() {
                log_uncaptured(format!(
                    "runOn mismatch: required serverless {serverless:?}"
                ));
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
    #[allow(dead_code)]
    Many(HashMap<String, Vec<Document>>),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Test {
    pub(crate) description: String,
    pub(crate) skip_reason: Option<String>,
    pub(crate) use_multiple_mongoses: Option<bool>,
    #[serde(default, rename = "clientOptions")]
    pub(crate) client_options: Option<ClientOptions>,
    pub(crate) fail_point: Option<FailPoint>,
    pub(crate) session_options: Option<HashMap<String, SessionOptions>>,
    pub(crate) operations: Vec<Operation>,
    #[serde(default, deserialize_with = "deserialize_command_started_events")]
    pub(crate) expectations: Option<Vec<CommandStartedEvent>>,
    pub(crate) outcome: Option<Outcome>,
}

#[derive(Debug)]
pub(crate) struct ClientOptions {
    pub(crate) uri: String,
    #[cfg(feature = "in-use-encryption")]
    pub(crate) auto_encrypt_opts: Option<crate::client::csfle::options::AutoEncryptionOptions>,
}

impl<'de> Deserialize<'de> for ClientOptions {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[cfg(feature = "in-use-encryption")]
        use serde::de::Error;
        #[allow(unused_mut)]
        let mut uri_options = Document::deserialize(deserializer)?;
        #[cfg(feature = "in-use-encryption")]
        let auto_encrypt_opts = uri_options
            .remove("autoEncryptOpts")
            .map(crate::bson_compat::deserialize_from_bson)
            .transpose()
            .map_err(D::Error::custom)?;
        let uri = merge_uri_options(&DEFAULT_URI, Some(&uri_options), true);
        Ok(Self {
            uri,
            #[cfg(feature = "in-use-encryption")]
            auto_encrypt_opts,
        })
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Outcome {
    pub(crate) collection: CollectionOutcome,
}

impl Outcome {
    pub(crate) async fn assert_matches_actual(
        &self,
        db_name: &str,
        coll_name: &str,
        client: &Client,
    ) {
        use crate::coll::options::CollectionOptions;

        let coll_name = match self.collection.name.as_deref() {
            Some(name) => name,
            None => coll_name,
        };
        #[cfg(not(feature = "in-use-encryption"))]
        let coll_opts = CollectionOptions::default();
        #[cfg(feature = "in-use-encryption")]
        let coll_opts = CollectionOptions::builder()
            .read_concern(crate::options::ReadConcern::local())
            .build();
        let coll = client
            .database(db_name)
            .collection_with_options(coll_name, coll_opts);
        let selection_criteria = SelectionCriteria::ReadPreference(ReadPreference::Primary);
        let actual_data: Vec<Document> = coll
            .find(doc! {})
            .sort(doc! { "_id": 1 })
            .selection_criteria(selection_criteria)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_data_matches(&actual_data, &self.collection.data);
    }
}

fn assert_data_matches(actual: &[Document], expected: &[Document]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "data length mismatch, expected {expected:?}, got {actual:?}"
    );
    for (a, e) in actual.iter().zip(expected.iter()) {
        assert_doc_matches(a, e);
    }
}

fn assert_doc_matches(actual: &Document, expected: &Document) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "doc length mismatch, expected {expected:?}, got {actual:?}"
    );
    for (k, expected_val) in expected {
        let actual_val = if let Some(v) = actual.get(k) {
            v
        } else {
            panic!("no value for {k:?}, expected {expected_val:?}");
        };
        if let Some(types) = is_expected_type(expected_val) {
            if types.contains(&actual_val.element_type()) {
                continue;
            } else {
                panic!("expected type {types:?}, actual value {actual_val:?}");
            }
        }
        match (expected_val, actual_val) {
            (Bson::Document(exp_d), Bson::Document(act_d)) => assert_doc_matches(act_d, exp_d),
            (e, a) => assert_eq!(e, a, "mismatch for {k:?}, expected {e:?} got {a:?}"),
        }
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
                deserialize_from_document(event.clone()).unwrap()
            })
            .collect(),
    ))
}
