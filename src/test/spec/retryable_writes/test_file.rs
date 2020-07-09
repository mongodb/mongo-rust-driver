use serde::Deserialize;

use super::super::{AnyTestOperation, RunOn};
use crate::{
    bson::{Bson, Document},
    options::ClientOptions,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestFile {
    pub run_on: Option<Vec<RunOn>>,
    pub data: Vec<Document>,
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestCase {
    pub description: String,
    pub client_options: Option<ClientOptions>,
    pub use_multiple_mongoses: Option<bool>,
    pub fail_point: Option<Document>,
    pub operation: AnyTestOperation,
    pub outcome: Outcome,
}

#[derive(Debug, Deserialize)]
pub struct Outcome {
    pub error: Option<bool>,
    pub result: Option<Result>,
    pub collection: CollectionOutcome,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Result {
    Value(Bson),
    Labels(Labels),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Labels {
    pub error_labels_contain: Option<Vec<String>>,
    pub error_labels_omit: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct CollectionOutcome {
    pub name: Option<String>,
    pub data: Vec<Document>,
}
