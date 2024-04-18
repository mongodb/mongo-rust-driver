use serde::Deserialize;

use super::super::{Operation, RunOn};
use crate::{
    bson::{Bson, Document},
    options::ClientOptions,
    test::util::fail_point::FailPoint,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TestFile {
    pub(crate) run_on: Option<Vec<RunOn>>,
    pub(crate) data: Vec<Document>,
    pub(crate) tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TestCase {
    pub(crate) description: String,
    pub(crate) client_options: Option<ClientOptions>,
    pub(crate) use_multiple_mongoses: Option<bool>,
    pub(crate) fail_point: Option<FailPoint>,
    pub(crate) operation: Operation,
    pub(crate) outcome: Outcome,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Outcome {
    pub(crate) error: Option<bool>,
    pub(crate) result: Option<TestResult>,
    pub(crate) collection: CollectionOutcome,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum TestResult {
    Labels(Labels),
    Value(Bson),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Labels {
    pub(crate) error_labels_contain: Option<Vec<String>>,
    pub(crate) error_labels_omit: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CollectionOutcome {
    pub(crate) name: Option<String>,
    pub(crate) data: Vec<Document>,
}
