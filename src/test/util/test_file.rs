use crate::test::util::test_event::TestEvent;
use bson::{doc, Bson, Document};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TestFile {
    #[serde(rename = "runOn")]
    pub run_on: Option<Vec<RunOn>>,
    pub database_name: Option<String>,
    pub collection_name: Option<String>,
    pub bucket_name: Option<String>,
    pub data: Option<Bson>,
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunOn {
    pub min_server_version: Option<String>,
    pub max_server_version: Option<String>,
    pub topology: Option<Vec<String>>,
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
    pub object: Option<String>,
    pub arguments: Option<Bson>,
    pub result: Option<Bson>,
    pub error: Option<bool>,
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
