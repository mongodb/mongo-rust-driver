use bson::Document;
use mongodb::{options::collation::Collation, Client};

#[derive(Debug, Deserialize)]
pub struct TestFile {
    pub data: Vec<Document>,
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
pub struct TestCase {
    pub description: String,
    pub operation: Operation,
    pub outcome: Document,
}

#[derive(Debug, Deserialize)]
pub struct Operation {
    name: String,
    arguments: Document,
}

#[derive(Debug, Deserialize)]
pub struct Outcome<R> {
    pub result: R,
    pub collection: Option<CollectionOutcome>,
}

#[derive(Debug, Deserialize)]
pub struct CollectionOutcome {
    pub name: Option<String>,
    pub data: Vec<Document>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub pipeline: Vec<Document>,
    pub batch_size: Option<i32>,
    pub collation: Option<Collation>,
}

fn matches(expected: Document, actual: Document) -> Result<(), &'static str> {
    unimplemented!();
}

#[function_name]
fn run_change_stream_test(test_file: TestFile) {
    let data = test_file.data;

    let global_client = Client::with_uri_str("mongodb://localhost:27017");

    for mut test_case in test_file.tests {
        test_case.description = test_case.description.replace('$', "%");

        match data.get("topology".to_string()) {}
    }
}

#[test]
fn run() {
    crate::spec::test(&["change-streams"], run_change_stream_test);
}
