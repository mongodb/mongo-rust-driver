mod aggregate;
mod count;
mod delete_many;
mod delete_one;
mod distinct;
mod find;
mod find_one_and_delete;
mod find_one_and_replace;
mod find_one_and_update;
mod insert_many;
mod insert_one;
mod replace_one;
mod update_many;
mod update_one;

use bson::Document;
use mongodb::Collection;

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

pub fn find_all(coll: &Collection) -> Vec<Document> {
    coll.find(None, None).unwrap().map(Result::unwrap).collect()
}
