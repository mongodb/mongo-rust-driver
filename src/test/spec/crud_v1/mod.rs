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

use futures::stream::TryStreamExt;
use serde::Deserialize;

use crate::{bson::Document, Collection};

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
    pub error: Option<bool>,
    pub collection: Option<CollectionOutcome>,
}

#[derive(Debug, Deserialize)]
pub struct CollectionOutcome {
    pub name: Option<String>,
    pub data: Vec<Document>,
}

pub async fn find_all(coll: &Collection) -> Vec<Document> {
    coll.find(None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}
