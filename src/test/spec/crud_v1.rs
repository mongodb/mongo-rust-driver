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

use std::future::Future;

use futures::stream::TryStreamExt;
use serde::Deserialize;

use crate::{
    bson::{doc, Document},
    test::log_uncaptured,
    Collection,
};

use super::{run_spec_test, Serverless};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TestFile {
    pub data: Vec<Document>,
    pub min_server_version: Option<String>,
    pub(crate) serverless: Option<Serverless>,
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

pub async fn find_all(coll: &Collection<Document>) -> Vec<Document> {
    coll.find(doc! {})
        .sort(doc! { "_id": 1 })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

pub async fn run_crud_v1_test<F, G>(spec: &[&str], run_test_file: F)
where
    F: Fn(TestFile) -> G,
    G: Future<Output = ()>,
{
    run_spec_test(spec, |t: TestFile| async {
        if let Some(ref serverless) = t.serverless {
            if !serverless.can_run() {
                log_uncaptured("skipping crud_v1_test");
                return;
            }
        }

        run_test_file(t).await
    })
    .await
}
