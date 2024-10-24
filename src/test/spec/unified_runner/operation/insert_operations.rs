use std::collections::HashMap;

use bson::{to_bson, Document, Bson, doc};
use serde::Deserialize;

use crate::options::{InsertManyOptions, InsertOneOptions};
use crate::test::spec::unified_runner::operation::TestOperation;
use crate::test::spec::unified_runner::{Entity, TestRunner};
use futures::future::BoxFuture;
use crate::error::Result;
use futures_util::FutureExt;
use crate::test::spec::unified_runner::operation::{with_opt_session, with_mut_session};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct InsertOne {
    document: Document,
    session: Option<String>,
    #[serde(flatten)]
    options: InsertOneOptions,
}
impl TestOperation for InsertOne {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = with_opt_session!(
                test_runner,
                &self.session,
                collection
                    .insert_one(self.document.clone())
                    .with_options(self.options.clone()),
            )
            .await?;
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct InsertMany {
    documents: Vec<Document>,
    session: Option<String>,
    #[serde(flatten)]
    options: InsertManyOptions,
}

impl TestOperation for InsertMany {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = with_opt_session!(
                test_runner,
                &self.session,
                collection
                    .insert_many(&self.documents)
                    .with_options(self.options.clone()),
            )
            .await?;
            let ids: HashMap<String, Bson> = result
                .inserted_ids
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();
            let ids = to_bson(&ids)?;
            Ok(Some(Bson::from(doc! { "insertedIds": ids }).into()))
        }
        .boxed()
    }
}
