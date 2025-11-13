use std::collections::HashMap;

use crate::{
    bson::{doc, Bson, Document},
    bson_compat::serialize_to_bson,
};
use serde::Deserialize;

use crate::{
    error::Result,
    options::{InsertManyOptions, InsertOneOptions},
    test::spec::unified_runner::{
        operation::{with_mut_session, with_opt_session, TestOperation},
        Entity,
        TestRunner,
    },
};
use futures::future::BoxFuture;
use futures_util::FutureExt;

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
            let result = serialize_to_bson(&result)?;
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
            let ids = serialize_to_bson(&ids)?;
            Ok(Some(Bson::from(doc! { "insertedIds": ids }).into()))
        }
        .boxed()
    }
}
