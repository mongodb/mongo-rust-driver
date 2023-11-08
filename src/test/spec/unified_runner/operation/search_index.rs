use bson::{Bson, to_bson};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use serde::Deserialize;

use crate::{error::Result, search_index::options::{CreateSearchIndexOptions, DropSearchIndexOptions}, SearchIndexModel, test::spec::unified_runner::{TestRunner, Entity}};

use super::TestOperation;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CreateSearchIndex {
    model: SearchIndexModel,
    #[serde(flatten)]
    options: CreateSearchIndexOptions,
}

impl TestOperation for CreateSearchIndex {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let name = collection.create_search_index(self.model.clone(), self.options.clone()).await?;
            Ok(Some(Bson::String(name).into()))
        }.boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CreateSearchIndexes {
    models: Vec<SearchIndexModel>,
    #[serde(flatten)]
    options: CreateSearchIndexOptions,
}

impl TestOperation for CreateSearchIndexes {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let names = collection.create_search_indexes(self.models.clone(), self.options.clone()).await?;
            Ok(Some(to_bson(&names)?.into()))
        }.boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DropSearchIndex {
    name: String,
    #[serde(flatten)]
    options: DropSearchIndexOptions,
}

impl TestOperation for DropSearchIndex {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            collection.drop_search_index(&self.name, self.options.clone()).await?;
            Ok(None)
        }.boxed()
    }
}