use crate::{
    error::Result,
    options::{CountOptions, DistinctOptions, EstimatedDocumentCountOptions},
    test::spec::unified_runner::{
        operation::{with_mut_session, with_opt_session, TestOperation},
        Entity,
        ExpectedEvent,
        TestRunner,
    },
};
use bson::{Bson, Document};
use futures::future::BoxFuture;
use futures_util::FutureExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Distinct {
    field_name: String,
    filter: Option<Document>,
    session: Option<String>,
    #[serde(flatten)]
    options: DistinctOptions,
}

impl TestOperation for Distinct {
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
                    .distinct(&self.field_name, self.filter.clone().unwrap_or_default())
                    .with_options(self.options.clone()),
            )
            .await?;
            Ok(Some(Bson::Array(result).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CountDocuments {
    session: Option<String>,
    filter: Option<Document>,
    #[serde(flatten)]
    options: CountOptions,
}

impl TestOperation for CountDocuments {
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
                    .count_documents(self.filter.clone().unwrap_or_default())
                    .with_options(self.options.clone()),
            )
            .await?;
            Ok(Some(Bson::Int64(result.try_into().unwrap()).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct EstimatedDocumentCount {
    #[serde(flatten)]
    options: EstimatedDocumentCountOptions,
}

impl TestOperation for EstimatedDocumentCount {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = collection
                .estimated_document_count()
                .with_options(self.options.clone())
                .await?;
            Ok(Some(Bson::Int64(result.try_into().unwrap()).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertEventCount {
    client: String,
    event: ExpectedEvent,
    count: usize,
}

impl TestOperation for AssertEventCount {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async {
            let client = test_runner.get_client(self.client.as_str()).await;
            let entities = test_runner.entities.read().await;
            let actual_events = client.matching_events(&self.event, &entities);
            assert_eq!(
                actual_events.len(),
                self.count,
                "expected to see {} events matching: {:#?}, instead saw: {:#?}",
                self.count,
                self.event,
                actual_events
            );
        }
        .boxed()
    }
}
