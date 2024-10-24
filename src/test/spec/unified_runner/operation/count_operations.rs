use bson::{Bson, Document};
use futures::TryStreamExt;
use serde::Deserialize;
use crate::options::{AggregateOptions, CountOptions, DistinctOptions, EstimatedDocumentCountOptions};
use crate::test::spec::unified_runner::operation::TestOperation;
use crate::test::spec::unified_runner::{Entity, ExpectedEvent, TestRunner};
use crate::{Collection, Database};
use futures::future::BoxFuture;
use crate::test::spec::unified_runner::operation::{with_opt_session, with_mut_session};
use crate::error::Result;
use futures_util::FutureExt;

#[derive(Debug, Deserialize)]
pub(super) struct Aggregate {
    pipeline: Vec<Document>,
    session: Option<String>,
    #[serde(flatten)]
    options: AggregateOptions,
}

impl TestOperation for Aggregate {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let result = match &self.session {
                Some(session_id) => {
                    enum AggregateEntity {
                        Collection(Collection<Document>),
                        Database(Database),
                        Other(String),
                    }
                    let entity = match test_runner.entities.read().await.get(id).unwrap() {
                        Entity::Collection(c) => AggregateEntity::Collection(c.clone()),
                        Entity::Database(d) => AggregateEntity::Database(d.clone()),
                        other => AggregateEntity::Other(format!("{:?}", other)),
                    };
                    with_mut_session!(test_runner, session_id, |session| async {
                        let mut cursor = match entity {
                            AggregateEntity::Collection(collection) => {
                                collection
                                    .aggregate(self.pipeline.clone())
                                    .with_options(self.options.clone())
                                    .session(&mut *session)
                                    .await?
                            }
                            AggregateEntity::Database(db) => {
                                db.aggregate(self.pipeline.clone())
                                    .with_options(self.options.clone())
                                    .session(&mut *session)
                                    .await?
                            }
                            AggregateEntity::Other(debug) => {
                                panic!("Cannot execute aggregate on {}", &debug)
                            }
                        };
                        cursor.stream(session).try_collect::<Vec<Document>>().await
                    })
                    .await?
                }
                None => {
                    let entities = test_runner.entities.read().await;
                    let cursor = match entities.get(id).unwrap() {
                        Entity::Collection(collection) => {
                            collection
                                .aggregate(self.pipeline.clone())
                                .with_options(self.options.clone())
                                .await?
                        }
                        Entity::Database(db) => {
                            db.aggregate(self.pipeline.clone())
                                .with_options(self.options.clone())
                                .await?
                        }
                        other => panic!("Cannot execute aggregate on {:?}", &other),
                    };
                    cursor.try_collect::<Vec<Document>>().await?
                }
            };
            Ok(Some(Bson::from(result).into()))
        }
        .boxed()
    }

    fn returns_root_documents(&self) -> bool {
        true
    }
}

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
