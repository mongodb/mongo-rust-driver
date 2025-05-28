use crate::{
    bson::{doc, Bson, Document},
    error::Result,
    options::{AggregateOptions, CreateCollectionOptions, DropCollectionOptions},
    test::spec::unified_runner::{
        operation::{with_mut_session, with_opt_session, TestOperation},
        Entity,
        TestRunner,
    },
    Collection,
    Database,
};
use futures::{future::BoxFuture, TryStreamExt};
use futures_util::FutureExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertCollectionExists {
    collection_name: String,
    database_name: String,
}

impl TestOperation for AssertCollectionExists {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let db = test_runner.internal_client.database(&self.database_name);
            let names = db.list_collection_names().await.unwrap();
            assert!(names.contains(&self.collection_name));
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertCollectionNotExists {
    collection_name: String,
    database_name: String,
}

impl TestOperation for AssertCollectionNotExists {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let db = test_runner.internal_client.database(&self.database_name);
            let names = db.list_collection_names().await.unwrap();
            assert!(!names.contains(&self.collection_name));
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CreateCollection {
    collection: String,
    #[serde(flatten)]
    options: CreateCollectionOptions,
    session: Option<String>,
}

impl TestOperation for CreateCollection {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let database = test_runner.get_database(id).await;
            with_opt_session!(
                test_runner,
                &self.session,
                database
                    .create_collection(&self.collection)
                    .with_options(self.options.clone()),
            )
            .await?;
            Ok(Some(Entity::Collection(
                database.collection(&self.collection),
            )))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DropCollection {
    collection: String,
    #[serde(flatten)]
    options: DropCollectionOptions,
    session: Option<String>,
}

impl TestOperation for DropCollection {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let database = test_runner.get_database(id).await;
            let collection = database.collection::<Document>(&self.collection).clone();
            with_opt_session!(
                test_runner,
                &self.session,
                collection.drop().with_options(self.options.clone()),
            )
            .await?;
            Ok(None)
        }
        .boxed()
    }
}

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
