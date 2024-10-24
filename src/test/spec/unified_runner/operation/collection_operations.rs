use bson::{doc, Document};
use serde::Deserialize;
use crate::options::{CreateCollectionOptions, DropCollectionOptions};
use crate::test::spec::unified_runner::operation::TestOperation;
use crate::test::spec::unified_runner::{Entity, TestRunner};
use futures::future::BoxFuture;
use crate::test::spec::unified_runner::operation::{with_opt_session, with_mut_session};
use crate::error::Result;
use futures_util::FutureExt;

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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RenameCollection {
    to: String,
}

impl TestOperation for RenameCollection {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let target = test_runner.get_collection(id).await;
            let ns = target.namespace();
            let mut to_ns = ns.clone();
            to_ns.coll.clone_from(&self.to);
            let cmd = doc! {
                "renameCollection": crate::bson::to_bson(&ns)?,
                "to": crate::bson::to_bson(&to_ns)?,
            };
            let admin = test_runner.internal_client.database("admin");
            admin.run_command(cmd).await?;
            Ok(None)
        }
        .boxed()
    }
}
