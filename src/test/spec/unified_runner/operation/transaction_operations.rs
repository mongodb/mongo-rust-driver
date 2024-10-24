use serde::Deserialize;
use crate::options::TransactionOptions;
use crate::test::spec::unified_runner::operation::TestOperation;
use crate::test::spec::unified_runner::TestRunner;
use crate::test::spec::unified_runner::Operation;
use futures::future::BoxFuture;
use crate::test::spec::unified_runner::operation::with_mut_session;
use crate::error::Result;
use futures_util::FutureExt;
use super::Entity;
use crate::test::spec::unified_runner::entity;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct StartTransaction {
    #[serde(flatten)]
    options: TransactionOptions,
}

impl TestOperation for StartTransaction {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            with_mut_session!(test_runner, id, |session| {
                async move {
                    session
                        .start_transaction()
                        .with_options(self.options.clone())
                        .await
                }
            })
            .await?;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CommitTransaction {}

impl TestOperation for CommitTransaction {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            with_mut_session!(test_runner, id, |session| {
                async move { session.commit_transaction().await }
            })
            .await?;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AbortTransaction {}

impl TestOperation for AbortTransaction {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            with_mut_session!(test_runner, id, |session| async move {
                session.abort_transaction().await
            })
            .await?;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WithTransaction {
    callback: Vec<Operation>,
    #[serde(flatten)]
    options: Option<TransactionOptions>,
}

impl TestOperation for WithTransaction {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            with_mut_session!(test_runner, id, |session| async move {
                session
                    .start_transaction()
                    .with_options(self.options.clone())
                    .and_run(
                        (&self.callback, test_runner),
                        |session, (callback, test_runner)| {
                            async move {
                                test_runner.entities.write().await.insert(
                                    id.to_string(),
                                    Entity::SessionPtr(entity::SessionPtr(session)),
                                );
                                let mut result = Ok(());
                                for op in callback.iter() {
                                    let r =
                                        op.execute_fallible(test_runner, "withTransaction").await;
                                    if r.is_err() {
                                        result = r;
                                        break;
                                    }
                                }
                                test_runner.entities.write().await.remove(id);
                                result
                            }
                            .boxed()
                        },
                    )
                    .await
            })
            .await?;
            Ok(None)
        }
        .boxed()
    }
}