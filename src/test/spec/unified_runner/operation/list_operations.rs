use crate::{
    action::Action,
    error::Result,
    options::ListCollectionsOptions,
    test::spec::unified_runner::{
        operation::{with_mut_session, with_opt_session, TestOperation},
        Entity,
        TestRunner,
    },
};
use bson::{Bson, Document};
use futures::{future::BoxFuture, TryStreamExt};
use futures_util::FutureExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ListDatabases {
    session: Option<String>,
    #[serde(flatten)]
    options: crate::db::options::ListDatabasesOptions,
}

impl TestOperation for ListDatabases {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let client = test_runner.get_client(id).await;
            let result = with_opt_session!(
                test_runner,
                &self.session,
                client.list_databases().with_options(self.options.clone()),
            )
            .await?;
            Ok(Some(bson::to_bson(&result)?.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ListDatabaseNames {
    #[serde(flatten)]
    options: crate::db::options::ListDatabasesOptions,
}

impl TestOperation for ListDatabaseNames {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let client = test_runner.get_client(id).await;
            let result = client
                .list_database_names()
                .with_options(self.options.clone())
                .await?;
            let result: Vec<Bson> = result.iter().map(|s| Bson::String(s.to_string())).collect();
            Ok(Some(Bson::Array(result).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ListCollections {
    session: Option<String>,
    #[serde(flatten)]
    options: ListCollectionsOptions,
}

impl TestOperation for ListCollections {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let db = test_runner.get_database(id).await;
            let result = match &self.session {
                Some(session_id) => {
                    with_mut_session!(test_runner, session_id, |session| async {
                        let mut cursor = db
                            .list_collections()
                            .with_options(self.options.clone())
                            .session(&mut *session)
                            .await?;
                        cursor.stream(session).try_collect::<Vec<_>>().await
                    })
                    .await?
                }
                None => {
                    let cursor = db
                        .list_collections()
                        .with_options(self.options.clone())
                        .await?;
                    cursor.try_collect::<Vec<_>>().await?
                }
            };
            Ok(Some(bson::to_bson(&result)?.into()))
        }
        .boxed()
    }

    fn returns_root_documents(&self) -> bool {
        true
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ListCollectionNames {
    filter: Option<Document>,
}

impl TestOperation for ListCollectionNames {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let db = test_runner.get_database(id).await;
            let result = db
                .list_collection_names()
                .optional(self.filter.clone(), |b, f| b.filter(f))
                .await?;
            let result: Vec<Bson> = result.iter().map(|s| Bson::String(s.to_string())).collect();
            Ok(Some(Bson::from(result).into()))
        }
        .boxed()
    }
}
