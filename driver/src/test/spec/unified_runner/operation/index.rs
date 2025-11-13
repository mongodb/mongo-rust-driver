use crate::{
    bson::{Bson, Document},
    error::Result,
    options::{DropIndexOptions, IndexOptions, ListIndexesOptions},
    test::spec::unified_runner::{
        operation::{with_mut_session, with_opt_session, TestOperation},
        Entity,
        TestRunner,
    },
    IndexModel,
};
use futures::{future::BoxFuture, TryStreamExt};
use futures_util::FutureExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateIndex {
    session: Option<String>,
    keys: Document,
    name: Option<String>,
    unique: Option<bool>,
}

impl TestOperation for CreateIndex {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let options = IndexOptions::builder()
                .name(self.name.clone())
                .unique(self.unique)
                .build();
            let index = IndexModel::builder()
                .keys(self.keys.clone())
                .options(options)
                .build();

            let collection = test_runner.get_collection(id).await;
            let name =
                with_opt_session!(test_runner, &self.session, collection.create_index(index))
                    .await?
                    .index_name;
            Ok(Some(Bson::String(name).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListIndexes {
    session: Option<String>,
    #[serde(flatten)]
    options: ListIndexesOptions,
}

impl TestOperation for ListIndexes {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let act = collection.list_indexes().with_options(self.options.clone());
            let indexes: Vec<IndexModel> = match self.session {
                Some(ref session) => {
                    with_mut_session!(test_runner, session, |session| {
                        async {
                            act.session(&mut *session)
                                .await?
                                .stream(session)
                                .try_collect()
                                .await
                        }
                    })
                    .await?
                }
                None => act.await?.try_collect().await?,
            };
            let indexes: Vec<Document> = indexes
                .iter()
                .map(|index| crate::bson_compat::serialize_to_document(index).unwrap())
                .collect();
            Ok(Some(Bson::from(indexes).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListIndexNames {
    session: Option<String>,
}

impl TestOperation for ListIndexNames {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let names =
                with_opt_session!(test_runner, &self.session, collection.list_index_names(),)
                    .await?;
            Ok(Some(Bson::from(names).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertIndexExists {
    collection_name: String,
    database_name: String,
    index_name: String,
}

impl TestOperation for AssertIndexExists {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let coll = test_runner
                .internal_client
                .database(&self.database_name)
                .collection::<Document>(&self.collection_name);
            let names = coll.list_index_names().await.unwrap();
            assert!(names.contains(&self.index_name));
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertIndexNotExists {
    collection_name: String,
    database_name: String,
    index_name: String,
}

impl TestOperation for AssertIndexNotExists {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let coll = test_runner
                .internal_client
                .database(&self.database_name)
                .collection::<Document>(&self.collection_name);
            match coll.list_index_names().await {
                Ok(indexes) => assert!(!indexes.contains(&self.index_name)),
                // a namespace not found error indicates that the index does not exist
                Err(err) => assert_eq!(err.sdam_code(), Some(26)),
            }
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct DropIndex {
    session: Option<String>,
    name: String,
    #[serde(flatten)]
    options: Option<DropIndexOptions>,
}

impl TestOperation for DropIndex {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            with_opt_session!(
                test_runner,
                &self.session,
                collection
                    .drop_index(&self.name)
                    .with_options(self.options.clone())
            )
            .await?;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct DropIndexes {
    session: Option<String>,
    #[serde(flatten)]
    options: Option<DropIndexOptions>,
}

impl TestOperation for DropIndexes {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            with_opt_session!(
                test_runner,
                &self.session,
                collection.drop_indexes().with_options(self.options.clone())
            )
            .await?;
            Ok(None)
        }
        .boxed()
    }
}
