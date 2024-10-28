use std::time::Duration;

use crate::{
    error::Result,
    options::{
        Collation,
        FindOneAndDeleteOptions,
        FindOneAndReplaceOptions,
        FindOneAndUpdateOptions,
        FindOneOptions,
        FindOptions,
        Hint,
        ReadConcern,
        UpdateModifications,
    },
    serde_util,
    test::spec::unified_runner::{
        operation::{with_mut_session, with_opt_session, TestOperation},
        Entity,
        TestCursor,
        TestRunner,
    },
};
use bson::{to_bson, Bson, Document};
use futures::{future::BoxFuture, TryStreamExt};
use futures_util::FutureExt;
use serde::{Deserialize, Deserializer};
use tokio::sync::Mutex;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Find {
    filter: Document,
    session: Option<String>,
    // `FindOptions` cannot be embedded directly because serde doesn't support combining `flatten`
    // and `deny_unknown_fields`, so its fields are replicated here.
    allow_disk_use: Option<bool>,
    allow_partial_results: Option<bool>,
    batch_size: Option<u32>,
    comment: Option<Bson>,
    hint: Option<Hint>,
    limit: Option<i64>,
    max: Option<Document>,
    max_scan: Option<u64>,
    #[serde(
        default,
        rename = "maxTimeMS",
        deserialize_with = "serde_util::deserialize_duration_option_from_u64_millis"
    )]
    max_time: Option<Duration>,
    min: Option<Document>,
    no_cursor_timeout: Option<bool>,
    projection: Option<Document>,
    read_concern: Option<ReadConcern>,
    return_key: Option<bool>,
    show_record_id: Option<bool>,
    skip: Option<u64>,
    sort: Option<Document>,
    collation: Option<Collation>,
    #[serde(rename = "let")]
    let_vars: Option<Document>,
}

impl Find {
    async fn get_cursor<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> Result<TestCursor> {
        let collection = test_runner.get_collection(id).await;

        // `FindOptions` is constructed without the use of `..Default::default()` to enforce at
        // compile-time that any new fields added there need to be considered here.
        let options = FindOptions {
            allow_disk_use: self.allow_disk_use,
            allow_partial_results: self.allow_partial_results,
            batch_size: self.batch_size,
            comment: self.comment.clone(),
            hint: self.hint.clone(),
            limit: self.limit,
            max: self.max.clone(),
            max_scan: self.max_scan,
            max_time: self.max_time,
            min: self.min.clone(),
            no_cursor_timeout: self.no_cursor_timeout,
            projection: self.projection.clone(),
            read_concern: self.read_concern.clone(),
            return_key: self.return_key,
            show_record_id: self.show_record_id,
            skip: self.skip,
            sort: self.sort.clone(),
            collation: self.collation.clone(),
            cursor_type: None,
            max_await_time: None,
            selection_criteria: None,
            let_vars: self.let_vars.clone(),
        };
        let act = collection.find(self.filter.clone()).with_options(options);
        match &self.session {
            Some(session_id) => {
                let cursor = with_mut_session!(test_runner, session_id, |session| async {
                    act.session(session).await
                })
                .await?;
                Ok(TestCursor::Session {
                    cursor,
                    session_id: session_id.clone(),
                })
            }
            None => {
                let cursor = act.await?;
                Ok(TestCursor::Normal(Mutex::new(cursor)))
            }
        }
    }
}

impl TestOperation for Find {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let result = match self.get_cursor(id, test_runner).await? {
                TestCursor::Session {
                    mut cursor,
                    session_id,
                } => {
                    with_mut_session!(test_runner, session_id.as_str(), |s| async {
                        cursor.stream(s).try_collect::<Vec<Document>>().await
                    })
                    .await?
                }
                TestCursor::Normal(cursor) => {
                    let cursor = cursor.into_inner();
                    cursor.try_collect::<Vec<Document>>().await?
                }
                TestCursor::ChangeStream(_) => panic!("get_cursor returned a change stream"),
                TestCursor::Closed => panic!("get_cursor returned a closed cursor"),
            };
            Ok(Some(Bson::from(result).into()))
        }
        .boxed()
    }

    fn returns_root_documents(&self) -> bool {
        true
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CreateFindCursor {
    // `Find` cannot be embedded directly because serde doesn't support combining `flatten`
    // and `deny_unknown_fields`, so its fields are replicated here.
    filter: Document,
    session: Option<String>,
    allow_disk_use: Option<bool>,
    allow_partial_results: Option<bool>,
    batch_size: Option<u32>,
    comment: Option<Bson>,
    hint: Option<Hint>,
    limit: Option<i64>,
    max: Option<Document>,
    max_scan: Option<u64>,
    #[serde(rename = "maxTimeMS")]
    max_time: Option<Duration>,
    min: Option<Document>,
    no_cursor_timeout: Option<bool>,
    projection: Option<Document>,
    read_concern: Option<ReadConcern>,
    return_key: Option<bool>,
    show_record_id: Option<bool>,
    skip: Option<u64>,
    sort: Option<Document>,
    collation: Option<Collation>,
    #[serde(rename = "let")]
    let_vars: Option<Document>,
}

impl TestOperation for CreateFindCursor {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let find = Find {
                filter: self.filter.clone(),
                session: self.session.clone(),
                allow_disk_use: self.allow_disk_use,
                allow_partial_results: self.allow_partial_results,
                batch_size: self.batch_size,
                comment: self.comment.clone(),
                hint: self.hint.clone(),
                limit: self.limit,
                max: self.max.clone(),
                max_scan: self.max_scan,
                max_time: self.max_time,
                min: self.min.clone(),
                no_cursor_timeout: self.no_cursor_timeout,
                projection: self.projection.clone(),
                read_concern: self.read_concern.clone(),
                return_key: self.return_key,
                show_record_id: self.show_record_id,
                skip: self.skip,
                sort: self.sort.clone(),
                collation: self.collation.clone(),
                let_vars: self.let_vars.clone(),
            };
            let cursor = find.get_cursor(id, test_runner).await?;
            Ok(Some(Entity::Cursor(cursor)))
        }
        .boxed()
    }

    fn returns_root_documents(&self) -> bool {
        false
    }
}

#[derive(Debug, Default)]
pub(super) struct FindOne {
    filter: Option<Document>,
    options: FindOneOptions,
}

// TODO RUST-1364: remove this impl and derive Deserialize instead
impl<'de> Deserialize<'de> for FindOne {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Helper {
            filter: Option<Document>,
            #[serde(flatten)]
            options: FindOneOptions,
        }

        let helper = Helper::deserialize(deserializer)?;

        Ok(Self {
            filter: helper.filter,
            options: helper.options,
        })
    }
}

impl TestOperation for FindOne {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = collection
                .find_one(self.filter.clone().unwrap_or_default())
                .with_options(self.options.clone())
                .await?;
            match result {
                Some(result) => Ok(Some(Bson::from(result).into())),
                None => Ok(Some(Entity::None)),
            }
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FindOneAndUpdate {
    filter: Document,
    update: UpdateModifications,
    session: Option<String>,
    #[serde(flatten)]
    options: FindOneAndUpdateOptions,
}

impl TestOperation for FindOneAndUpdate {
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
                    .find_one_and_update(self.filter.clone(), self.update.clone())
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
pub(super) struct FindOneAndReplace {
    filter: Document,
    replacement: Document,
    session: Option<String>,
    #[serde(flatten)]
    options: FindOneAndReplaceOptions,
}

impl TestOperation for FindOneAndReplace {
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
                    .find_one_and_replace(self.filter.clone(), self.replacement.clone())
                    .with_options(self.options.clone())
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
pub(super) struct FindOneAndDelete {
    filter: Document,
    session: Option<String>,
    #[serde(flatten)]
    options: FindOneAndDeleteOptions,
}

impl TestOperation for FindOneAndDelete {
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
                    .find_one_and_delete(self.filter.clone())
                    .with_options(self.options.clone())
            )
            .await?;
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}
