use bson::{to_bson, Document};
use futures::TryStreamExt;
use serde::Deserialize;
use tokio::sync::Mutex;
use crate::action::Action;
use crate::test::spec::unified_runner::operation::TestOperation;
use crate::options::{RunCursorCommandOptions, SelectionCriteria};
use crate::test::spec::unified_runner::{Entity, TestCursor, TestRunner};
use futures::future::BoxFuture;
use crate::test::spec::unified_runner::operation::{with_opt_session, with_mut_session};
use crate::error::Result;
use futures_util::FutureExt;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RunCommand {
    command: Document,
    // We don't need to use this field, but it needs to be included during deserialization so that
    // we can use the deny_unknown_fields tag.
    #[serde(rename = "commandName")]
    _command_name: String,
    read_preference: Option<SelectionCriteria>,
    session: Option<String>,
}

impl TestOperation for RunCommand {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let command = self.command.clone();

            let db = test_runner.get_database(id).await;
            let result = with_opt_session!(
                test_runner,
                &self.session,
                db.run_command(command)
                    .optional(self.read_preference.clone(), |a, rp| {
                        a.selection_criteria(rp)
                    }),
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
pub(super) struct RunCursorCommand {
    command: Document,
    // We don't need to use this field, but it needs to be included during deserialization so that
    // we can use the deny_unknown_fields tag.
    #[serde(rename = "commandName")]
    _command_name: String,

    #[serde(flatten)]
    options: RunCursorCommandOptions,
    session: Option<String>,
}

impl TestOperation for RunCursorCommand {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let command = self.command.clone();
            let db = test_runner.get_database(id).await;
            let options = self.options.clone();

            let action = db.run_cursor_command(command).with_options(options);
            let result = match &self.session {
                Some(session_id) => {
                    with_mut_session!(test_runner, session_id, |session| async {
                        let mut cursor = action.session(&mut *session).await?;
                        cursor.stream(session).try_collect::<Vec<_>>().await
                    })
                    .await?
                }
                None => {
                    let cursor = action.await?;
                    cursor.try_collect::<Vec<_>>().await?
                }
            };

            Ok(Some(bson::to_bson(&result)?.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateCommandCursor {
    command: Document,
    // We don't need to use this field, but it needs to be included during deserialization so that
    // we can use the deny_unknown_fields tag.
    #[serde(rename = "commandName")]
    _command_name: String,

    #[serde(flatten)]
    options: RunCursorCommandOptions,
    session: Option<String>,
}

impl TestOperation for CreateCommandCursor {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let command = self.command.clone();
            let db = test_runner.get_database(id).await;
            let options = self.options.clone();

            let action = db.run_cursor_command(command).with_options(options);
            match &self.session {
                Some(session_id) => {
                    let mut ses_cursor = None;
                    with_mut_session!(test_runner, session_id, |session| async {
                        ses_cursor = Some(action.session(session).await);
                    })
                    .await;
                    let test_cursor = TestCursor::Session {
                        cursor: ses_cursor.unwrap().unwrap(),
                        session_id: session_id.clone(),
                    };
                    Ok(Some(Entity::Cursor(test_cursor)))
                }
                None => {
                    let doc_cursor = action.await?;
                    let test_cursor = TestCursor::Normal(Mutex::new(doc_cursor));
                    Ok(Some(Entity::Cursor(test_cursor)))
                }
            }
        }
        .boxed()
    }

    fn returns_root_documents(&self) -> bool {
        false
    }
}
