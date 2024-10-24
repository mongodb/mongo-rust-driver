use bson::Bson;
use futures::StreamExt;
use serde::Deserialize;
use crate::test::spec::unified_runner::operation::TestOperation;
use crate::test::spec::unified_runner::{Entity, TestCursor, TestRunner};
use futures::future::BoxFuture;
use crate::error::Result;
use futures_util::FutureExt;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct IterateOnce {}

impl TestOperation for IterateOnce {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let mut cursor = test_runner.take_cursor(id).await;
            match &mut cursor {
                TestCursor::Normal(cursor) => {
                    let mut cursor = cursor.lock().await;
                    cursor.try_advance().await?;
                }
                TestCursor::Session { cursor, session_id } => {
                    cursor
                        .try_advance(
                            test_runner
                                .entities
                                .write()
                                .await
                                .get_mut(session_id)
                                .unwrap()
                                .as_mut_session(),
                        )
                        .await?;
                }
                TestCursor::ChangeStream(change_stream) => {
                    let mut change_stream = change_stream.lock().await;
                    change_stream.next_if_any().await?;
                }
                TestCursor::Closed => panic!("Attempted to call IterateOnce on a closed cursor"),
            }
            test_runner.return_cursor(id, cursor).await;
            Ok(None)
        }
        .boxed()
    }
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct IterateUntilDocumentOrError {}

impl TestOperation for IterateUntilDocumentOrError {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            // A `SessionCursor` also requires a `&mut Session`, which would cause conflicting
            // borrows, so take the cursor from the map and return it after execution instead.
            let mut cursor = test_runner.take_cursor(id).await;
            let next = match &mut cursor {
                TestCursor::Normal(cursor) => {
                    let mut cursor = cursor.lock().await;
                    cursor.next().await
                }
                TestCursor::Session { cursor, session_id } => {
                    cursor
                        .next(
                            test_runner
                                .entities
                                .write()
                                .await
                                .get_mut(session_id)
                                .unwrap()
                                .as_mut_session(),
                        )
                        .await
                }
                TestCursor::ChangeStream(stream) => {
                    let mut stream = stream.lock().await;
                    stream.next().await.map(|res| {
                        res.map(|ev| match bson::to_bson(&ev) {
                            Ok(Bson::Document(doc)) => doc,
                            _ => panic!("invalid serialization result"),
                        })
                    })
                }
                TestCursor::Closed => None,
            };
            test_runner.return_cursor(id, cursor).await;
            next.transpose()
                .map(|opt| opt.map(|doc| Entity::Bson(Bson::Document(doc))))
        }
        .boxed()
    }

    fn returns_root_documents(&self) -> bool {
        true
    }
}

