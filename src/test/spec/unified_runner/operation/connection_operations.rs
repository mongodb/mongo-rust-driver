use serde::Deserialize;
use crate::test::spec::unified_runner::operation::TestOperation;
use crate::test::spec::unified_runner::{Entity, TestCursor, TestRunner};
use futures::future::BoxFuture;
use crate::error::Result;
use futures_util::FutureExt;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Close {}

impl TestOperation for Close {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let mut entities = test_runner.entities.write().await;
            let target_entity = entities.get(id).unwrap();
            match target_entity {
                Entity::Client(_) => {
                    let client = entities.get_mut(id).unwrap().as_mut_client();
                    let closed_client_topology_id = client.topology_id;
                    client
                        .client
                        .take()
                        .unwrap()
                        .shutdown()
                        .immediate(true)
                        .await;

                    let mut entities_to_remove = vec![];
                    for (key, value) in entities.iter() {
                        match value {
                            // skip clients so that we don't remove the client entity itself from
                            // the map: we want to preserve it so we can
                            // access the other data stored on the entity.
                            Entity::Client(_) => {}
                            _ => {
                                if value.client_topology_id().await
                                    == Some(closed_client_topology_id)
                                {
                                    entities_to_remove.push(key.clone());
                                }
                            }
                        }
                    }
                    for entity_id in entities_to_remove {
                        entities.remove(&entity_id);
                    }

                    Ok(None)
                }
                Entity::Cursor(_) => {
                    let cursor = entities.get_mut(id).unwrap().as_mut_cursor();
                    let rx = cursor.make_kill_watcher().await;
                    *cursor = TestCursor::Closed;
                    drop(entities);
                    let _ = rx.await;
                    Ok(None)
                }
                _ => panic!(
                    "Unsupported entity {:?} for close operation; expected Client or Cursor",
                    target_entity
                ),
            }
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertNumberConnectionsCheckedOut {
    client: String,
    connections: u32,
}

impl TestOperation for AssertNumberConnectionsCheckedOut {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let client = test_runner.get_client(&self.client).await;
            client.sync_workers().await;
            assert_eq!(client.connections_checked_out(), self.connections);
        }
        .boxed()
    }
}
