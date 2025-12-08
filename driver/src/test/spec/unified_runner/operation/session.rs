use crate::{
    bson::Bson,
    client::session::TransactionState,
    error::Result,
    test::spec::unified_runner::{
        operation::{with_mut_session, TestOperation},
        Entity,
        TestRunner,
    },
};
use futures::future::BoxFuture;
use futures_util::FutureExt;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct EndSession {}

impl TestOperation for EndSession {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            match test_runner.entities.write().await.get_mut(id) {
                Some(Entity::Session(session)) => session.client_session.take(),
                e => panic!("expected session for {id:?}, got {e:?}"),
            };
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertSessionTransactionState {
    session: String,
    state: String,
}

impl TestOperation for AssertSessionTransactionState {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let session_state =
                with_mut_session!(test_runner, self.session.as_str(), |session| async {
                    match &session.transaction.state {
                        TransactionState::None => "none",
                        TransactionState::Starting => "starting",
                        TransactionState::InProgress => "in_progress",
                        TransactionState::Committed { data_committed: _ } => "committed",
                        TransactionState::Aborted => "aborted",
                    }
                })
                .await;
            assert_eq!(session_state, self.state);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertSessionPinned {
    session: String,
}

impl TestOperation for AssertSessionPinned {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let is_pinned =
                with_mut_session!(test_runner, self.session.as_str(), |session| async {
                    session.transaction.is_pinned()
                })
                .await;
            assert!(is_pinned);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertSessionUnpinned {
    session: String,
}

impl TestOperation for AssertSessionUnpinned {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let is_pinned = with_mut_session!(test_runner, self.session.as_str(), |session| {
                async move { session.transaction.pinned_mongos().is_some() }
            })
            .await;
            assert!(!is_pinned);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertDifferentLsidOnLastTwoCommands {
    client: String,
}

impl TestOperation for AssertDifferentLsidOnLastTwoCommands {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let entities = test_runner.entities.read().await;
            let client = entities.get(&self.client).unwrap().as_client();
            let events = client.get_all_command_started_events();

            let lsid1 = events[events.len() - 1].command.get("lsid").unwrap();
            let lsid2 = events[events.len() - 2].command.get("lsid").unwrap();
            assert_ne!(lsid1, lsid2);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertSameLsidOnLastTwoCommands {
    client: String,
}

impl TestOperation for AssertSameLsidOnLastTwoCommands {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let entities = test_runner.entities.read().await;
            let client = entities.get(&self.client).unwrap().as_client();
            client.sync_workers().await;
            let events = client.get_all_command_started_events();

            let lsid1 = events[events.len() - 1].command.get("lsid").unwrap();
            let lsid2 = events[events.len() - 2].command.get("lsid").unwrap();
            assert_eq!(lsid1, lsid2);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertSessionDirty {
    session: String,
}

impl TestOperation for AssertSessionDirty {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let dirty = with_mut_session!(test_runner, self.session.as_str(), |session| {
                async move { session.is_dirty() }.boxed()
            })
            .await;
            assert!(dirty);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertSessionNotDirty {
    session: String,
}

impl TestOperation for AssertSessionNotDirty {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let dirty = with_mut_session!(test_runner, self.session.as_str(), |session| {
                async move { session.is_dirty() }
            })
            .await;
            assert!(!dirty);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct GetSnapshotTime {}

impl TestOperation for GetSnapshotTime {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            with_mut_session!(test_runner, id, |session| {
                async move {
                    session
                        .snapshot_time()
                        .map(|option| option.map(|ts| Bson::Timestamp(ts).into()))
                }
            })
            .await
        }
        .boxed()
    }
}
