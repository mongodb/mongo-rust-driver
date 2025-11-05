use crate::test::{
    spec::unified_runner::{
        operation::{with_mut_session, TestOperation},
        Entity,
        TestRunner,
    },
    util::fail_point::FailPoint,
};
use futures::future::BoxFuture;
use futures_util::FutureExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FailPointCommand {
    fail_point: FailPoint,
    client: String,
}

impl TestOperation for FailPointCommand {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let client = test_runner.get_client(&self.client).await;
            let guard = client
                .enable_fail_point(self.fail_point.clone())
                .await
                .unwrap();
            test_runner.fail_point_guards.write().await.push(guard);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct TargetedFailPoint {
    fail_point: FailPoint,
    session: String,
}

impl TestOperation for TargetedFailPoint {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let selection_criteria =
                with_mut_session!(test_runner, self.session.as_str(), |session| async {
                    session
                        .transaction
                        .pinned_mongos()
                        .cloned()
                        .unwrap_or_else(|| panic!("ClientSession not pinned"))
                })
                .await;
            let guard = test_runner
                .internal_client
                .enable_fail_point(
                    self.fail_point
                        .clone()
                        .selection_criteria(selection_criteria),
                )
                .await
                .unwrap();
            test_runner.fail_point_guards.write().await.push(guard);
        }
        .boxed()
    }
}
