use std::sync::Arc;

use serde::Deserialize;
use crate::test::spec::unified_runner::operation::TestOperation;
use crate::test::spec::unified_runner::TestRunner;
use crate::test::spec::unified_runner::Operation;
use futures::future::BoxFuture;
use futures_util::FutureExt;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RunOnThread {
    thread: String,
    operation: Arc<Operation>,
}

impl TestOperation for RunOnThread {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async {
            let thread = test_runner.get_thread(self.thread.as_str()).await;
            thread.run_operation(self.operation.clone());
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WaitForThread {
    thread: String,
}

impl TestOperation for WaitForThread {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async {
            let thread = test_runner.get_thread(self.thread.as_str()).await;
            thread.wait().await.unwrap_or_else(|e| {
                panic!("thread {:?} did not exit successfully: {}", self.thread, e)
            });
        }
        .boxed()
    }
}
