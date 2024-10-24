use std::time::Duration;

use crate::{
    runtime,
    test::spec::unified_runner::{operation::TestOperation, ExpectedEvent, TestRunner},
    ServerType,
};
use futures::future::BoxFuture;
use futures_util::FutureExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WaitForEvent {
    client: String,
    event: ExpectedEvent,
    count: usize,
}

impl TestOperation for WaitForEvent {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async {
            let client = test_runner.get_client(self.client.as_str()).await;
            let entities = test_runner.entities.clone();
            client
                .wait_for_matching_events(&self.event, self.count, entities)
                .await
                .unwrap();
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WaitForPrimaryChange {
    client: String,
    prior_topology_description: String,
    #[serde(rename = "timeoutMS")]
    timeout_ms: Option<u64>,
}

impl TestOperation for WaitForPrimaryChange {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let client = test_runner.get_client(&self.client).await;
            let td = test_runner
                .get_topology_description(&self.prior_topology_description)
                .await;
            let old_primary = td.servers_with_type(&[ServerType::RsPrimary]).next();
            let timeout = Duration::from_millis(self.timeout_ms.unwrap_or(10_000));

            runtime::timeout(timeout, async {
                let mut watcher = client.topology().watch();

                loop {
                    let latest = watcher.observe_latest();
                    if let Some(primary) = latest.description.primary() {
                        if Some(primary) != old_primary {
                            return;
                        }
                    }
                    watcher.wait_for_update(None).await;
                }
            })
            .await
            .unwrap();
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Wait {
    ms: u64,
}

impl TestOperation for Wait {
    fn execute_test_runner_operation<'a>(
        &'a self,
        _test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        tokio::time::sleep(Duration::from_millis(self.ms)).boxed()
    }
}
