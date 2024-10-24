use serde::Deserialize;
use crate::test::spec::unified_runner::operation::TestOperation;
use crate::test::spec::unified_runner:: TestRunner;
use crate::TopologyType;
use futures::future::BoxFuture;
use futures_util::FutureExt;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RecordTopologyDescription {
    id: String,
    client: String,
}

impl TestOperation for RecordTopologyDescription {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async {
            let client = test_runner.get_client(&self.client).await;
            let description = client.topology_description();
            test_runner.insert_entity(&self.id, description).await;
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertTopologyType {
    topology_description: String,
    topology_type: TopologyType,
}

impl TestOperation for AssertTopologyType {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async {
            let td = test_runner
                .get_topology_description(&self.topology_description)
                .await;
            assert_eq!(td.topology_type, self.topology_type);
        }
        .boxed()
    }
}
