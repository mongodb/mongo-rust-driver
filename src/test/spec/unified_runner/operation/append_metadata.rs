use futures_util::FutureExt;
use serde::Deserialize;

use super::TestOperation;
use crate::options::DriverInfo;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AppendMetadata {
    driver_info_options: DriverInfo,
}

impl TestOperation for AppendMetadata {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a crate::test::spec::unified_runner::TestRunner,
    ) -> futures::future::BoxFuture<
        'a,
        crate::error::Result<Option<crate::test::spec::unified_runner::Entity>>,
    > {
        async move {
            let client = test_runner.get_client(id).await;
            client.append_metadata(self.driver_info_options.clone())?;
            Ok(None)
        }
        .boxed()
    }
}
