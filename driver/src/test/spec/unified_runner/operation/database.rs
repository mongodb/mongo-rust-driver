use futures::FutureExt;
use serde::Deserialize;

use crate::{
    options::DropDatabaseOptions,
    test::spec::unified_runner::operation::{with_opt_session, TestOperation},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DropDatabase {
    database: String,
    #[serde(flatten)]
    options: DropDatabaseOptions,
    session: Option<String>,
}

impl TestOperation for DropDatabase {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a crate::test::spec::unified_runner::TestRunner,
    ) -> futures::prelude::future::BoxFuture<
        'a,
        crate::error::Result<Option<crate::test::spec::unified_runner::Entity>>,
    > {
        async move {
            let client = test_runner.get_client(id).await;
            let db = client.database(&self.database);
            with_opt_session!(
                test_runner,
                &self.session,
                db.drop().with_options(self.options.clone()),
            )
            .await?;
            Ok(None)
        }
        .boxed()
    }
}
