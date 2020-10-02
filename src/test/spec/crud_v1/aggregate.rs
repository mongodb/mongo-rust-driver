use futures::stream::TryStreamExt;
use serde::Deserialize;
use tokio::sync::RwLockReadGuard;

use super::{Outcome, TestFile};
use crate::{
    bson::{Bson, Document},
    options::{AggregateOptions, Collation},
    test::{run_spec_test, util::TestClient, LOCK},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub pipeline: Vec<Document>,
    pub batch_size: Option<u32>,
    pub collation: Option<Collation>,
}

#[function_name::named]
async fn run_aggregate_test(test_file: TestFile) {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;
    let client = TestClient::new().await;

    let data = test_file.data;

    for test_case in test_file.tests {
        if test_case.operation.name != "aggregate" {
            continue;
        }

        let coll = client
            .init_db_and_coll(
                function_name!(),
                &test_case.description.replace('$', "%").replace(' ', "_"),
            )
            .await;
        coll.insert_many(data.clone(), None)
            .await
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<Option<Vec<Document>>> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                client.drop_collection(function_name!(), name).await;
            }
        }

        let options = AggregateOptions {
            batch_size: arguments.batch_size,
            collation: arguments.collation,
            ..Default::default()
        };

        {
            let cursor = coll
                .aggregate(arguments.pipeline, options)
                .await
                .expect(&test_case.description);

            let results = cursor
                .try_collect::<Vec<_>>()
                .await
                .expect(&test_case.description);

            assert_eq!(
                outcome.result.unwrap_or_default(),
                results,
                "{}",
                test_case.description,
            );
        }

        if let Some(c) = outcome.collection {
            let outcome_coll = match c.name {
                Some(ref name) => client.get_coll(function_name!(), name),
                None => coll,
            };

            assert_eq!(
                c.data,
                super::find_all(&outcome_coll).await,
                "{}",
                test_case.description
            );
        }
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    run_spec_test(&["crud", "v1", "read"], run_aggregate_test).await;
}
