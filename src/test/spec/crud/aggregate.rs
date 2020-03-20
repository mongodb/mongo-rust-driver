use bson::{Bson, Document};
use futures::stream::TryStreamExt;
use serde::Deserialize;

use super::{Outcome, TestFile};
use crate::{
    options::{AggregateOptions, Collation},
    test::{run_spec_test, util::TestClient, LOCK},
    RUNTIME,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub pipeline: Vec<Document>,
    pub batch_size: Option<u32>,
    pub collation: Option<Collation>,
}

#[function_name::named]
fn run_aggregate_test(test_file: TestFile) {
    let client = RUNTIME.block_on(TestClient::new());

    let data = test_file.data;

    for test_case in test_file.tests {
        if test_case.operation.name != "aggregate" {
            continue;
        }

        let _guard = LOCK.run_concurrently();

        let coll = client.init_db_and_coll(
            function_name!(),
            &test_case.description.replace('$', "%").replace(' ', "_"),
        );
        RUNTIME
            .block_on(coll.insert_many(data.clone(), None))
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<Option<Vec<Document>>> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                RUNTIME.block_on(client.drop_collection(function_name!(), name));
            }
        }

        let options = AggregateOptions {
            batch_size: arguments.batch_size,
            collation: arguments.collation,
            ..Default::default()
        };

        {
            let cursor = RUNTIME
                .block_on(coll.aggregate(arguments.pipeline, options))
                .expect(&test_case.description);

            assert_eq!(
                outcome.result.unwrap_or_default(),
                RUNTIME.block_on(cursor.try_collect::<Vec<_>>()).unwrap(),
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
                super::find_all(&outcome_coll),
                "{}",
                test_case.description
            );
        }
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(core_threads = 2))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    run_spec_test(&["crud", "v1", "read"], run_aggregate_test);
}
