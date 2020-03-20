use bson::{Bson, Document};
use serde::Deserialize;

use super::{Outcome, TestFile};
use crate::{
    options::{Collation, DeleteOptions},
    test::{run_spec_test, util::TestClient, LOCK},
    RUNTIME,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub filter: Document,
    pub collation: Option<Collation>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultDoc {
    pub deleted_count: i64,
}

#[function_name::named]
fn run_delete_many_test(test_file: TestFile) {
    let client = RUNTIME.block_on(TestClient::new());
    let data = test_file.data;

    for mut test_case in test_file.tests {
        if test_case.operation.name != "deleteMany" {
            continue;
        }

        let _guard = LOCK.run_concurrently();

        test_case.description = test_case.description.replace('$', "%");

        let coll =
            RUNTIME.block_on(client.init_db_and_coll(function_name!(), &test_case.description));
        RUNTIME
            .block_on(coll.insert_many(data.clone(), None))
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<ResultDoc> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                RUNTIME.block_on(client.drop_collection(function_name!(), name));
            }
        }

        let options = DeleteOptions {
            collation: arguments.collation,
            ..Default::default()
        };

        let result = RUNTIME
            .block_on(coll.delete_many(arguments.filter, options))
            .expect(&test_case.description);

        assert_eq!(
            outcome.result.deleted_count, result.deleted_count,
            "{}",
            test_case.description
        );

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
    run_spec_test(&["crud", "v1", "write"], run_delete_many_test);
}
