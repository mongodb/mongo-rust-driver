use bson::{Bson, Document};
use serde::Deserialize;

use super::{Outcome, TestFile};
use crate::{
    options::{Collation, FindOptions},
    test::{run_spec_test, util::TestClient, LOCK},
    RUNTIME,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub filter: Document,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
    pub batch_size: Option<u32>,
    pub collation: Option<Collation>,
}

#[function_name::named]
fn run_find_test(test_file: TestFile) {
    let client = RUNTIME.block_on(TestClient::new());
    let data = test_file.data;

    for mut test_case in test_file.tests {
        if test_case.operation.name != "find" {
            continue;
        }

        let _guard = LOCK.run_concurrently();

        test_case.description = test_case.description.replace('$', "%");

        let coll = client.init_db_and_coll(function_name!(), &test_case.description);
        coll.insert_many(data.clone(), None)
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<Vec<Document>> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                client.drop_collection(function_name!(), name);
            }
        }

        let options = FindOptions {
            skip: arguments.skip,
            limit: arguments.limit,
            batch_size: arguments.batch_size,
            collation: arguments.collation,
            ..Default::default()
        };

        let cursor = coll
            .find(arguments.filter, options)
            .expect(&test_case.description);
        assert_eq!(
            outcome.result,
            cursor.map(Result::unwrap).collect::<Vec<Document>>(),
            "{}",
            test_case.description,
        );
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(core_threads = 2))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    run_spec_test(&["crud", "v1", "read"], run_find_test);
}
