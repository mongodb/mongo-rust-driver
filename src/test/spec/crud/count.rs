use bson::{Bson, Document};
use serde::Deserialize;

use super::{Outcome, TestFile};
use crate::{
    options::{Collation, CountOptions},
    test::{run_spec_test, util::TestClient, LOCK},
    RUNTIME,
};

#[derive(Debug, Deserialize)]
struct Arguments {
    pub filter: Option<Document>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
    pub collation: Option<Collation>,
}

#[function_name::named]
fn run_count_test(test_file: TestFile) {
    let client = RUNTIME.block_on(TestClient::new());
    let data = test_file.data;

    for mut test_case in test_file.tests {
        let lower_description = test_case.description.to_lowercase();

        // old `count` not implemented, collation not implemented
        if !test_case.operation.name.contains("count") || lower_description.contains("deprecated") {
            continue;
        }

        let _guard = LOCK.run_concurrently();

        test_case.description = test_case.description.replace('$', "%");

        let coll = client.init_db_and_coll(function_name!(), &test_case.description);

        if !data.is_empty() {
            coll.insert_many(data.clone(), None)
                .expect(&test_case.description);
        }

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<i64> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                client.drop_collection(function_name!(), name);
            }
        }

        let result = match test_case.operation.name.as_str() {
            "countDocuments" => {
                let options = CountOptions::builder()
                    .skip(arguments.skip)
                    .limit(arguments.limit)
                    .collation(arguments.collation)
                    .build();
                coll.count_documents(arguments.filter.unwrap_or_default(), options)
            }
            "estimatedDocumentCount" => coll.estimated_document_count(None),
            other => panic!("unexpected count operation: {}", other),
        }
        .expect(&test_case.description);

        assert_eq!(result, outcome.result, "{}", test_case.description);
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(core_threads = 2))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    run_spec_test(&["crud", "v1", "read"], run_count_test);
}
