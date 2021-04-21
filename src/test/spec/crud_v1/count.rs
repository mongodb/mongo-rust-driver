use serde::Deserialize;
use tokio::sync::RwLockReadGuard;

use super::{Outcome, TestFile};
use crate::{
    bson::{Bson, Document},
    options::{Collation, CountOptions},
    test::{run_spec_test, util::TestClient, LOCK},
};

#[derive(Debug, Deserialize)]
struct Arguments {
    pub filter: Option<Document>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
    pub collation: Option<Collation>,
}

#[function_name::named]
async fn run_count_test(test_file: TestFile) {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    let data = test_file.data;

    for mut test_case in test_file.tests {
        let lower_description = test_case.description.to_lowercase();

        // old `count` not implemented, collation not implemented
        if !test_case.operation.name.contains("count") || lower_description.contains("deprecated") {
            continue;
        }

        test_case.description = test_case.description.replace('$', "%");

        let coll = client
            .init_db_and_coll(function_name!(), &test_case.description)
            .await;

        if !data.is_empty() {
            coll.insert_many(data.clone(), None)
                .await
                .expect(&test_case.description);
        }

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<i64> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                client.drop_collection(function_name!(), name).await;
            }
        }

        let result = match test_case.operation.name.as_str() {
            "countDocuments" => {
                let mut options = CountOptions::builder().build();
                options.skip = arguments.skip;
                options.limit = arguments.limit;
                options.collation = arguments.collation;
                coll.count_documents(arguments.filter.unwrap_or_default(), options)
                    .await
            }
            "estimatedDocumentCount" => coll.estimated_document_count(None).await,
            other => panic!("unexpected count operation: {}", other),
        }
        .expect(&test_case.description);

        assert_eq!(result, outcome.result, "{}", test_case.description);
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    run_spec_test(&["crud", "v1", "read"], run_count_test).await;
}
