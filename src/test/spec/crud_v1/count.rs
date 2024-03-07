use serde::Deserialize;

use super::{run_crud_v1_test, Outcome, TestFile};
use crate::{
    bson::{Bson, Document},
    options::{Collation, CountOptions},
    test::util::TestClient,
};

#[derive(Debug, Deserialize)]
struct Arguments {
    pub filter: Option<Document>,
    pub skip: Option<u64>,
    pub limit: Option<u64>,
    pub collation: Option<Collation>,
}

#[function_name::named]
async fn run_count_test(test_file: TestFile) {
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
            coll.insert_many(data.clone())
                .await
                .expect(&test_case.description);
        }

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<u64> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                client.drop_collection(function_name!(), name).await;
            }
        }

        let result = match test_case.operation.name.as_str() {
            "countDocuments" => {
                let options = CountOptions::builder()
                    .skip(arguments.skip)
                    .limit(arguments.limit)
                    .collation(arguments.collation)
                    .build();
                coll.count_documents(arguments.filter.unwrap_or_default())
                    .with_options(options)
                    .await
            }
            "estimatedDocumentCount" => coll.estimated_document_count().await,
            other => panic!("unexpected count operation: {}", other),
        }
        .expect(&test_case.description);

        assert_eq!(result, outcome.result, "{}", test_case.description);
    }
}

#[tokio::test]
async fn run() {
    run_crud_v1_test(&["crud", "v1", "read"], run_count_test).await;
}
