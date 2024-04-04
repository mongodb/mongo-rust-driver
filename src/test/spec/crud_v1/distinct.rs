use serde::Deserialize;

use super::{run_crud_v1_test, Outcome, TestFile};
use crate::{
    bson::{Bson, Document},
    options::{Collation, DistinctOptions},
    test::util::TestClient,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub filter: Option<Document>,
    pub field_name: String,
    pub collation: Option<Collation>,
}

#[function_name::named]
async fn run_distinct_test(test_file: TestFile) {
    let client = TestClient::new().await;
    let data = test_file.data;

    for mut test_case in test_file.tests {
        if test_case.operation.name != "distinct" {
            continue;
        }

        test_case.description = test_case.description.replace('$', "%");

        let coll = client
            .init_db_and_coll(function_name!(), &test_case.description)
            .await;
        coll.insert_many(data.clone())
            .await
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<Vec<Bson>> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                client.drop_collection(function_name!(), name).await;
            }
        }

        let opts = DistinctOptions {
            collation: arguments.collation,
            ..Default::default()
        };

        let result = coll
            .distinct(&arguments.field_name, arguments.filter.unwrap_or_default())
            .with_options(opts)
            .await
            .expect(&test_case.description);
        assert_eq!(result, outcome.result, "{}", test_case.description);
    }
}

#[tokio::test]
async fn run() {
    run_crud_v1_test(&["crud", "v1", "read"], run_distinct_test).await;
}
