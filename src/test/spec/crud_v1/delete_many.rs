use serde::Deserialize;

use super::{run_crud_v1_test, Outcome, TestFile};
use crate::{
    action::Action,
    bson::{Bson, Document},
    options::Collation,
    test::util::TestClient,
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
    pub deleted_count: u64,
}

#[function_name::named]
async fn run_delete_many_test(test_file: TestFile) {
    let client = TestClient::new().await;
    let data = test_file.data;

    for mut test_case in test_file.tests {
        if test_case.operation.name != "deleteMany" {
            continue;
        }

        test_case.description = test_case.description.replace('$', "%");

        let coll = client
            .init_db_and_coll(function_name!(), &test_case.description)
            .await;
        coll.insert_many(data.clone(), None)
            .await
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<ResultDoc> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                client.drop_collection(function_name!(), name).await;
            }
        }

        let result = coll
            .delete_many(arguments.filter)
            .optional(arguments.collation, |a, c| a.collation(c))
            .await
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
                super::find_all(&outcome_coll).await,
                "{}",
                test_case.description
            );
        }
    }
}

#[tokio::test]
async fn run() {
    run_crud_v1_test(&["crud", "v1", "write"], run_delete_many_test).await;
}
