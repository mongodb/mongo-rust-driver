use serde::Deserialize;

use super::{run_crud_v1_test, Outcome, TestFile};
use crate::{
    bson::{Bson, Document},
    test::util::TestClient,
};

#[derive(Debug, Deserialize)]
struct Arguments {
    pub documents: Vec<Document>,
    pub options: Options,
}

#[derive(Debug, Deserialize)]
struct Options {
    pub ordered: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultDoc {
    inserted_ids: Option<Document>,
}

#[function_name::named]
async fn run_insert_many_test(test_file: TestFile) {
    let client = TestClient::new().await;
    let data = test_file.data;

    for test_case in test_file.tests {
        if test_case.operation.name != "insertMany" {
            continue;
        }

        let coll = client
            .init_db_and_coll(function_name!(), &test_case.description)
            .await;
        coll.insert_many(data.clone())
            .await
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<ResultDoc> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        let result = match coll
            .insert_many(arguments.documents)
            .ordered(arguments.options.ordered)
            .await
        {
            Ok(result) => {
                assert_ne!(outcome.error, Some(true), "{}", test_case.description);
                result.inserted_ids
            }
            Err(e) => {
                assert!(
                    outcome.error.unwrap_or(false),
                    "{}: expected no error, got {:?}",
                    test_case.description,
                    e
                );
                Default::default()
            }
        };

        if let Some(outcome_result_inserted_ids) = outcome.result.inserted_ids {
            let mut result_inserted_ids: Vec<_> = result
                .into_iter()
                .map(|(index, val)| (index.to_string(), val))
                .collect();
            result_inserted_ids.sort_by(|pair1, pair2| pair1.0.cmp(&pair2.0));

            let mut outcome_result_inserted_ids: Vec<_> =
                outcome_result_inserted_ids.into_iter().collect();
            outcome_result_inserted_ids.sort_by(|pair1, pair2| pair1.0.cmp(&pair2.0));

            assert_eq!(
                outcome_result_inserted_ids, result_inserted_ids,
                "{}",
                test_case.description
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

#[tokio::test]
async fn run() {
    run_crud_v1_test(&["crud", "v1", "write"], run_insert_many_test).await;
}
