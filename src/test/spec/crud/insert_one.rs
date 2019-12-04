use bson::{Bson, Document};
use serde::Deserialize;

use super::{Outcome, TestFile};
use crate::test::{run_spec_test, CLIENT, LOCK};

#[derive(Debug, Deserialize)]
struct Arguments {
    pub document: Document,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultDoc {
    inserted_id: Bson,
}

#[function_name::named]
fn run_insert_one_test(test_file: TestFile) {
    let data = test_file.data;

    for mut test_case in test_file.tests {
        if test_case.operation.name != "insertOne" {
            continue;
        }

        let _guard = LOCK.run_concurrently();

        test_case.description = test_case.description.replace('$', "%");

        let coll = CLIENT.init_db_and_coll(function_name!(), &test_case.description);
        coll.insert_many(data.clone(), None)
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<ResultDoc> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                CLIENT.drop_collection(function_name!(), name);
            }
        }

        let result = coll
            .insert_one(arguments.document, None)
            .expect(&test_case.description);
        assert_eq!(
            outcome.result.inserted_id, result.inserted_id,
            "{}",
            test_case.description
        );

        if let Some(c) = outcome.collection {
            let outcome_coll = match c.name {
                Some(ref name) => CLIENT.get_coll(function_name!(), name),
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

#[test]
fn run() {
    run_spec_test(&["crud", "v1", "write"], run_insert_one_test);
}
