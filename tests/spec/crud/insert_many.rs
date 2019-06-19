use bson::{Bson, Document};
use mongodb::options::InsertManyOptions;

use super::{Outcome, TestFile};

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

#[function_name]
fn run_insert_many_test(test_file: TestFile) {
    let data = test_file.data;

    for test_case in test_file.tests {
        if test_case.operation.name != "insertMany" || test_case.description.contains("collation") {
            continue;
        }

        let coll = crate::init_db_and_coll(function_name!(), &test_case.description);
        coll.insert_many(data.clone(), None)
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<ResultDoc> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        let options = InsertManyOptions::builder()
            .ordered(arguments.options.ordered)
            .build();

        let result = coll
            .insert_many(arguments.documents, Some(options))
            .expect(&test_case.description);

        if let Some(outcome_result_inserted_ids) = outcome.result.inserted_ids {
            let mut result_inserted_ids: Vec<_> = result
                .inserted_ids
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
                Some(ref name) => crate::get_coll(function_name!(), name),
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
    crate::spec::test(&["crud", "v1", "write"], run_insert_many_test);
}
