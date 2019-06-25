use bson::{Bson, Document};

use super::{Outcome, TestFile};
use mongodb::{collation::Collation, options::DeleteOptions};

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

#[function_name]
fn run_delete_one_test(test_file: TestFile) {
    let data = test_file.data;

    for mut test_case in test_file.tests {
        if test_case.operation.name != "deleteOne" {
            continue;
        }

        test_case.description = test_case.description.replace('$', "%");

        let coll = crate::init_db_and_coll(function_name!(), &test_case.description);
        coll.insert_many(data.clone(), None)
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<ResultDoc> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                crate::get_coll(function_name!(), name)
                    .drop()
                    .expect(&test_case.description);
            }
        }

        let opts = DeleteOptions {
            collation: arguments.collation,
        };

        let result = coll
            .delete_one(arguments.filter, Some(opts))
            .expect(&test_case.description);

        assert_eq!(
            outcome.result.deleted_count, result.deleted_count,
            "{}",
            test_case.description
        );

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
    crate::spec::test(&["crud", "v1", "write"], run_delete_one_test);
}
