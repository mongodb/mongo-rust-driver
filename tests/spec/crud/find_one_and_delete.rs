use bson::{Bson, Document};
use mongodb::options::{Collation, FindOneAndDeleteOptions};
use serde::Deserialize;

use super::{Outcome, TestFile};
use crate::CLIENT;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub filter: Document,
    pub projection: Option<Document>,
    pub sort: Option<Document>,
    pub collation: Option<Collation>,
}

#[function_name::named]
fn run_find_one_and_delete_test(test_file: TestFile) {
    let data = test_file.data;

    for mut test_case in test_file.tests {
        if test_case.operation.name != "findOneAndDelete" {
            continue;
        }

        test_case.description = test_case.description.replace('$', "%");

        let coll = CLIENT.init_db_and_coll(function_name!(), &test_case.description);
        coll.insert_many(data.clone(), None)
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<Option<Document>> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                CLIENT.drop_collection(function_name!(), name);
            }
        }

        let options = FindOneAndDeleteOptions {
            projection: arguments.projection,
            sort: arguments.sort,
            collation: arguments.collation,
            ..Default::default()
        };

        let result = coll
            .find_one_and_delete(arguments.filter, Some(options))
            .expect(&test_case.description);
        assert_eq!(result, outcome.result, "{}", test_case.description);

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
    crate::spec::test(&["crud", "v1", "write"], run_find_one_and_delete_test);
}
