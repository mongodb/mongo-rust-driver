use bson::{Bson, Document};

use super::{Outcome, TestFile};
use mongodb::options::{collation::Collation, DistinctOptions};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub filter: Option<Document>,
    pub field_name: String,
    pub collation: Option<Collation>,
}

#[function_name]
fn run_distinct_test(test_file: TestFile) {
    let data = test_file.data;

    for mut test_case in test_file.tests {
        if test_case.operation.name != "distinct" {
            continue;
        }

        test_case.description = test_case.description.replace('$', "%");

        let coll = crate::init_db_and_coll(function_name!(), &test_case.description);
        coll.insert_many(data.clone(), None)
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<Vec<Bson>> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                crate::get_coll(function_name!(), name)
                    .drop()
                    .expect(&test_case.description);
            }
        }

        let opts = DistinctOptions {
            collation: arguments.collation,
            ..Default::default()
        };

        let result = coll
            .distinct(&arguments.field_name, arguments.filter, Some(opts))
            .expect(&test_case.description);
        assert_eq!(result, outcome.result, "{}", test_case.description);
    }
}

#[test]
fn run() {
    crate::spec::test(&["crud", "v1", "read"], run_distinct_test);
}
