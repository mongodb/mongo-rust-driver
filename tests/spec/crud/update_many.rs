use bson::{Bson, Document};
use mongodb::options::UpdateOptions;

use super::{Outcome, TestFile};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub filter: Document,
    pub update: Document,
    pub upsert: Option<bool>,
    pub array_filters: Option<Vec<Document>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultDoc {
    pub matched_count: i64,
    pub modified_count: i64,
    pub upserted_count: Option<i64>,
    pub upserted_id: Option<Bson>,
}

#[function_name]
fn run_update_many_test(test_file: TestFile) {
    let data = test_file.data;

    for mut test_case in test_file.tests {
        if test_case.operation.name != "updateMany" || test_case.description.contains("collation") {
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

        let options = UpdateOptions {
            upsert: arguments.upsert,
            array_filters: arguments.array_filters,
            ..Default::default()
        };

        let result = coll
            .update_many(arguments.filter, arguments.update, Some(options))
            .expect(&test_case.description);
        assert_eq!(
            outcome.result.matched_count, result.matched_count,
            "{}",
            test_case.description
        );
        assert_eq!(
            outcome.result.modified_count, result.modified_count,
            "{}",
            test_case.description
        );

        assert_eq!(
            outcome.result.upserted_count.unwrap_or(0),
            if result.upserted_id.is_some() { 1 } else { 0 },
            "{}",
            test_case.description
        );
        assert_eq!(
            outcome.result.upserted_id, result.upserted_id,
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
    crate::spec::test(&["crud", "v1", "write"], run_update_many_test);
}
