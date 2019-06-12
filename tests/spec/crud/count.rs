use bson::{Bson, Document};
use mongodb::options::CountOptions;

use super::{Outcome, TestFile};

#[derive(Debug, Deserialize)]
struct Arguments {
    pub filter: Option<Document>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
}

fn run_count_test(test_file: TestFile) {
    let data = test_file.data;

    for mut test_case in test_file.tests {
        let lower_description = test_case.description.to_lowercase();

        // old `count` not implemented, collation not implemented
        if !lower_description.contains("count")
            || lower_description.contains("collation")
            || lower_description.contains("deprecated")
        {
            continue;
        }

        test_case.description = test_case.description.replace('$', "%");

        let coll = crate::init_db_and_coll("count", &test_case.description);
        coll.insert_many(data.clone(), None)
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<i64> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                crate::get_coll("count", name)
                    .drop()
                    .expect(&test_case.description);
            }
        }

        let result = match test_case.operation.name.as_str() {
            "countDocuments" => {
                let options = CountOptions::builder()
                    .skip(arguments.skip)
                    .limit(arguments.limit)
                    .build();
                coll.count_documents(arguments.filter, Some(options))
            }
            "estimatedDocumentCount" => coll.estimated_document_count(None),
            other => panic!("unexpected count operation: {}", other),
        }
        .expect(&test_case.description);

        assert_eq!(result, outcome.result, "{}", test_case.description);
    }
}

#[test]
fn run() {
    crate::spec::test(&["crud", "v1", "read"], run_count_test);
}
