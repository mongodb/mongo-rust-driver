use bson::{Bson, Document};
use mongodb::options::AggregateOptions;

use super::{Outcome, TestFile};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub pipeline: Vec<Document>,
    pub batch_size: Option<i32>,
}

fn db_aggregate_test(test_file: TestFile) {
    for mut test_case in test_file.tests {
        if test_case.operation.name != "aggregate"
            || test_case.description.contains("collation")
            || test_case.operation.object != Some("database".to_string())
        {
            continue;
        }

        test_case.description = test_case.description.replace('$', "%");

        let db = crate::get_db("db_aggregate");

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<Vec<Document>> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        let options = AggregateOptions {
            batch_size: arguments.batch_size,
            ..Default::default()
        };

        let out = arguments
            .pipeline
            .get(arguments.pipeline.len() - 1)
            .map(|doc| doc.contains_key("$out"))
            .unwrap_or(false);

        {
            let cursor = db
                .aggregate(arguments.pipeline, Some(options))
                .expect(&test_case.description);
            assert_eq!(
                if out { Vec::new() } else { outcome.result },
                cursor.map(Result::unwrap).collect::<Vec<_>>(),
                "{}",
                test_case.description,
            );
        }
    }
}

#[test]
fn run() {
    crate::spec::test(&["crud", "read"], db_aggregate_test);
}
