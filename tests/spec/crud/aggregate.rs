use bson::{Bson, Document};
use mongodb::options::AggregateOptions;

use super::{Outcome, TestFile};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub pipeline: Vec<Document>,
    pub batch_size: Option<i32>,
}

fn run_aggregate_test(test_file: TestFile) {
    let data = test_file.data;

    for mut test_case in test_file.tests {
        if test_case.operation.name != "aggregate"
            || test_case.description.contains("collation")
            || test_case.operation.object != Some("database".to_string())
        {
            continue;
        }

        test_case.description = test_case.description.replace('$', "%");

        let coll = crate::init_db_and_coll("aggregate", &test_case.description);
        coll.insert_many(data.clone(), None)
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<Vec<Document>> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                crate::get_coll("aggregate", name)
                    .drop()
                    .expect(&test_case.description);
            }
        }

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
            let cursor = coll
                .aggregate(arguments.pipeline, Some(options))
                .expect(&test_case.description);
            assert_eq!(
                if out { Vec::new() } else { outcome.result },
                cursor.map(Result::unwrap).collect::<Vec<_>>(),
                "{}",
                test_case.description,
            );
        }

        if let Some(c) = outcome.collection {
            let outcome_coll = match c.name {
                Some(ref name) => crate::get_coll("aggregate", name),
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
    crate::spec::test(&["crud", "v1", "read"], run_aggregate_test);
}
