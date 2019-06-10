use bson::{Bson, Document};
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument};

use super::{Outcome, TestFile};

use std::cmp;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub filter: Document,
    pub update: Document,
    pub array_filters: Option<Vec<Document>>,
    pub bypass_document_validation: Option<bool>,
    pub projection: Option<Document>,
    pub return_document: Option<String>,
    pub sort: Option<Document>,
    pub upsert: Option<bool>,
}

fn run_find_one_and_update_test(test_file: TestFile) {
    let data = test_file.data;

    for mut test_case in test_file.tests {
        if test_case.operation.name != "findOneAndUpdate"
            || test_case.description.contains("collation")
        {
            continue;
        }

        test_case.description = test_case.description.replace('$', "%");
        let sub = cmp::min(test_case.description.len(), 50);
        let coll = crate::init_db_and_coll("findOneAndUpdate", &test_case.description[..sub]);
        coll.insert_many(data.clone(), None)
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<Option<Document>> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                crate::get_coll("findOneAndUpdate", name)
                    .drop()
                    .expect(&test_case.description);
            }
        }

        let new = match arguments.return_document.as_ref() {
            Some(s) if s == "Before" => Some(ReturnDocument::Before),
            Some(s) if s == "After" => Some(ReturnDocument::After),
            _ => None,
        };

        let options = FindOneAndUpdateOptions {
            bypass_document_validation: arguments.bypass_document_validation,
            array_filters: arguments.array_filters,
            projection: arguments.projection,
            return_document: new,
            sort: arguments.sort,
            upsert: arguments.upsert,
            ..Default::default()
        };

        let result = coll
            .find_one_and_update(arguments.filter, arguments.update, Some(options))
            .expect(&test_case.description);
        assert_eq!(
            result, outcome.result,
            "{}
        ",
            test_case.description
        );

        if let Some(c) = outcome.collection {
            let outcome_coll = match c.name {
                Some(ref name) => crate::get_coll("findOneAndUpdate", name),
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
    crate::spec::test(&["crud", "v1", "write"], run_find_one_and_update_test);
}
