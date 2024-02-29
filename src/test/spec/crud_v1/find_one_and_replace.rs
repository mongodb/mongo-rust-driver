use std::cmp;

use serde::Deserialize;

use super::{run_crud_v1_test, Outcome, TestFile};
use crate::{
    bson::{Bson, Document},
    options::{Collation, FindOneAndReplaceOptions, ReturnDocument},
    test::util::TestClient,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub filter: Document,
    pub replacement: Document,
    pub bypass_document_validation: Option<bool>,
    pub projection: Option<Document>,
    pub return_document: Option<String>,
    pub sort: Option<Document>,
    pub upsert: Option<bool>,
    pub collation: Option<Collation>,
}

#[function_name::named]
async fn run_find_one_and_replace_test(test_file: TestFile) {
    let client = TestClient::new().await;
    let data = test_file.data;

    for mut test_case in test_file.tests {
        if test_case.operation.name != "findOneAndReplace" {
            continue;
        }

        test_case.description = test_case.description.replace('$', "%");
        let sub = cmp::min(test_case.description.len(), 50);
        let coll = client
            .init_db_and_coll(function_name!(), &test_case.description[..sub])
            .await;
        coll.insert_many(data.clone(), None)
            .await
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<Option<Document>> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                client.drop_collection(function_name!(), name).await;
            }
        }
        let new = match arguments.return_document.as_ref() {
            Some(s) if s == "Before" => Some(ReturnDocument::Before),
            Some(s) if s == "After" => Some(ReturnDocument::After),
            _ => None,
        };

        let options = FindOneAndReplaceOptions {
            bypass_document_validation: arguments.bypass_document_validation,
            projection: arguments.projection,
            return_document: new,
            sort: arguments.sort,
            upsert: arguments.upsert,
            collation: arguments.collation,
            ..Default::default()
        };

        let result = coll
            .find_one_and_replace(arguments.filter, arguments.replacement, options)
            .await
            .expect(&test_case.description);
        assert_eq!(
            result, outcome.result,
            "{}
        ",
            test_case.description
        );

        if let Some(c) = outcome.collection {
            let outcome_coll = match c.name {
                Some(ref name) => client.get_coll(function_name!(), name),
                None => coll,
            };

            assert_eq!(
                c.data,
                super::find_all(&outcome_coll).await,
                "{}",
                test_case.description
            );
        }
    }
}

#[tokio::test]
async fn run() {
    run_crud_v1_test(&["crud", "v1", "write"], run_find_one_and_replace_test).await;
}
