use serde::Deserialize;

use super::{Outcome, TestFile};
use crate::{
    bson::{Bson, Document},
    options::{Collation, ReplaceOptions},
    test::{run_spec_test, util::TestClient, LOCK},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    pub filter: Document,
    pub replacement: Document,
    pub upsert: Option<bool>,
    pub collation: Option<Collation>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultDoc {
    pub matched_count: i64,
    pub modified_count: i64,
    pub upserted_count: Option<i64>,
    pub upserted_id: Option<Bson>,
}

#[function_name::named]
async fn run_replace_one_test(test_file: TestFile) {
    let client = TestClient::new().await;
    let data = test_file.data;

    for test_case in test_file.tests {
        if test_case.operation.name != "replaceOne" {
            continue;
        }

        let _guard = LOCK.run_concurrently().await;

        let coll = client
            .init_db_and_coll(
                function_name!(),
                &test_case.description.replace('$', "%").replace(' ', "_"),
            )
            .await;
        coll.insert_many(data.clone(), None)
            .await
            .expect(&test_case.description);

        let arguments: Arguments = bson::from_bson(Bson::Document(test_case.operation.arguments))
            .expect(&test_case.description);
        let outcome: Outcome<ResultDoc> =
            bson::from_bson(Bson::Document(test_case.outcome)).expect(&test_case.description);

        if let Some(ref c) = outcome.collection {
            if let Some(ref name) = c.name {
                client.drop_collection(function_name!(), name).await;
            }
        }

        let options = ReplaceOptions {
            upsert: arguments.upsert,
            collation: arguments.collation,
            ..Default::default()
        };

        let result = coll
            .replace_one(arguments.filter, arguments.replacement, options)
            .await
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    run_spec_test(&["crud", "v1", "write"], run_replace_one_test).await;
}
