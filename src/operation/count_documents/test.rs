use crate::{
    bson::doc,
    bson_util,
    cmap::{CommandResponse, StreamDescription},
    coll::Namespace,
    concern::ReadConcern,
    operation::{test, Operation},
    options::{CountOptions, Hint},
};

use super::CountDocuments;

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let count_op = CountDocuments::new(ns, Some(doc! { "x": 1 }), None);
    let mut count_command = count_op
        .build(&StreamDescription::new_testing())
        .expect("error on build");

    let mut expected_body = doc! {
        "aggregate": "test_coll",
        "pipeline": [
            { "$match": { "x": 1 } },
            { "$group": { "_id": 1, "n": { "$sum": 1 } } },
        ],
        "cursor": { }
    };

    bson_util::sort_document(&mut expected_body);
    bson_util::sort_document(&mut count_command.body);

    assert_eq!(count_command.body, expected_body);
    assert_eq!(count_command.target_db, "test_db");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn build_with_options() {
    let skip = 2;
    let limit = 5;
    let options = CountOptions::builder()
        .skip(skip)
        .limit(limit)
        .hint(Hint::Name("_id_1".to_string()))
        .read_concern(ReadConcern::available())
        .build();
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let count_op = CountDocuments::new(ns, None, Some(options));
    let mut count_command = count_op
        .build(&StreamDescription::new_testing())
        .expect("error on build");

    let mut expected_body = doc! {
        "aggregate": "test_coll",
        "pipeline": [
            { "$match": {} },
            { "$skip": skip },
            { "$limit": limit },
            { "$group": { "_id": 1, "n": { "$sum": 1 } } },
        ],
        "hint": "_id_1",
        "cursor": { },
        "readConcern": { "level": "available" },
    };

    bson_util::sort_document(&mut expected_body);
    bson_util::sort_document(&mut count_command.body);

    assert_eq!(count_command.body, expected_body);
    assert_eq!(count_command.target_db, "test_db");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn op_selection_criteria() {
    test::op_selection_criteria(|selection_criteria| {
        let options = CountOptions {
            selection_criteria,
            ..Default::default()
        };
        CountDocuments::new(Namespace::empty(), None, Some(options))
    });
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn handle_success() {
    let ns = Namespace {
        db: "test_db".to_string(),
        coll: "test_coll".to_string(),
    };
    let count_op = CountDocuments::new(ns, None, None);

    let n = 26;
    let response = CommandResponse::with_document(doc! {
        "cursor" : {
            "firstBatch" : [
                {
                    "_id" : 1,
                    "n" : n
                }
            ],
            "id" : 0,
            "ns" : "test_db.test_coll"
        },
        "ok" : 1
    });

    let actual_values = count_op
        .handle_response(response, &Default::default())
        .expect("supposed to succeed");

    assert_eq!(actual_values, n);
}
