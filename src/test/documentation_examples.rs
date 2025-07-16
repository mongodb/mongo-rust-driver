mod aggregation_data;

use crate::bson::Document;
use futures::TryStreamExt;

use crate::{
    bson::{doc, Bson},
    error::Result,
    options::{ClientOptions, ServerApi, ServerApiVersion},
    test::{
        log_uncaptured,
        server_version_lt,
        server_version_matches,
        topology_is_load_balanced,
        topology_is_replica_set,
        topology_is_sharded,
        transactions_supported,
        DEFAULT_URI,
    },
    Client,
    Collection,
};

macro_rules! assert_coll_count {
    ($coll:expr, $expected:expr) => {
        assert_eq!($coll.count_documents(doc! {}).await.unwrap(), $expected);
    };
}

macro_rules! assert_cursor_count {
    ($cursor:expr, $expected:expr) => {{
        let docs: Vec<_> = $cursor.try_collect().await.unwrap();
        assert_eq!(docs.len(), $expected);
    }};
}

macro_rules! run_on_each_doc {
    ($cursor:expr, $name:ident, $check:block) => {{
        let mut cursor = $cursor;

        while let Some($name) = cursor.try_next().await.unwrap() $check;
    }};
}

async fn insert_examples(collection: &Collection<Document>) -> Result<()> {
    collection.drop().await?;

    // Start Example 1
    collection
        .insert_one(doc! {
            "item": "canvas",
            "qty": 100,
            "tags": ["cotton"],
            "size": {
                "h": 28,
                "w": 35.5,
                "uom": "cm",
            }
        })
        .await?;
    // End Example 1

    assert_coll_count!(collection, 1);

    // Start Example 2
    let cursor = collection.find(doc! { "item": "canvas" }).await?;
    // End Example 2

    assert_cursor_count!(cursor, 1);

    // Start Example 3
    let docs = vec![
        doc! {
            "item": "journal",
            "qty": 25,
            "tags": ["blank", "red"],
            "size": {
                "h": 14,
                "w": 21,
                "uom": "cm"
            }
        },
        doc! {
            "item": "mat",
            "qty": 85,
            "tags": ["gray"],
            "size":{
                "h": 27.9,
                "w": 35.5,
                "uom": "cm"
            }
        },
        doc! {
            "item": "mousepad",
            "qty": 25,
            "tags": ["gel", "blue"],
            "size": {
                "h": 19,
                "w": 22.85,
                "uom": "cm"
            }
        },
    ];

    collection.insert_many(docs).await?;
    // End Example 3

    assert_coll_count!(collection, 4);

    Ok(())
}

async fn query_top_level_fields_examples(collection: &Collection<Document>) -> Result<()> {
    collection.drop().await?;

    // Start Example 6
    let docs = vec![
        doc! {
            "item": "journal",
            "qty": 25,
            "size": {
                "h": 14,
                "w": 21,
                "uom": "cm"
            },
            "status": "A"
        },
        doc! {
            "item": "notebook",
            "qty": 50,
            "size": {
                "h": 8.5,
                "w": 11,
                "uom": "in"
            },
            "status": "A"
        },
        doc! {
            "item": "paper",
            "qty": 100,
            "size": {
                "h": 8.5,
                "w": 11,
                "uom": "in"
            },
            "status": "D"
        },
        doc! {
            "item": "planner",
            "qty": 75,
            "size": {
                "h": 22.85,
                "w": 30,
                "uom": "cm"
            },
            "status": "D"
        },
        doc! {
            "item": "postcard",
            "qty": 45,
            "size": {
                "h": 10,
                "w": 15.25,
                "uom": "cm"
            },
            "status": "A"
        },
    ];

    collection.insert_many(docs).await?;
    // End Example 6

    assert_coll_count!(collection, 5);

    // Start Example 7
    let cursor = collection.find(doc! {}).await?;
    // End Example 7

    assert_cursor_count!(cursor, 5);

    // Start Example 9
    let cursor = collection.find(doc! { "status": "D" }).await?;
    // End Example 9

    assert_cursor_count!(cursor, 2);

    // Start Example 10
    let cursor = collection
        .find(doc! {
            "status": {
                "$in": ["A", "D"],
            }
        })
        .await?;
    // End Example 10

    assert_cursor_count!(cursor, 5);

    // Start Example 11
    let cursor = collection
        .find(doc! {
            "status": "A",
            "qty": { "$lt": 30 },
        })
        .await?;
    // End Example 11

    assert_cursor_count!(cursor, 1);

    // Start Example 12
    let cursor = collection
        .find(doc! {
            "$or": [
                { "status": "A" },
                {
                    "qty": { "$lt": 30 },
                }
            ],
        })
        .await?;
    // End Example 12

    assert_cursor_count!(cursor, 3);

    // Start Example 13
    let cursor = collection
        .find(doc! {
            "status": "A",
            "$or": [
                {
                    "qty": { "$lt": 30 },
                },
                {
                    "item": { "$regex": "^p" },
                },
            ],
        })
        .await?;
    // End Example 13

    assert_cursor_count!(cursor, 2);

    Ok(())
}

async fn query_embedded_documents_examples(collection: &Collection<Document>) -> Result<()> {
    collection.drop().await?;

    // Start Example 14
    let docs = vec![
        doc! {
            "item": "journal",
            "qty": 25,
            "size": {
                "h": 14,
                "w": 21,
                "uom": "cm",
            },
            "status": "A",
        },
        doc! {
            "item": "notebook",
            "qty": 50,
            "size": {
                "h": 8.5,
                "w": 11,
                "uom": "in",
            },
            "status": "A",
        },
        doc! {
            "item": "paper",
            "qty": 100,
            "size": {
                "h": 8.5,
                "w": 11,
                "uom": "in",
            },
            "status": "D"
        },
        doc! {
            "item": "planner",
            "qty": 75,
            "size": {
                "h": 22.85,
                "w": 30,
                "uom": "cm",
            },
            "status": "D"
        },
        doc! {
            "item": "postcard",
            "qty": 45,
            "size": {
                "h": 10,
                "w": 15.25,
                "uom": "cm"
            },
            "status": "A",
        },
    ];

    collection.insert_many(docs).await?;
    // End Example 14

    assert_coll_count!(collection, 5);

    // Start Example 15
    let cursor = collection
        .find(doc! {
            "size": {
                "h": 14,
                "w": 21,
                "uom": "cm",
            },
        })
        .await?;
    // End Example 15

    assert_cursor_count!(cursor, 1);

    // Start Example 16
    let cursor = collection
        .find(doc! {
            "size": {
                "w": 21,
                "h": 14,
                "uom": "cm",
            },
        })
        .await?;
    // End Example 16

    assert_cursor_count!(cursor, 0);

    // Start Example 17
    let cursor = collection.find(doc! { "size.uom": "in" }).await?;
    // End Example 17

    assert_cursor_count!(cursor, 2);

    // Start Example 18
    let cursor = collection
        .find(doc! {
            "size.h": { "$lt": 15 },
        })
        .await?;
    // End Example 18

    assert_cursor_count!(cursor, 4);

    // Start Example 19
    let cursor = collection
        .find(doc! {
            "size.h": { "$lt": 15 },
            "size.uom": "in",
            "status": "D",
        })
        .await?;
    // End Example 19

    assert_cursor_count!(cursor, 1);

    Ok(())
}

async fn query_arrays_examples(collection: &Collection<Document>) -> Result<()> {
    collection.drop().await?;

    // Start Example 20
    let docs = vec![
        doc! {
            "item": "journal",
            "qty": 25,
            "tags": ["blank", "red"],
            "dim_cm": [14, 21],
        },
        doc! {
            "item": "notebook",
            "qty": 50,
            "tags": ["red", "blank"],
            "dim_cm": [14, 21],
        },
        doc! {
            "item": "paper",
            "qty": 100,
            "tags": ["red", "blank", "plain"],
            "dim_cm": [14, 21],
        },
        doc! {
            "item": "planner",
            "qty": 75,
            "tags": ["blank", "red"],
            "dim_cm": [22.85, 30],
        },
        doc! {
            "item": "postcard",
            "qty": 45,
            "tags": ["blue"],
            "dim_cm": [10, 15.25],
        },
    ];

    collection.insert_many(docs).await?;
    // End Example 20

    assert_coll_count!(collection, 5);

    // Start Example 21
    let cursor = collection
        .find(doc! {
            "tags": ["red", "blank"],
        })
        .await?;
    // End Example 21

    assert_cursor_count!(cursor, 1);

    // Start Example 22
    let cursor = collection
        .find(doc! {
            "tags": {
                "$all": ["red", "blank"],
            }
        })
        .await?;
    // End Example 22

    assert_cursor_count!(cursor, 4);

    // Start Example 23
    let cursor = collection
        .find(doc! {
            "tags": "red",
        })
        .await?;
    // End Example 23

    assert_cursor_count!(cursor, 4);

    // Start Example 24
    let cursor = collection
        .find(doc! {
            "dim_cm": { "$gt": 25 },
        })
        .await?;
    // End Example 24

    assert_cursor_count!(cursor, 1);

    // Start Example 25
    let cursor = collection
        .find(doc! {
            "dim_cm": {
                "$gt": 15,
                "$lt": 20,
            },
        })
        .await?;
    // End Example 25

    assert_cursor_count!(cursor, 4);

    // Start Example 26
    let cursor = collection
        .find(doc! {
            "dim_cm": {
                "$elemMatch": {
                    "$gt": 22,
                    "$lt": 30,
                }
            },
        })
        .await?;
    // End Example 26

    assert_cursor_count!(cursor, 1);

    // Start Example 27
    let cursor = collection
        .find(doc! {
            "dim_cm.1": { "$gt": 25 },
        })
        .await?;
    // End Example 27

    assert_cursor_count!(cursor, 1);

    // Start Example 28
    let cursor = collection
        .find(doc! {
            "tags": { "$size": 3 },
        })
        .await?;
    // End Example 28

    assert_cursor_count!(cursor, 1);

    Ok(())
}

async fn query_array_embedded_documents_examples(collection: &Collection<Document>) -> Result<()> {
    collection.drop().await?;

    // Start Example 29
    let docs = vec![
        doc! {
            "item": "journal",
            "instock": [
                { "warehouse": "A", "qty": 5 },
                { "warehouse": "C", "qty": 15 }
            ]
        },
        doc! {
            "item": "notebook",
            "instock": [{ "warehouse": "C", "qty": 5 }]
        },
        doc! {
            "item": "paper",
            "instock": [
                { "warehouse": "A", "qty": 60 },
                { "warehouse": "B", "qty": 15 }
            ]
        },
        doc! {
            "item": "planner",
            "instock": [
                { "warehouse": "A", "qty": 40 },
                { "warehouse": "B", "qty": 5 }
            ]
        },
        doc! {
            "item": "postcard",
            "instock": [
                { "warehouse": "B", "qty": 15 },
                { "warehouse": "C", "qty": 35 }
            ]
        },
    ];

    collection.insert_many(docs).await?;
    // End Example 29

    assert_coll_count!(collection, 5);

    // Start Example 30
    let cursor = collection
        .find(doc! {
            "instock": {
                "warehouse": "A",
                "qty": 5,
            },
        })
        .await?;
    // End Example 30

    assert_cursor_count!(cursor, 1);

    // Start Example 31
    let cursor = collection
        .find(doc! {
            "instock": {
                "qty": 5,
                "warehouse": "A",
            },
        })
        .await?;
    // End Example 31

    assert_cursor_count!(cursor, 0);

    // Start Example 32
    let cursor = collection
        .find(doc! {
            "instock.0.qty": { "$lte": 20 },
        })
        .await?;
    // End Example 32

    assert_cursor_count!(cursor, 3);

    // Start Example 33
    let cursor = collection
        .find(doc! {
            "instock.qty": { "$lte": 20 },
        })
        .await?;
    // End Example 33

    assert_cursor_count!(cursor, 5);

    // Start Example 34
    let cursor = collection
        .find(doc! {
            "instock": {
                "$elemMatch": {
                    "qty": 5,
                    "warehouse": "A",
                }
            },
        })
        .await?;
    // End Example 34

    assert_cursor_count!(cursor, 1);

    // Start Example 35
    let cursor = collection
        .find(doc! {
            "instock": {
                "$elemMatch": {
                    "qty": {
                        "$gt": 10,
                        "$lte": 20,
                    }
                }
            },
        })
        .await?;
    // End Example 35

    assert_cursor_count!(cursor, 3);

    // Start Example 36
    let cursor = collection
        .find(doc! {
            "instock.qty": {
                "$gt": 10,
                "$lte": 20,
            },
        })
        .await?;
    // End Example 36

    assert_cursor_count!(cursor, 4);

    // Start Example 37
    let cursor = collection
        .find(doc! {
            "instock.qty": 5,
            "instock.warehouse": "A",
        })
        .await?;
    // End Example 37

    assert_cursor_count!(cursor, 2);

    Ok(())
}

async fn query_null_or_missing_fields_examples(collection: &Collection<Document>) -> Result<()> {
    collection.drop().await?;

    // Start Example 38
    let docs = vec![
        doc! {
            "_id": 1,
            "item": Bson::Null,
        },
        doc! {
            "_id": 2,
        },
    ];

    collection.insert_many(docs).await?;
    // End Example 38

    assert_coll_count!(collection, 2);

    // Start Example 39
    let cursor = collection
        .find(doc! {
            "item": Bson::Null,
        })
        .await?;
    // End Example 39

    assert_cursor_count!(cursor, 2);

    // Start Example 40
    let cursor = collection
        .find(doc! {
            "item": { "$type": 10 },
        })
        .await?;
    // End Example 40

    assert_cursor_count!(cursor, 1);

    // Start Example 41
    let cursor = collection
        .find(doc! {
            "item": { "$exists": false },
        })
        .await?;
    // End Example 41

    assert_cursor_count!(cursor, 1);

    Ok(())
}

async fn projection_examples(collection: &Collection<Document>) -> Result<()> {
    collection.drop().await?;

    // Start Example 42
    let docs = vec![
        doc! {
            "item": "journal",
            "status": "A",
            "size": {
                "h": 14,
                "w": 21,
                "uom": "cm",
            },
            "instock": [
                {
                    "warehouse": "A",
                    "qty": 5,
                },
            ],
        },
        doc! {
            "item": "notebook",
            "status": "A",
            "size": {
                "h": 8.5,
                "w": 11,
                "uom": "in",
            },
            "instock": [
                {
                    "warehouse":
                    "C",
                    "qty": 5,
                },
            ]
        },
        doc! {
            "item": "paper",
            "status": "D",
            "size": {
                "h": 8.5,
                "w": 11,
                "uom": "in",
            },
            "instock": [
                {
                    "warehouse": "A",
                    "qty": 60,
                },
            ],
        },
        doc! {
            "item": "planner",
            "status": "D",
            "size": {
                "h": 22.85,
                "w": 30,
                "uom": "cm",
            },
            "instock": [
                {
                    "warehouse": "A",
                    "qty": 40,
                },
            ],
        },
        doc! {
            "item": "postcard",
            "status": "A",
            "size": {
                "h": 10,
                "w": 15.25,
                "uom": "cm",
            },
            "instock": [
                {
                    "warehouse": "B",
                    "qty": 15,
                },
                {
                    "warehouse": "C",
                    "qty": 35,
                },
            ],
        },
    ];

    collection.insert_many(docs).await?;
    // End Example 42

    assert_coll_count!(collection, 5);

    // Start Example 43
    let cursor = collection
        .find(doc! {
            "status": "A",
        })
        .await?;
    // End Example 43

    assert_cursor_count!(cursor, 3);

    // Start Example 44
    let cursor = collection
        .find(doc! {
            "status": "A",
        })
        .projection(doc! {
            "item": 1,
            "status": 1,
        })
        .await?;
    // End Example 44

    run_on_each_doc!(cursor, doc, {
        assert!(doc.contains_key("_id"));
        assert!(doc.contains_key("item"));
        assert!(doc.contains_key("status"));
        assert!(!doc.contains_key("size"));
        assert!(!doc.contains_key("instock"));
    });

    // Start Example 45
    let cursor = collection
        .find(doc! {
            "status": "A",
        })
        .projection(doc! {
            "item": 1,
            "status": 1,
            "_id": 0,
        })
        .await?;
    // End Example 45

    run_on_each_doc!(cursor, doc, {
        assert!(!doc.contains_key("_id"));
        assert!(doc.contains_key("item"));
        assert!(doc.contains_key("status"));
        assert!(!doc.contains_key("size"));
        assert!(!doc.contains_key("instock"));
    });

    // Start Example 46
    let cursor = collection
        .find(doc! {
            "status": "A",
        })
        .projection(doc! {
            "status": 0,
            "instock": 0,
        })
        .await?;
    // End Example 46

    run_on_each_doc!(cursor, doc, {
        assert!(doc.contains_key("_id"));
        assert!(doc.contains_key("item"));
        assert!(!doc.contains_key("status"));
        assert!(doc.contains_key("size"));
        assert!(!doc.contains_key("instock"));
    });

    // Start Example 47
    let cursor = collection
        .find(doc! {
            "status": "A",
        })
        .projection(doc! {
            "item": 1,
            "status": 1,
            "size.uom": 1,
        })
        .await?;
    // End Example 47

    run_on_each_doc!(cursor, doc, {
        assert!(doc.contains_key("_id"));
        assert!(doc.contains_key("item"));
        assert!(doc.contains_key("status"));
        assert!(doc.contains_key("size"));
        assert!(!doc.contains_key("instock"));

        let size = doc.get_document("size").unwrap();

        assert!(size.contains_key("uom"));
        assert!(!size.contains_key("h"));
        assert!(!size.contains_key("w"));
    });

    // Start Example 48
    let cursor = collection
        .find(doc! {
            "status": "A",
        })
        .projection(doc! {
            "size.uom": 0,
        })
        .await?;
    // End Example 48

    run_on_each_doc!(cursor, doc, {
        assert!(doc.contains_key("_id"));
        assert!(doc.contains_key("item"));
        assert!(doc.contains_key("status"));
        assert!(doc.contains_key("size"));
        assert!(doc.contains_key("instock"));

        let size = doc.get_document("size").unwrap();

        assert!(!size.contains_key("uom"));
        assert!(size.contains_key("h"));
        assert!(size.contains_key("w"));
    });

    // Start Example 50
    let cursor = collection
        .find(doc! {
            "status": "A",
        })
        .projection(doc! {
            "item": 1,
            "status": 1,
            "instock": { "$slice": -1 },
        })
        .await?;
    // End Example 50

    run_on_each_doc!(cursor, doc, {
        assert!(doc.contains_key("_id"));
        assert!(doc.contains_key("item"));
        assert!(doc.contains_key("status"));
        assert!(!doc.contains_key("size"));
        assert!(doc.contains_key("instock"));

        let instock = doc.get_array("instock").unwrap();

        assert_eq!(instock.len(), 1);
    });

    Ok(())
}

async fn update_examples(collection: &Collection<Document>) -> Result<()> {
    collection.drop().await?;

    // Start Example 51
    let docs = vec![
        doc! {
            "item": "canvas",
            "qty": 100,
            "size": {
                "h": 28,
                "w": 35.5,
                "uom": "cm",
            },
            "status": "A",
        },
        doc! {
            "item": "journal",
            "qty": 25,
            "size": {
                "h": 14,
                "w": 21,
                "uom": "cm",
            },
            "status": "A",
        },
        doc! {
            "item": "mat",
            "qty": 85,
            "size": {
                "h": 27.9,
                "w": 35.5,
                "uom": "cm",
            },
            "status": "A",
        },
        doc! {
            "item": "mousepad",
            "qty": 25,
            "size": {
                "h": 19,
                "w": 22.85,
                "uom": "cm",
            },
            "status": "P",
        },
        doc! {
            "item": "notebook",
            "qty": 50,
            "size": {
                "h": 8.5,
                "w": 11,
                "uom": "in",
            },
            "status": "P",
        },
        doc! {
            "item": "paper",
            "qty": 100,
            "size": {
                "h": 8.5,
                "w": 11,
                "uom": "in",
            },
            "status": "D",
        },
        doc! {
            "item": "planner",
            "qty": 75,
            "size": {
                "h": 22.85,
                "w": 30,
                "uom": "cm",
            },
            "status": "D",
        },
        doc! {
            "item": "postcard",
            "qty": 45,
            "size": {
                "h": 10,
                "w": 15.25,
                "uom": "cm",
            },
            "status": "A",
        },
        doc! {
            "item": "sketchbook",
            "qty": 80,
            "size": {
                "h": 14,
                "w": 21,
                "uom": "cm",
            },
            "status": "A",
        },
        doc! {
            "item": "sketch pad",
            "qty": 95,
            "size": {
                "h": 22.85,
                "w": 30.5,
                "uom": "cm",
            },
            "status": "A",
        },
    ];

    collection.insert_many(docs).await?;
    // End Example 51

    assert_coll_count!(collection, 10);

    // Start Example 52
    collection
        .update_one(
            doc! { "item": "paper" },
            doc! {
                "$set": {
                    "size.uom": "cm",
                    "status": "P",
                },
                "$currentDate": { "lastModified": true },
            },
        )
        .await?;
    // End Example 52

    run_on_each_doc!(
        collection.find(doc! { "item": "paper" }).await.unwrap(),
        doc,
        {
            let uom = doc.get_document("size").unwrap().get_str("uom").unwrap();
            assert_eq!(uom, "cm");

            let status = doc.get_str("status").unwrap();
            assert_eq!(status, "P");

            assert!(doc.contains_key("lastModified"));
        }
    );

    // Start Example 53
    collection
        .update_many(
            doc! {
                "qty": { "$lt": 50 },
            },
            doc! {
                "$set": {
                    "size.uom": "in",
                    "status": "P",
                },
                "$currentDate": { "lastModified": true },
            },
        )
        .await?;
    // End Example 53

    run_on_each_doc!(
        collection
            .find(doc! {
                "qty": { "$lt": 50  },
            })
            .await
            .unwrap(),
        doc,
        {
            let uom = doc.get_document("size").unwrap().get_str("uom").unwrap();
            assert_eq!(uom, "in");

            let status = doc.get_str("status").unwrap();
            assert_eq!(status, "P");

            assert!(doc.contains_key("lastModified"));
        }
    );

    // Start Example 54
    collection
        .replace_one(
            doc! { "item": "paper" },
            doc! {
                "item": "paper",
                "instock": [
                    {
                        "warehouse": "A",
                        "qty": 60,
                    },
                    {
                        "warehouse": "B",
                        "qty": 40,
                    },
                ],
            },
        )
        .await?;
    // End Example 54

    run_on_each_doc!(
        collection.find(doc! { "item": "paper" }).await.unwrap(),
        doc,
        {
            assert_eq!(doc.len(), 3);
            assert!(doc.contains_key("_id"));
            assert!(doc.contains_key("item"));
            assert!(doc.contains_key("instock"));

            let instock = doc.get_array("instock").unwrap();
            assert_eq!(instock.len(), 2);
        }
    );

    Ok(())
}

async fn delete_examples(collection: &Collection<Document>) -> Result<()> {
    collection.drop().await?;

    // Start Example 55
    let docs = vec![
        doc! {
            "item": "journal",
            "qty": 25,
            "size": {
                "h": 14,
                "w": 21,
                "uom": "cm",
            },
            "status": "A",
        },
        doc! {
            "item": "notebook",
            "qty": 50,
            "size": {
                "h": 8.5,
                "w": 11,
                "uom": "in",
            },
            "status": "P",
        },
        doc! {
            "item": "paper",
            "qty": 100,
            "size": {
                "h": 8.5,
                "w": 11,
                "uom": "in",
            },
            "status": "D",
        },
        doc! {
            "item": "planner",
            "qty": 75,
            "size": {
                "h": 22.85,
                "w": 30,
                "uom": "cm",
            },
            "status": "D",
        },
        doc! {
            "item": "postcard",
            "qty": 45,
            "size": {
                "h": 10,
                "w": 15.25,
                "uom": "cm",
            },
            "status": "A",
        },
    ];

    collection.insert_many(docs).await?;
    // End Example 55

    assert_coll_count!(collection, 5);

    // Start Example 57
    collection.delete_many(doc! { "status": "A" }).await?;
    // End Example 57

    assert_coll_count!(collection, 3);

    // Start Example 58
    collection.delete_one(doc! { "status": "D" }).await?;
    // End Example 58

    assert_coll_count!(collection, 2);

    // Start Example 56
    collection.delete_many(doc! {}).await?;
    // End Example 56

    assert_coll_count!(collection, 0);

    Ok(())
}

type GenericResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[allow(unused_variables)]
async fn stable_api_examples() -> GenericResult<()> {
    if server_version_lt(4, 9).await {
        log_uncaptured("skipping stable API examples due to unsupported server version");
        return Ok(());
    }
    if topology_is_sharded().await && server_version_matches("<=5.0.2").await {
        // See SERVER-58794.
        log_uncaptured(
            "skipping stable API examples due to unsupported server version on sharded topology",
        );
        return Ok(());
    }
    if topology_is_load_balanced().await {
        log_uncaptured("skipping stable API examples due to load-balanced topology");
        return Ok(());
    }

    let setup_client = Client::for_test().await;

    let uri = DEFAULT_URI.clone();
    // Start Versioned API Example 1
    let mut options = ClientOptions::parse(&uri).await?;
    let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
    options.server_api = Some(server_api);
    let client = Client::with_options(options)?;
    // End Versioned API Example 1

    // Start Versioned API Example 2
    let mut options = ClientOptions::parse(&uri).await?;
    let server_api = ServerApi::builder()
        .version(ServerApiVersion::V1)
        .strict(true)
        .build();
    options.server_api = Some(server_api);
    let client = Client::with_options(options)?;
    // End Versioned API Example 2

    // Start Versioned API Example 3
    let mut options = ClientOptions::parse(&uri).await?;
    let server_api = ServerApi::builder()
        .version(ServerApiVersion::V1)
        .strict(false)
        .build();
    options.server_api = Some(server_api);
    let client = Client::with_options(options)?;
    // End Versioned API Example 3

    // Start Versioned API Example 4
    let mut options = ClientOptions::parse(&uri).await?;
    let server_api = ServerApi::builder()
        .version(ServerApiVersion::V1)
        .deprecation_errors(true)
        .build();
    options.server_api = Some(server_api);
    let client = Client::with_options(options)?;
    // End Versioned API Example 4

    let mut options = ClientOptions::parse(&uri).await?;
    let server_api = ServerApi::builder()
        .version(ServerApiVersion::V1)
        .strict(true)
        .build();
    options.server_api = Some(server_api);
    let client = Client::with_options(options)?;
    let db = client.database("stable-api-migration-examples");
    db.collection::<Document>("sales").drop().await?;

    use std::{error::Error, result::Result};

    // Start Versioned API Example 5
    // With the `bson-chrono-0_4` feature enabled, this function can be dropped in favor of using
    // `chrono::DateTime` values directly.
    fn iso_date(text: &str) -> Result<crate::bson::DateTime, Box<dyn Error>> {
        let chrono_dt = chrono::DateTime::parse_from_rfc3339(text)?;
        Ok(crate::bson::DateTime::from_millis(
            chrono_dt.timestamp_millis(),
        ))
    }
    db.collection("sales").insert_many(vec![
        doc! { "_id" : 1, "item" : "abc", "price" : 10, "quantity" : 2, "date" : iso_date("2021-01-01T08:00:00Z")? },
        doc! { "_id" : 3, "item" : "xyz", "price" : 5, "quantity" : 5, "date" : iso_date("2021-02-03T09:05:00Z")? },
        doc! { "_id" : 2, "item" : "jkl", "price" : 20, "quantity" : 1, "date" : iso_date("2021-02-03T09:00:00Z")? },
        doc! { "_id" : 4, "item" : "abc", "price" : 10, "quantity" : 10, "date" : iso_date("2021-02-15T08:00:00Z")? },
        doc! { "_id" : 5, "item" : "xyz", "price" : 5, "quantity" : 10, "date" : iso_date("2021-02-15T09:05:00Z")? },
        doc! { "_id" : 6, "item" : "xyz", "price" : 5, "quantity" : 5, "date" : iso_date("2021-02-15T12:05:10Z")? },
        doc! { "_id" : 7, "item" : "xyz", "price" : 5, "quantity" : 10, "date" : iso_date("2021-02-15T14:12:12Z")? },
        doc! { "_id" : 8, "item" : "abc", "price" : 10, "quantity" : 5, "date" : iso_date("2021-03-16T20:20:13Z")? }
    ]).await?;
    // End Versioned API Example 5

    // Start Versioned API Example 6
    let result = db
        .run_command(doc! {
            "count": "sales"
        })
        .await;
    if let Err(err) = &result {
        println!("{:#?}", err.kind);
        // Prints:
        // Command(
        //     CommandError {
        //         code: 323,
        //         code_name: "APIStrictError",
        //         message: "Provided apiStrict:true, but the command count is not in API Version 1. Information on supported commands and migrations in API Version 1 can be found at https://www.mongodb.com/docs/v5.0/reference/stable-api/",
        //     },
        // )
    }
    // End Versioned API Example 6

    // TODO: Uncomment or remove this test once the prior example is updated
    // if let ErrorKind::Command(ref err) = *result.as_ref().unwrap_err().kind {
    //     assert_eq!(err.code, 323);
    //     assert_eq!(err.code_name, "APIStrictError".to_string());
    // } else {
    //     panic!("invalid result {:?}", result);
    // };

    // Start Versioned API Example 7
    let count = db
        .collection::<Document>("sales")
        .count_documents(doc! {})
        .await?;
    // End Versioned API Example 7

    // Start Versioned API Example 8
    assert_eq!(count, 8);
    // End Versioned API Example 8

    Ok(())
}

#[allow(unused_imports)]
async fn aggregation_examples() -> GenericResult<()> {
    let client = Client::for_test().await;
    let db = client.database("aggregation_examples");
    db.drop().await?;
    aggregation_data::populate(&db).await?;

    // Each example is within its own scope to allow the example to include
    // `use futures::TryStreamExt;` without causing multiple definition errors.

    {
        // Start Aggregation Example 1
        use futures::TryStreamExt;
        let cursor = db
            .collection::<Document>("sales")
            .aggregate(vec![
                doc! { "$match": { "items.fruit": "banana" } },
                doc! { "$sort": { "date": 1 } },
            ])
            .await?;
        let values: Vec<_> = cursor.try_collect().await?;
        // End Aggregation Example 1
        assert_eq!(5, values.len());
    }

    {
        // Start Aggregation Example 2
        use futures::TryStreamExt;
        let cursor = db
            .collection::<Document>("sales")
            .aggregate(vec![
                doc! {
                    "$unwind": "$items"
                },
                doc! { "$match": {
                    "items.fruit": "banana",
                }},
                doc! {
                    "$group": {
                        "_id": { "day": { "$dayOfWeek": "$date" } },
                        "count": { "$sum": "$items.quantity" }
                    }
                },
                doc! {
                    "$project": {
                        "dayOfWeek": "$_id.day",
                        "numberSold": "$count",
                        "_id": 0
                    }
                },
                doc! {
                    "$sort": { "numberSold": 1 }
                },
            ])
            .await?;
        let values: Vec<_> = cursor.try_collect().await?;
        // End Aggregation Example 2
        assert_eq!(4, values.len());
    }

    {
        // Start Aggregation Example 3
        use futures::TryStreamExt;
        let cursor = db.collection::<Document>("sales").aggregate(
            vec![
                doc! {
                    "$unwind": "$items"
                },
                doc! {
                    "$group": {
                        "_id": { "day": { "$dayOfWeek": "$date" } },
                        "items_sold": { "$sum": "$items.quantity" },
                        "revenue": { "$sum": { "$multiply": [ "$items.quantity", "$items.price" ] } }
                    }
                },
                doc! {
                    "$project": {
                        "day": "$_id.day",
                        "revenue": 1,
                        "items_sold": 1,
                        "discount": {
                            "$cond": { "if": { "$lte": [ "$revenue", 250 ] }, "then": 25, "else": 0 }
                        }
                    }
                },
            ]
        ).await?;
        let values: Vec<_> = cursor.try_collect().await?;
        // End Aggregation Example 3
        assert_eq!(4, values.len());
    }

    {
        // Start Aggregation Example 4
        use futures::TryStreamExt;
        let cursor = db
            .collection::<Document>("air_alliances")
            .aggregate(vec![
                doc! {
                    "$lookup": {
                        "from": "air_airlines",
                        "let": { "constituents": "$airlines" },
                        "pipeline": [
                            {
                                "$match": { "$expr": { "$in": [ "$name", "$$constituents" ] } }
                            }
                        ],
                        "as": "airlines"
                    }
                },
                doc! {
                    "$project": {
                        "_id": 0,
                        "name": 1,
                        "airlines": {
                            "$filter": {
                                "input": "$airlines",
                                "as": "airline",
                                "cond": { "$eq": ["$$airline.country", "Canada"] }
                            }
                        }
                    }
                },
            ])
            .await?;
        let values: Vec<_> = cursor.try_collect().await?;
        // End Aggregation Example 4
        assert_eq!(3, values.len());
    }

    Ok(())
}

async fn run_command_examples() -> Result<()> {
    let client = Client::for_test().await;
    let db = client.database("run_command_examples");
    db.drop().await?;
    db.collection::<Document>("restaurants")
        .insert_one(doc! {
            "name": "Chez Panisse",
            "city": "Oakland",
            "state": "California",
            "country": "United States",
            "rating": 4.4,
        })
        .await?;

    #[allow(unused)]
    // Start runCommand Example 1
    let info = db.run_command(doc! {"buildInfo": 1}).await?;
    // End runCommand Example 1

    #[allow(unused)]
    // Start runCommand Example 2
    let stats = db.run_command(doc! {"collStats": "restaurants"}).await?;
    // End runCommand Example 2

    Ok(())
}

async fn index_examples() -> Result<()> {
    let client = Client::for_test().await;
    let db = client.database("index_examples");
    db.drop().await?;
    db.collection::<Document>("records")
        .insert_many(vec![
            doc! {
                "student": "Marty McFly",
                "classYear": 1986,
                "school": "Hill Valley High",
                "score": 56.5,
            },
            doc! {
                "student": "Ferris F. Bueller",
                "classYear": 1987,
                "school": "Glenbrook North High",
                "status": "Suspended",
                "score": 76.0,
            },
        ])
        .await?;
    db.collection::<Document>("restaurants")
        .insert_many(vec![
            doc! {
                "name": "Chez Panisse",
                "city": "Oakland",
                "state": "California",
                "country": "United States",
                "rating": 4.4,
            },
            doc! {
                "name": "Eleven Madison Park",
                "cuisine": "French",
                "city": "New York City",
                "state": "New York",
                "country": "United States",
                "rating": 7.1,
            },
        ])
        .await?;

    use crate::IndexModel;
    // Start Index Example 1
    db.collection::<Document>("records")
        .create_index(IndexModel::builder().keys(doc! { "score": 1 }).build())
        .await?;
    // End Index Example 1

    use crate::options::IndexOptions;
    // Start Index Example 2
    db.collection::<Document>("records")
        .create_index(
            IndexModel::builder()
                .keys(doc! { "cuisine": 1, "name": 1 })
                .options(
                    IndexOptions::builder()
                        .partial_filter_expression(doc! { "rating": { "$gt": 5 } })
                        .build(),
                )
                .build(),
        )
        .await?;
    // End Index Example 2

    Ok(())
}

#[allow(unused_variables)]
async fn change_streams_examples() -> Result<()> {
    use crate::{options::FullDocumentType, runtime};
    use std::time::Duration;

    if !topology_is_replica_set().await && !topology_is_sharded().await {
        log_uncaptured("skipping change_streams_examples due to unsupported topology");
        return Ok(());
    }

    let client = Client::for_test().await;
    let db = client.database("change_streams_examples");
    db.drop().await?;
    let inventory = db.collection::<Document>("inventory");
    // Populate an item so the collection exists for the change stream to watch.
    inventory.insert_one(doc! {}).await?;

    // Background writer thread so that the `stream.next()` calls return something.
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    let writer_inventory = inventory.clone();
    let handle = runtime::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    writer_inventory.insert_one(doc! {}).await?;
                }
                _ = &mut rx => break,
            }
        }
        Result::Ok(())
    });

    #[allow(unused_variables, unused_imports)]
    {
        {
            // Start Changestream Example 1
            use futures::stream::TryStreamExt;

            let mut stream = inventory.watch().await?;
            let next = stream.try_next().await?;
            // End Changestream Example 1
        }

        {
            // Start Changestream Example 2
            use futures::stream::TryStreamExt;
            let mut stream = inventory
                .watch()
                .full_document(FullDocumentType::UpdateLookup)
                .await?;
            let next = stream.try_next().await?;
            // End Changestream Example 2
        }

        {
            let stream = inventory.watch().await?;
            // Start Changestream Example 3
            use futures::stream::TryStreamExt;
            let resume_token = stream.resume_token();
            let mut stream = inventory.watch().resume_after(resume_token).await?;
            stream.try_next().await?;
            // End Changestream Example 3
        }
    }

    // Shut down the writer thread.
    let _ = tx.send(());
    handle.await?;

    Ok(())
}

async fn convenient_transaction_examples() -> Result<()> {
    use crate::ClientSession;
    if !transactions_supported().await {
        log_uncaptured(
            "skipping convenient transaction API examples due to no transaction support",
        );
        return Ok(());
    }

    let client = Client::for_test().await;
    // Start Transactions withTxn API Example 1

    // Prereq: Create collections. CRUD operations in transactions must be on existing collections.

    client
        .database("mydb1")
        .collection::<Document>("foo")
        .insert_one(doc! { "abc": 0})
        .await?;
    client
        .database("mydb2")
        .collection::<Document>("bar")
        .insert_one(doc! { "xyz": 0})
        .await?;

    // Step 1: Define the callback that specifies the sequence of operations to perform inside the
    // transaction.
    async fn callback(session: &mut ClientSession) -> Result<()> {
        let collection_one = session
            .client()
            .database("mydb1")
            .collection::<Document>("foo");
        let collection_two = session
            .client()
            .database("mydb2")
            .collection::<Document>("bar");

        // Important: You must pass the session to the operations.
        collection_one
            .insert_one(doc! { "abc": 1 })
            .session(&mut *session)
            .await?;
        collection_two
            .insert_one(doc! { "xyz": 999 })
            .session(session)
            .await?;

        Ok(())
    }

    // Step 2: Start a client session.
    let mut session = client.start_session().await?;

    // Step 3: Use and_run2 to start a transaction, execute the callback, and commit (or
    // abort on error).
    session.start_transaction().and_run2(callback).await?;

    // End Transactions withTxn API Example 1

    Ok(())
}

#[tokio::test]
async fn test() {
    let client = Client::for_test().await;
    let coll = client
        .database("documentation_examples")
        .collection("inventory");

    insert_examples(&coll).await.unwrap();
    query_top_level_fields_examples(&coll).await.unwrap();
    query_embedded_documents_examples(&coll).await.unwrap();
    query_arrays_examples(&coll).await.unwrap();
    query_array_embedded_documents_examples(&coll)
        .await
        .unwrap();
    query_null_or_missing_fields_examples(&coll).await.unwrap();
    projection_examples(&coll).await.unwrap();
    update_examples(&coll).await.unwrap();
    delete_examples(&coll).await.unwrap();
    stable_api_examples().await.unwrap();
    aggregation_examples().await.unwrap();
    run_command_examples().await.unwrap();
    index_examples().await.unwrap();
    change_streams_examples().await.unwrap();
    convenient_transaction_examples().await.unwrap();
}
