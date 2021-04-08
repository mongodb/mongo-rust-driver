use bson::Document;
use futures::TryStreamExt;
use tokio::sync::RwLockReadGuard;

use crate::{
    bson::{doc, Bson},
    error::Result,
    options::{ClientOptions, CursorType, FindOptions, ServerApi, ServerApiVersion},
    test::{TestClient, DEFAULT_URI, LOCK},
    Client,
    Collection,
};

macro_rules! assert_coll_count {
    ($coll:expr, $expected:expr) => {
        assert_eq!($coll.count_documents(None, None).await.unwrap(), $expected);
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

async fn insert_examples(collection: &Collection) -> Result<()> {
    collection.drop(None).await.unwrap();

    // Start Example 1
    collection
        .insert_one(
            doc! {
                "item": "canvas",
                "qty": 100,
                "tags": ["cotton"],
                "size": {
                    "h": 28,
                    "w": 35.5,
                    "uom": "cm",
                }
            },
            None,
        )
        .await?;
    // End Example 1

    assert_coll_count!(collection, 1);

    // Start Example 2
    let cursor = collection.find(doc! { "item": "canvas" }, None).await?;
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

    collection.insert_many(docs, None).await?;
    // End Example 3

    assert_coll_count!(collection, 4);

    Ok(())
}

async fn query_top_level_fields_examples(collection: &Collection) -> Result<()> {
    collection.drop(None).await.unwrap();

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

    collection.insert_many(docs, None).await?;
    // End Example 6

    assert_coll_count!(collection, 5);

    // Start Example 7
    let cursor = collection.find(None, None).await?;
    // End Example 7

    assert_cursor_count!(cursor, 5);

    // Start Example 9
    let cursor = collection.find(doc! { "status": "D" }, None).await?;
    // End Example 9

    assert_cursor_count!(cursor, 2);

    // Start Example 10
    let cursor = collection
        .find(
            doc! {
                "status": {
                    "$in": ["A", "D"],
                }
            },
            None,
        )
        .await?;
    // End Example 10

    assert_cursor_count!(cursor, 5);

    // Start Example 11
    let cursor = collection
        .find(
            doc! {
                "status": "A",
                "qty": { "$lt": 30 },
            },
            None,
        )
        .await?;
    // End Example 11

    assert_cursor_count!(cursor, 1);

    // Start Example 12
    let cursor = collection
        .find(
            doc! {
                "$or": [
                    { "status": "A" },
                    {
                        "qty": { "$lt": 30 },
                    }
                ],
            },
            None,
        )
        .await?;
    // End Example 12

    assert_cursor_count!(cursor, 3);

    // Start Example 13
    let cursor = collection
        .find(
            doc! {
                "status": "A",
                "$or": [
                    {
                        "qty": { "$lt": 30 },
                    },
                    {
                        "item": { "$regex": "^p" },
                    },
                ],
            },
            None,
        )
        .await?;
    // End Example 13

    assert_cursor_count!(cursor, 2);

    Ok(())
}

async fn query_embedded_documents_examples(collection: &Collection) -> Result<()> {
    collection.drop(None).await.unwrap();

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

    collection.insert_many(docs, None).await?;
    // End Example 14

    assert_coll_count!(collection, 5);

    // Start Example 15
    let cursor = collection
        .find(
            doc! {
                "size": {
                    "h": 14,
                    "w": 21,
                    "uom": "cm",
                },
            },
            None,
        )
        .await?;
    // End Example 15

    assert_cursor_count!(cursor, 1);

    // Start Example 16
    let cursor = collection
        .find(
            doc! {
                "size": {
                    "w": 21,
                    "h": 14,
                    "uom": "cm",
                },
            },
            None,
        )
        .await?;
    // End Example 16

    assert_cursor_count!(cursor, 0);

    // Start Example 17
    let cursor = collection.find(doc! { "size.uom": "in" }, None).await?;
    // End Example 17

    assert_cursor_count!(cursor, 2);

    // Start Example 18
    let cursor = collection
        .find(
            doc! {
                "size.h": { "$lt": 15 },
            },
            None,
        )
        .await?;
    // End Example 18

    assert_cursor_count!(cursor, 4);

    // Start Example 19
    let cursor = collection
        .find(
            doc! {
                "size.h": { "$lt": 15 },
                "size.uom": "in",
                "status": "D",
            },
            None,
        )
        .await?;
    // End Example 19

    assert_cursor_count!(cursor, 1);

    Ok(())
}

async fn query_arrays_examples(collection: &Collection) -> Result<()> {
    collection.drop(None).await?;

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

    collection.insert_many(docs, None).await?;
    // End Example 20

    assert_coll_count!(collection, 5);

    // Start Example 21
    let cursor = collection
        .find(
            doc! {
                "tags": ["red", "blank"],
            },
            None,
        )
        .await?;
    // End Example 21

    assert_cursor_count!(cursor, 1);

    // Start Example 22
    let cursor = collection
        .find(
            doc! {
                "tags": {
                    "$all": ["red", "blank"],
                }
            },
            None,
        )
        .await?;
    // End Example 22

    assert_cursor_count!(cursor, 4);

    // Start Example 23
    let cursor = collection
        .find(
            doc! {
                "tags": "red",
            },
            None,
        )
        .await?;
    // End Example 23

    assert_cursor_count!(cursor, 4);

    // Start Example 24
    let cursor = collection
        .find(
            doc! {
                "dim_cm": { "$gt": 25 },
            },
            None,
        )
        .await?;
    // End Example 24

    assert_cursor_count!(cursor, 1);

    // Start Example 25
    let cursor = collection
        .find(
            doc! {
                "dim_cm": {
                    "$gt": 15,
                    "$lt": 20,
                },
            },
            None,
        )
        .await?;
    // End Example 25

    assert_cursor_count!(cursor, 4);

    // Start Example 26
    let cursor = collection
        .find(
            doc! {
                "dim_cm": {
                    "$elemMatch": {
                        "$gt": 22,
                        "$lt": 30,
                    }
                },
            },
            None,
        )
        .await?;
    // End Example 26

    assert_cursor_count!(cursor, 1);

    // Start Example 27
    let cursor = collection
        .find(
            doc! {
                "dim_cm.1": { "$gt": 25 },
            },
            None,
        )
        .await?;
    // End Example 27

    assert_cursor_count!(cursor, 1);

    // Start Example 28
    let cursor = collection
        .find(
            doc! {
                "tags": { "$size": 3 },
            },
            None,
        )
        .await?;
    // End Example 28

    assert_cursor_count!(cursor, 1);

    Ok(())
}

async fn query_array_embedded_documents_examples(collection: &Collection) -> Result<()> {
    collection.drop(None).await?;

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

    collection.insert_many(docs, None).await?;
    // End Example 29

    assert_coll_count!(collection, 5);

    // Start Example 30
    let cursor = collection
        .find(
            doc! {
                "instock": {
                    "warehouse": "A",
                    "qty": 5,
                },
            },
            None,
        )
        .await?;
    // End Example 30

    assert_cursor_count!(cursor, 1);

    // Start Example 31
    let cursor = collection
        .find(
            doc! {
                "instock": {
                    "qty": 5,
                    "warehouse": "A",
                },
            },
            None,
        )
        .await?;
    // End Example 31

    assert_cursor_count!(cursor, 0);

    // Start Example 32
    let cursor = collection
        .find(
            doc! {
                "instock.0.qty": { "$lte": 20 },
            },
            None,
        )
        .await?;
    // End Example 32

    assert_cursor_count!(cursor, 3);

    // Start Example 33
    let cursor = collection
        .find(
            doc! {
                "instock.qty": { "$lte": 20 },
            },
            None,
        )
        .await?;
    // End Example 33

    assert_cursor_count!(cursor, 5);

    // Start Example 34
    let cursor = collection
        .find(
            doc! {
                "instock": {
                    "$elemMatch": {
                        "qty": 5,
                        "warehouse": "A",
                    }
                },
            },
            None,
        )
        .await?;
    // End Example 34

    assert_cursor_count!(cursor, 1);

    // Start Example 35
    let cursor = collection
        .find(
            doc! {
                "instock": {
                    "$elemMatch": {
                        "qty": {
                            "$gt": 10,
                            "$lte": 20,
                        }
                    }
                },
            },
            None,
        )
        .await?;
    // End Example 35

    assert_cursor_count!(cursor, 3);

    // Start Example 36
    let cursor = collection
        .find(
            doc! {
                "instock.qty": {
                    "$gt": 10,
                    "$lte": 20,
                },
            },
            None,
        )
        .await?;
    // End Example 36

    assert_cursor_count!(cursor, 4);

    // Start Example 37
    let cursor = collection
        .find(
            doc! {
                "instock.qty": 5,
                "instock.warehouse": "A",
            },
            None,
        )
        .await?;
    // End Example 37

    assert_cursor_count!(cursor, 2);

    Ok(())
}

async fn query_null_or_missing_fields_examples(collection: &Collection) -> Result<()> {
    collection.drop(None).await.unwrap();

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

    collection.insert_many(docs, None).await?;
    // End Example 38

    assert_coll_count!(collection, 2);

    // Start Example 39
    let cursor = collection
        .find(
            doc! {
                "item": Bson::Null,
            },
            None,
        )
        .await?;
    // End Example 39

    assert_cursor_count!(cursor, 2);

    // Start Example 40
    let cursor = collection
        .find(
            doc! {
                "item": { "$type": 10 },
            },
            None,
        )
        .await?;
    // End Example 40

    assert_cursor_count!(cursor, 1);

    // Start Example 41
    let cursor = collection
        .find(
            doc! {
                "item": { "$exists": false },
            },
            None,
        )
        .await?;
    // End Example 41

    assert_cursor_count!(cursor, 1);

    Ok(())
}

async fn projection_examples(collection: &Collection) -> Result<()> {
    collection.drop(None).await?;

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

    collection.insert_many(docs, None).await?;
    // End Example 42

    assert_coll_count!(collection, 5);

    // Start Example 43
    let cursor = collection
        .find(
            doc! {
                "status": "A",
            },
            None,
        )
        .await?;
    // End Example 43

    assert_cursor_count!(cursor, 3);

    // Start Example 44
    let options = FindOptions::builder()
        .projection(doc! {
            "item": 1,
            "status": 1,
        })
        .build();

    let cursor = collection
        .find(
            doc! {
                "status": "A",
            },
            options,
        )
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
    let options = FindOptions::builder()
        .projection(doc! {
            "item": 1,
            "status": 1,
            "_id": 0,
        })
        .build();

    let cursor = collection
        .find(
            doc! {
                "status": "A",
            },
            options,
        )
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
    let options = FindOptions::builder()
        .projection(doc! {
            "status": 0,
            "instock": 0,
        })
        .build();

    let cursor = collection
        .find(
            doc! {
                "status": "A",
            },
            options,
        )
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
    let options = FindOptions::builder()
        .projection(doc! {
            "item": 1,
            "status": 1,
            "size.uom": 1,
        })
        .build();

    let cursor = collection
        .find(
            doc! {
                "status": "A",
            },
            options,
        )
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
    let options = FindOptions::builder()
        .projection(doc! {
            "size.uom": 0,
        })
        .build();

    let cursor = collection
        .find(
            doc! {
                "status": "A",
            },
            options,
        )
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
    let options = FindOptions::builder()
        .projection(doc! {
            "item": 1,
            "status": 1,
            "instock": { "$slice": -1 },
        })
        .build();

    let cursor = collection
        .find(
            doc! {
                "status": "A",
            },
            options,
        )
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

async fn update_examples(collection: &Collection) -> Result<()> {
    collection.drop(None).await.unwrap();

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

    collection.insert_many(docs, None).await?;
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
            None,
        )
        .await?;
    // End Example 52

    run_on_each_doc!(
        collection
            .find(doc! { "item": "paper" }, None)
            .await
            .unwrap(),
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
            None,
        )
        .await?;
    // End Example 53

    run_on_each_doc!(
        collection
            .find(
                doc! {
                    "qty": { "$lt": 50  },
                },
                None,
            )
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
            None,
        )
        .await
        .unwrap();
    // End Example 54

    run_on_each_doc!(
        collection
            .find(doc! { "item": "paper" }, None,)
            .await
            .unwrap(),
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

async fn delete_examples(collection: &Collection) -> Result<()> {
    collection.drop(None).await.unwrap();

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

    collection.insert_many(docs, None).await?;
    // End Example 55

    assert_coll_count!(collection, 5);

    // Start Example 57
    collection.delete_many(doc! { "status": "A" }, None).await?;
    // End Example 57

    assert_coll_count!(collection, 3);

    // Start Example 58
    collection.delete_one(doc! { "status": "D" }, None).await?;
    // End Example 58

    assert_coll_count!(collection, 2);

    // Start Example 56
    collection.delete_many(doc! {}, None).await?;
    // End Example 56

    assert_coll_count!(collection, 0);

    Ok(())
}

#[allow(unused_variables)]
#[cfg(not(feature = "sync"))]
async fn versioned_api_examples() -> Result<()> {
    let setup_client = TestClient::new().await;
    if setup_client.server_version_lt(4, 9) {
        println!("skipping versioned API examples due to unsupported server version");
        return Ok(());
    }

    let uri = DEFAULT_URI.clone();
    // Start 1. Declare an API version on a client
    let mut options = ClientOptions::parse(&uri).await?;
    let server_api = ServerApi::builder()
        .version(ServerApiVersion::Version1)
        .build();
    options.server_api = Some(server_api);
    let client = Client::with_options(options)?;
    let cursor = client
        .database("versioned_api_example")
        .collection::<Document>("example")
        .find(None, None)
        .await?;
    // End 1.

    // Start 2. Strict option
    let mut options = ClientOptions::parse(&uri).await?;
    let server_api = ServerApi::builder()
        .version(ServerApiVersion::Version1)
        .strict(true)
        .build();
    options.server_api = Some(server_api);
    let client = Client::with_options(options)?;

    let find_options = FindOptions::builder()
        .cursor_type(CursorType::Tailable)
        .build();
    let cursor = client
        .database("versioned_api_example")
        .collection::<Document>("example")
        .find(None, find_options)
        .await
        .expect_err("should fail");
    // End 2.

    // Start 3. deprecationErrors option
    let mut options = ClientOptions::parse(&uri).await?;
    let server_api = ServerApi::builder()
        .version(ServerApiVersion::Version1)
        .deprecation_errors(true)
        .build();
    options.server_api = Some(server_api);
    let client = Client::with_options(options)?;
    // End 3.

    Ok(())
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn test() {
    let _guard: RwLockReadGuard<_> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
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
    versioned_api_examples().await.unwrap();
}
