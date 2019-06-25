mod indexes;

use bson::Bson;
use mongodb::options::{AggregateOptions, UpdateOptions};

#[test]
#[function_name]
fn count() {
    let coll = crate::get_coll(function_name!(), function_name!());

    coll.drop().unwrap();
    assert_eq!(coll.estimated_document_count(None).unwrap(), 0);

    let _ = coll.insert_one(doc! { "x": 1 }, None).unwrap();
    assert_eq!(coll.estimated_document_count(None).unwrap(), 1);

    let result = coll
        .insert_many((1..4).map(|i| doc! { "x": i }).collect::<Vec<_>>(), None)
        .unwrap();
    assert_eq!(result.inserted_ids.len(), 3);
    assert_eq!(coll.estimated_document_count(None).unwrap(), 4);
}

#[test]
#[function_name]
fn find() {
    let coll = crate::get_coll(function_name!(), function_name!());

    coll.drop().unwrap();

    let result = coll
        .insert_many((0i32..5).map(|i| doc! { "x": i }).collect::<Vec<_>>(), None)
        .unwrap();
    assert_eq!(result.inserted_ids.len(), 5);

    for (i, result) in coll.find(None, None).unwrap().enumerate() {
        let doc = result.unwrap();
        if i > 4 {
            panic!("expected 4 result, got {}", i);
        }

        assert_eq!(doc.len(), 2);
        assert!(doc.contains_key("_id"));
        assert_eq!(doc.get("x"), Some(&Bson::I32(i as i32)));
    }
}

#[test]
#[function_name]
fn update() {
    let coll = crate::get_coll(function_name!(), function_name!());

    coll.drop().unwrap();

    let result = coll
        .insert_many((0i32..5).map(|_| doc! { "x": 3 }).collect::<Vec<_>>(), None)
        .unwrap();
    assert_eq!(result.inserted_ids.len(), 5);

    let update_one_results = coll
        .update_one(doc! {"x": 3}, doc! {"$set": { "x": 5 }}, None)
        .unwrap();
    assert_eq!(update_one_results.modified_count, 1);
    assert!(update_one_results.upserted_id.is_none());

    let update_many_results = coll
        .update_many(doc! {"x": 3}, doc! {"$set": { "x": 4}}, None)
        .unwrap();
    assert_eq!(update_many_results.modified_count, 4);
    assert!(update_many_results.upserted_id.is_none());

    let options = UpdateOptions {
        array_filters: None,
        bypass_document_validation: None,
        upsert: Some(true),
    };
    let upsert_results = coll
        .update_one(doc! {"b": 7}, doc! {"$set": { "b": 7 }}, Some(options))
        .unwrap();
    assert_eq!(upsert_results.modified_count, 0);
    assert!(upsert_results.upserted_id.is_some());
}

#[test]
#[function_name]
fn delete() {
    let coll = crate::get_coll(function_name!(), function_name!());

    coll.drop().unwrap();

    let result = coll
        .insert_many((0i32..5).map(|_| doc! { "x": 3 }).collect::<Vec<_>>(), None)
        .unwrap();
    assert_eq!(result.inserted_ids.len(), 5);

    let delete_one_result = coll.delete_one(doc! {"x": 3}, None).unwrap();
    assert_eq!(delete_one_result.deleted_count, 1);

    assert_eq!(coll.count_documents(Some(doc! {"x": 3}), None).unwrap(), 4);
    let delete_many_result = coll.delete_many(doc! {"x": 3}, None).unwrap();
    assert_eq!(delete_many_result.deleted_count, 4);
    assert_eq!(coll.count_documents(Some(doc! {"x": 3 }), None).unwrap(), 0);
}

#[test]
#[function_name]
fn aggregate_out() {
    let db = crate::get_db(function_name!());
    let coll = db.collection(function_name!());

    coll.drop().unwrap();

    let result = coll
        .insert_many((0i32..5).map(|n| doc! { "x": n }).collect::<Vec<_>>(), None)
        .unwrap();
    assert_eq!(result.inserted_ids.len(), 5);

    let out_coll = db.collection(&format!("{}_1", function_name!()));
    let pipeline = vec![
        doc! {
            "$match": {
                "x": { "$gt": 1 },
            }
        },
        doc! {"$out": out_coll.name()},
    ];
    out_coll.drop().unwrap();

    coll.aggregate(pipeline.clone(), None).unwrap();
    assert!(db
        .list_collection_names(None)
        .unwrap()
        .into_iter()
        .any(|name| name.as_str() == out_coll.name()));
    out_coll.drop().unwrap();

    // check that even with a batch size of 0, a new collection is created.
    coll.aggregate(
        pipeline,
        Some(AggregateOptions::builder().batch_size(0).build()),
    )
    .unwrap();
    assert!(db
        .list_collection_names(None)
        .unwrap()
        .into_iter()
        .any(|name| name.as_str() == out_coll.name()));
}

#[test]
#[function_name]
fn large_insert() {
    #[allow(clippy::unreadable_literal)]
    let doc = doc! {
        "text": "@wildfits you're not getting one.....",
        "in_reply_to_status_id": 22773233453i64,
        "retweet_count": Bson::Null,
        "contributors": Bson::Null,
        "created_at": "Thu Sep 02 19:38:18 +0000 2010",
        "geo": Bson::Null,
        "source": "web",
        "coordinates": Bson::Null,
        "in_reply_to_screen_name": "wildfits",
        "truncated": false,
        "entities": {
            "user_mentions": [
                {
                    "indices": [
                        0,
                        9
                    ],
                    "screen_name": "wildfits",
                    "name": "Mairin Goetzinger",
                    "id": 41832464
                }
            ],
            "urls": [],
            "hashtags": []
        },
        "retweeted": false,
        "place": Bson::Null,
        "user": {
            "friends_count": 179,
            "profile_sidebar_fill_color": "7a7a7a",
            "location": "Minneapols, MN/Brookings SD",
            "verified": false,
            "follow_request_sent": Bson::Null,
            "favourites_count": 0,
            "profile_sidebar_border_color": "a3a3a3",
            "profile_image_url": "http://a1.twimg.com/profile_images/1110614677/Screen_shot_2010-08-25_at_10.12.40_AM_normal.png",
            "geo_enabled": false,
            "created_at": "Sun Aug 17 00:23:13 +0000 2008",
            "description": "graphic designer + foodie, with a love of music, movies, running, design, + the outdoors!",
            "time_zone": "Mountain Time (US & Canada)",
            "url": "http://jessiefarris.com/",
            "screen_name": "jessiekf",
            "notifications": Bson::Null,
            "profile_background_color": "303030",
            "listed_count": 1,
            "lang": "en"
        }
    };

    let docs = vec![doc; 35000];

    let coll = crate::init_db_and_coll(function_name!(), function_name!());
    coll.insert_many(docs, None).unwrap();
}
