use std::time::Duration;

use futures::{future::Either, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    bson::doc,
    options::{CreateCollectionOptions, CursorType, FindOptions},
    runtime,
    test::{log_uncaptured, util::EventClient, TestClient, SERVERLESS},
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn tailable_cursor() {
    if *SERVERLESS {
        log_uncaptured(
            "skipping cursor::tailable_cursor; serverless does not support capped collections",
        );
        return;
    }

    let client = TestClient::new().await;
    let coll = client
        .create_fresh_collection(
            function_name!(),
            function_name!(),
            CreateCollectionOptions::builder()
                .capped(true)
                .max(5)
                .size(1_000_000)
                .build(),
        )
        .await;

    coll.insert_many((0..5).map(|i| doc! { "_id": i }), None)
        .await
        .unwrap();

    let await_time = Duration::from_millis(500);

    let mut cursor = coll
        .find(
            None,
            FindOptions::builder()
                .cursor_type(CursorType::TailableAwait)
                .max_await_time(await_time)
                .build(),
        )
        .await
        .unwrap();

    for i in 0..5 {
        assert_eq!(
            cursor.next().await.transpose().unwrap(),
            Some(doc! { "_id": i })
        );
    }

    let delay = runtime::delay_for(await_time);
    let next_doc = cursor.next();

    let next_doc = match futures::future::select(Box::pin(delay), Box::pin(next_doc)).await {
        Either::Left((_, next_doc)) => next_doc,
        Either::Right((next_doc, _)) => panic!(
            "should have timed out before getting next document, but instead immediately got
        {:?}",
            next_doc
        ),
    };

    runtime::execute(async move {
        coll.insert_one(doc! { "_id": 5 }, None).await.unwrap();
    });

    let delay = runtime::delay_for(await_time);

    match futures::future::select(Box::pin(delay), Box::pin(next_doc)).await {
        Either::Left((..)) => panic!("should have gotten next document, but instead timed"),
        Either::Right((next_doc, _)) => {
            assert_eq!(next_doc.transpose().unwrap(), Some(doc! { "_id": 5 }))
        }
    };
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn session_cursor_next() {
    let client = TestClient::new().await;
    let mut session = client.start_session().await.unwrap();

    let coll = client
        .create_fresh_collection(function_name!(), function_name!(), None)
        .await;

    coll.insert_many_with_session((0..5).map(|i| doc! { "_id": i }), None, &mut session)
        .await
        .unwrap();

    let opts = FindOptions::builder().batch_size(1).build();
    let mut cursor = coll
        .find_with_session(None, opts, &mut session)
        .await
        .unwrap();

    for i in 0..5 {
        assert_eq!(
            cursor.next(&mut session).await.transpose().unwrap(),
            Some(doc! { "_id": i })
        );
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn batch_exhaustion() {
    let client = EventClient::new().await;

    let coll = client
        .create_fresh_collection(
            "cursor_batch_exhaustion_db",
            "cursor_batch_exhaustion_coll",
            None,
        )
        .await;
    coll.insert_many(
        vec![
            doc! { "foo": 1 },
            doc! { "foo": 2 },
            doc! { "foo": 3 },
            doc! { "foo": 4 },
            doc! { "foo": 5 },
            doc! { "foo": 6 },
        ],
        None,
    )
    .await
    .unwrap();

    // Start a find where batch size will line up with limit.
    let cursor = coll
        .find(None, FindOptions::builder().batch_size(2).limit(4).build())
        .await
        .unwrap();
    let v: Vec<_> = cursor.try_collect().await.unwrap();
    assert_eq!(4, v.len());

    // Assert that the last `getMore` response always has id 0, i.e. is exhausted.
    let replies: Vec<_> = client
        .get_command_events(&["getMore"])
        .into_iter()
        .filter_map(|e| e.as_command_succeeded().map(|e| e.reply.clone()))
        .collect();
    let last = replies.last().unwrap();
    let cursor = last.get_document("cursor").unwrap();
    let id = cursor.get_i64("id").unwrap();
    assert_eq!(0, id);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn borrowed_deserialization() {
    let client = EventClient::new().await;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Doc<'a> {
        #[serde(rename = "_id")]
        id: i32,

        #[serde(borrow)]
        foo: &'a str,
    }

    let coll = client
        .create_fresh_collection("cursor_borrowed_db", "cursor_borrowed_coll", None)
        .await
        .clone_with_type::<Doc>();

    let docs = vec![
        Doc {
            id: 0,
            foo: "a string",
        },
        Doc {
            id: 1,
            foo: "another string",
        },
        Doc {
            id: 2,
            foo: "more strings",
        },
        Doc {
            id: 3,
            foo: "striiiiiing",
        },
        Doc { id: 4, foo: "s" },
        Doc { id: 5, foo: "1" },
    ];

    coll.insert_many(&docs, None).await.unwrap();

    let options = FindOptions::builder()
        .batch_size(2)
        .sort(doc! { "_id": 1 })
        .build();

    let mut cursor = coll.find(None, options.clone()).await.unwrap();

    let mut i = 0;
    while cursor.advance().await.unwrap() {
        let de = cursor.deserialize_current().unwrap();
        assert_eq!(de, docs[i]);
        i += 1;
    }

    let mut session = client.start_session().await.unwrap();
    let mut cursor = coll
        .find_with_session(None, options.clone(), &mut session)
        .await
        .unwrap();

    let mut i = 0;
    while cursor.advance(&mut session).await.unwrap() {
        let de = cursor.deserialize_current().unwrap();
        assert_eq!(de, docs[i]);
        i += 1;
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn session_cursor_with_type() {
    let client = TestClient::new().await;

    let mut session = client.start_session().await.unwrap();
    let coll = client.database("db").collection("coll");
    coll.drop_with_session(None, &mut session).await.unwrap();

    coll.insert_many_with_session(
        vec![doc! { "x": 1 }, doc! { "x": 2 }, doc! { "x": 3 }],
        None,
        &mut session,
    )
    .await
    .unwrap();

    let mut cursor: crate::SessionCursor<bson::Document> = coll
        .find_with_session(doc! {}, None, &mut session)
        .await
        .unwrap();

    let _ = cursor.next(&mut session).await.unwrap().unwrap();

    let mut cursor_with_type: crate::SessionCursor<bson::RawDocumentBuf> = cursor.with_type();

    let _ = cursor_with_type.next(&mut session).await.unwrap().unwrap();
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn cursor_final_batch() {
    let client = TestClient::new().await;
    let coll = client
        .create_fresh_collection("test_cursor_final_batch", "test", None)
        .await;
    coll.insert_many(
        vec![
            doc! { "foo": 1 },
            doc! { "foo": 2 },
            doc! { "foo": 3 },
            doc! { "foo": 4 },
            doc! { "foo": 5 },
        ],
        None,
    )
    .await
    .unwrap();

    let mut cursor = coll
        .find(None, FindOptions::builder().batch_size(3).build())
        .await
        .unwrap();
    let mut found = 0;
    while cursor.advance().await.unwrap() {
        found += 1;
    }
    assert_eq!(found, 5);
}
