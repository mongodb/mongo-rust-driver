use std::time::Duration;

use futures::{future::Either, StreamExt, TryStreamExt};
use tokio::sync::RwLockReadGuard;

use crate::{
    bson::doc,
    options::{CreateCollectionOptions, CursorType, FindOptions},
    test::{util::EventClient, TestClient, LOCK},
    RUNTIME,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn tailable_cursor() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

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

    let delay = RUNTIME.delay_for(await_time);
    let next_doc = cursor.next();

    let next_doc = match futures::future::select(Box::pin(delay), Box::pin(next_doc)).await {
        Either::Left((_, next_doc)) => next_doc,
        Either::Right((next_doc, _)) => panic!(
            "should have timed out before getting next document, but instead immediately got
        {:?}",
            next_doc
        ),
    };

    RUNTIME.execute(async move {
        coll.insert_one(doc! { "_id": 5 }, None).await.unwrap();
    });

    let delay = RUNTIME.delay_for(await_time);

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
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    let mut session = client.start_session(None).await.unwrap();

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
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;
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
