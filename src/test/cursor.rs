use std::time::Duration;

use bson::doc;
use futures::{future::Either, StreamExt};

use crate::{
    options::{CreateCollectionOptions, CursorType, FindOptions},
    test::{TestClient, LOCK},
    RUNTIME,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn tailable_cursor() {
    let _guard = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    client
        .drop_collection(function_name!(), function_name!())
        .await;

    let db = client.database(function_name!());

    db.create_collection(
        function_name!(),
        CreateCollectionOptions::builder()
            .capped(true)
            .max(5)
            .size(1_000_000)
            .build(),
    )
    .await
    .unwrap();

    let coll = db.collection(function_name!());

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

    coll.insert_one(doc! { "_id": 5 }, None).await.unwrap();

    let delay = RUNTIME.delay_for(await_time);

    match futures::future::select(Box::pin(delay), Box::pin(next_doc)).await {
        Either::Left((_, next_doc)) => {
            panic!("should have gotten next document, but instead timed")
        }
        Either::Right((next_doc, _)) => {
            assert_eq!(next_doc.transpose().unwrap(), Some(doc! { "_id": 5 }))
        }
    };
}
