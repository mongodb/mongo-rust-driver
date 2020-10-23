use futures::stream::TryStreamExt;

use super::Cursor;
use crate::{
    bson::{doc, Document},
    operation::{Find, GetMoreResponseBody},
    options::FindOptions,
    test::EventClient,
    Namespace,
};

fn doc_x(i: i32) -> Document {
    doc! { "x": i }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn exhaust_test() {
    let client = EventClient::new().await;
    let events = client.command_events.clone();

    let coll = client
        .init_db_and_coll(function_name!(), function_name!())
        .await;

    let docs = (1i32..=20).map(doc_x);
    coll.insert_many(docs, None).await.unwrap();

    let op = Find::new(
        Namespace {
            db: function_name!().into(),
            coll: function_name!().into(),
        },
        None,
        Some(
            FindOptions::builder()
                .batch_size(5)
                .sort(doc! { "x": 1 })
                .build(),
        ),
    );

    let cursor_response = client.execute_cursor_operation(op).await.unwrap();

    let mut cursor: Cursor = Cursor::new(client.client.into_client(), cursor_response, true);

    for i in 1i32..=20 {
        let doc = cursor.try_next().await.unwrap().unwrap();

        assert_eq!(doc.get_i32("x"), Ok(i));
    }

    assert!(cursor.try_next().await.unwrap().is_none());

    let events_guard = events.read().unwrap();

    // Assert that only one getMore was sent.
    assert_eq!(
        events_guard
            .iter()
            .filter(|event| event.is_command_started() && event.command_name() == "getMore")
            .count(),
        1
    );

    let get_more_replies: Vec<_> = events_guard
        .iter()
        .filter_map(|c| {
            let command_succeeded = c.as_command_succeeded()?;

            if command_succeeded.command_name != "getMore" {
                return None;
            }

            Some(&command_succeeded.reply)
        })
        .collect();

    // Assert that only one explicit getMore response was sent from the server.
    assert_eq!(get_more_replies.len(), 1);

    let GetMoreResponseBody { cursor } = bson::from_document(get_more_replies[0].clone()).unwrap();

    // Assert that only five responses were present in the reply to the explicit getMore.
    assert_eq!(cursor.next_batch.len(), 5);

    // Assert that the last document in the reply to the explicit getMore was the 10th document,
    // meaning the last ten must have come from exhaust replies.
    assert_eq!(cursor.next_batch[4].get_i32("x"), Ok(10));
}
