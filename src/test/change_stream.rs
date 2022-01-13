use bson::{Document, doc, Bson};
use futures_util::StreamExt;

use crate::{
    test::{CommandEvent, FailPoint, FailPointMode, FailCommandOptions},
    event::command::CommandSucceededEvent,
    change_stream::event::{ChangeStreamEvent, OperationType},
};

use super::{LOCK, EventClient};

/// Prose test 1: ChangeStream must continuously track the last seen resumeToken
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn tracks_resume_token() -> Result<(), Box<dyn std::error::Error>> {
    // TODO aegnor: restrict to replica sets and sharded clusters
    let _guard = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>("track_resume_token");
    let mut stream = coll.watch(None, None).await?;
    let mut tokens = vec![];
    tokens.push(stream.resume_token().unwrap().parsed()?);
    for _ in 0..3 {
        coll.insert_one(doc! {}, None).await?;
        stream.next().await.transpose()?;
        tokens.push(stream.resume_token().unwrap().parsed()?);
    }

    let expected_tokens: Vec<_> = client.get_command_events(&["aggregate", "getMore"])
        .into_iter()
        .filter_map(|ev| {
            match ev {
                CommandEvent::Succeeded(s) => Some(s),
                _ => None,
            }
        })
        .map(expected_token)
        .collect();
    assert_eq!(tokens, expected_tokens);

    Ok(())
}

fn expected_token(ev: CommandSucceededEvent) -> Bson {
    let cursor = ev.reply.get_document("cursor").unwrap();
    if let Some(token) = cursor.get("postBatchResumeToken") {
        token.clone()
    } else {
        cursor
            .get_array("nextBatch")
            .unwrap()[0]
            .as_document()
            .unwrap()
            .get("_id")
            .unwrap()
            .clone()
    }
}

/// Prose test 2: ChangeStream will throw an exception if the server response is missing the resume token
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn errors_on_missing_token() -> Result<(), Box<dyn std::error::Error>> {
    // TODO aegnor: restrict to replica sets and sharded clusters
    let _guard = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>("errors_on_missing_token");
    let mut stream = coll.watch(vec![
        doc! { "$project": { "_id": 0 } },
    ], None).await?;
    coll.insert_one(doc! {}, None).await?;
    assert!(stream.next().await.transpose().is_err());

    Ok(())
}

/// Prose test 3: After receiving a resumeToken, ChangeStream will automatically resume one time on a resumable error
// Using the multi_thread flavor of tokio is required for FailPoint.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn resumes_on_error() -> Result<(), Box<dyn std::error::Error>> {
    // TODO aegnor: restrict to replica sets and sharded clusters
    let _guard = LOCK.run_exclusively().await;

    let client = EventClient::new().await;
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>("resumes_on_error");
    coll.drop(None).await?;
    let mut stream = coll.watch(None, None).await?;

    coll.insert_one(doc! { "_id": 1 }, None).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 1 }
    ));

    let _guard = FailPoint::fail_command(
        &["getMore"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .error_code(43)
            .build(),
    ).enable(&client, None).await?;

    coll.insert_one(doc! { "_id": 2 }, None).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 2 }
    ));

    Ok(())
}