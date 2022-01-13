use bson::{Document, doc, Bson};
use futures_util::StreamExt;
use tokio::sync::RwLockReadGuard;

use crate::{
    test::CommandEvent, event::command::CommandSucceededEvent,
};

use super::{LOCK, EventClient};

/// Prose test 1: ChangeStream must continuously track the last seen resumeToken
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn tracks_resume_token() -> Result<(), Box<dyn std::error::Error>> {
    // TODO aegnor: restrict to replica sets and sharded clusters
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>("track_resume_token");
    let mut stream = coll.watch(None, None).await?;
    let mut tokens = vec![];
    tokens.push(stream.resume_token().cloned().unwrap().parsed()?);
    for _ in 0..3 {
        coll.insert_one(doc! {}, None).await?;
        stream.next().await.transpose()?;
        tokens.push(stream.resume_token().cloned().unwrap().parsed()?);
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
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

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