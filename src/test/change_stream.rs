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
async fn track_resume_token() -> Result<(), Box<dyn std::error::Error>> {
    // TODO aegnor: restrict to replica sets and sharded clusters
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>("track_resume_token");
    let mut changes = client.watch(None, None).await?;
    let mut tokens = vec![];
    tokens.push(changes.resume_token().cloned().unwrap().parsed()?);
    for _ in 0..3 {
        coll.insert_one(doc! {}, None).await?;
        changes.next().await.transpose()?;
        tokens.push(changes.resume_token().cloned().unwrap().parsed()?);
    }
    let events: Vec<_> = client.get_command_events(&["aggregate", "getMore"])
        .into_iter()
        .filter_map(|ev| {
            match ev {
                CommandEvent::Succeeded(s) => Some(s),
                _ => None,
            }
        })
        .collect();

    assert_eq!(events.len(), 4);
    assert_eq!(events[0].command_name, "aggregate");
    for n in 1..4 {
        assert_eq!(events[n].command_name, "getMore");
    }
    let expected_tokens: Vec<_> = events.iter()
        .map(expected_token)
        .collect();
    assert_eq!(tokens, expected_tokens);

    Ok(())
}

fn expected_token(ev: &CommandSucceededEvent) -> Bson {
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