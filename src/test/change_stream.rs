use bson::{Document, doc};
use tokio::sync::RwLockReadGuard;

use crate::event::command::CommandSucceededEvent;

use super::{LOCK, EventClient};

/// Prose test 1: ChangeStream must continuously track the last seen resumeToken
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn track_resume_token() {
    // TODO aegnor: restrict to replica sets and sharded clusters
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>("track_resume_token");
    let mut changes = client.watch(None, None).await.unwrap();
    dbg!(changes.resume_token());
    for _ in 0..3 {
        coll.insert_one(doc! {}, None).await.unwrap();
        dbg!(changes.next_if_any().await.unwrap());
        dbg!(changes.resume_token());
    }
}

/*
fn expected_resume_token(commands: &[CommandSucceededEvent], events_read: usize) -> Option<Document> {
    let mut token = None;
    let mut commands_checked: usize = 0;
    for event in commands {

    }
    token
}
*/