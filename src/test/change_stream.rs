use bson::Document;
use tokio::sync::RwLockReadGuard;

use super::{LOCK, EventClient};

/// Prose test 1: ChangeStream must continuously track the last seen resumeToken
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn track_resume_token() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>("track_resume_token");
}