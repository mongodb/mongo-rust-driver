use std::sync::Arc;

use bson::{Document, doc};

use crate::{Client, test::{CLIENT_OPTIONS, EventHandler}};

type Result<T> = anyhow::Result<T>;

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_tests() -> Result<()> {
    check_faas_handshake(&doc! { }).await?;
    
    Ok(())
}

async fn check_faas_handshake(_expected: &Document) -> Result<()> {
    let handler = Arc::new(EventHandler::new());
    let mut options = CLIENT_OPTIONS.get().await.clone();
    options.command_event_handler = Some(handler.clone());
    let client = Client::with_options(options)?;
    client.list_database_names(doc! { }, None).await?;

    let events = handler.get_all_command_started_events();
    dbg!(events);

    Ok(())
}