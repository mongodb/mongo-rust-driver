use std::sync::{Arc, RwLock};

use bson::doc;

use super::TestClient;
use crate::{
    event::{
        cmap::{CmapEventHandler, PoolClearedEvent},
        command::{
            CommandEventHandler,
            CommandFailedEvent,
            CommandStartedEvent,
            CommandSucceededEvent,
        },
    },
    test::LOCK,
    Collection,
};

pub type EventQueue<T> = Arc<RwLock<Vec<T>>>;

#[derive(Debug)]
pub enum CommandEvent {
    CommandStartedEvent(CommandStartedEvent),
    CommandSucceededEvent(CommandSucceededEvent),
    CommandFailedEvent(CommandFailedEvent),
}

impl CommandEvent {
    fn command_name(&self) -> &str {
        match self {
            CommandEvent::CommandStartedEvent(event) => event.command_name.as_str(),
            CommandEvent::CommandFailedEvent(event) => event.command_name.as_str(),
            CommandEvent::CommandSucceededEvent(event) => event.command_name.as_str(),
        }
    }

    fn is_command_started(&self) -> bool {
        match self {
            CommandEvent::CommandStartedEvent(_) => true,
            _ => false,
        }
    }
}

#[derive(Default)]
pub struct EventHandler {
    pub command_events: EventQueue<CommandEvent>,
    pub pool_cleared_events: EventQueue<PoolClearedEvent>,
}

impl CmapEventHandler for EventHandler {
    fn handle_pool_cleared_event(&self, event: PoolClearedEvent) {
        self.pool_cleared_events.write().unwrap().push(event)
    }
}

impl CommandEventHandler for EventHandler {
    fn handle_command_started_event(&self, event: CommandStartedEvent) {
        self.command_events
            .write()
            .unwrap()
            .push(CommandEvent::CommandStartedEvent(event))
    }

    fn handle_command_failed_event(&self, event: CommandFailedEvent) {
        self.command_events
            .write()
            .unwrap()
            .push(CommandEvent::CommandFailedEvent(event))
    }

    fn handle_command_succeeded_event(&self, event: CommandSucceededEvent) {
        self.command_events
            .write()
            .unwrap()
            .push(CommandEvent::CommandSucceededEvent(event))
    }
}

pub struct EventClient {
    client: TestClient,
    pub command_events: EventQueue<CommandEvent>,
    pub pool_cleared_events: EventQueue<PoolClearedEvent>,
}

impl std::ops::Deref for EventClient {
    type Target = TestClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl std::ops::DerefMut for EventClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl EventClient {
    pub fn new() -> Self {
        let handler = EventHandler::default();
        let command_events = handler.command_events.clone();
        let pool_cleared_events = handler.pool_cleared_events.clone();
        let client = TestClient::with_handler(Some(handler));

        Self {
            client,
            command_events,
            pool_cleared_events,
        }
    }

    pub fn run_operation_with_events(
        &self,
        command_names: &[&str],
        database_name: &str,
        collection_name: &str,
        function: impl FnOnce(Collection),
    ) -> Vec<CommandEvent> {
        function(self.database(database_name).collection(collection_name));
        self.command_events
            .write()
            .unwrap()
            .drain(..)
            .filter(|event| command_names.contains(&event.command_name()))
            .collect()
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(core_threads = 2))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_started_event_count() {
    let _guard = LOCK.run_concurrently();

    let client = EventClient::new();
    let coll = client.database("foo").collection("bar");

    for i in 0..10 {
        coll.insert_one(doc! { "x": i }, None).unwrap();
    }

    assert_eq!(
        client
            .command_events
            .read()
            .unwrap()
            .iter()
            .filter(|event| event.is_command_started() && event.command_name() == "insert")
            .count(),
        10
    );
}
