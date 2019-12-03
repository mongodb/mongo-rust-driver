use std::sync::{Arc, RwLock};

use bson::{bson, doc};

use super::TestClient;
use crate::{
    event::{
        cmap::{CmapEventHandler, PoolClearedEvent},
        command::{CommandEventHandler, CommandStartedEvent},
    },
    test::LOCK,
};

pub type EventQueue<T> = Arc<RwLock<Vec<T>>>;

#[derive(Default)]
pub struct EventHandler {
    pub command_started_events: EventQueue<CommandStartedEvent>,
    pub pool_cleared_events: EventQueue<PoolClearedEvent>,
}

impl CmapEventHandler for EventHandler {
    fn handle_pool_cleared_event(&self, event: PoolClearedEvent) {
        self.pool_cleared_events.write().unwrap().push(event)
    }
}

impl CommandEventHandler for EventHandler {
    fn handle_command_started_event(&self, event: CommandStartedEvent) {
        self.command_started_events.write().unwrap().push(event)
    }
}

pub struct EventClient {
    client: TestClient,
    #[allow(dead_code)]
    pub command_started_events: EventQueue<CommandStartedEvent>,
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
        let command_started_events = handler.command_started_events.clone();
        let pool_cleared_events = handler.pool_cleared_events.clone();
        let client = TestClient::with_handler(Some(handler));

        Self {
            client,
            command_started_events,
            pool_cleared_events,
        }
    }
}

// TODO RUST-185: Enable once command monitoring is implemented.
//#[test]
#[allow(dead_code)]
fn command_started_event_count() {
    let _guard = LOCK.run_concurrently();

    let client = EventClient::new();
    let coll = client.database("foo").collection("bar");

    for i in 0..10 {
        coll.insert_one(doc! { "x": i }, None).unwrap();
    }

    assert_eq!(
        client
            .command_started_events
            .read()
            .unwrap()
            .iter()
            .filter(|event| event.command_name == "insert")
            .count(),
        10
    );
}
