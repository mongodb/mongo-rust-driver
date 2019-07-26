use std::sync::{Arc, RwLock};

use mongodb::{
    event::{CommandEventHandler, CommandStartedEvent},
    Client,
};

use super::TestClient;

pub type Events = Arc<RwLock<Vec<CommandStartedEvent>>>;

#[derive(Default)]
pub struct EventHandler {
    pub events: Events,
}

impl CommandEventHandler for EventHandler {
    fn handle_command_started_event(&self, event: CommandStartedEvent) {
        self.events.write().unwrap().push(event)
    }
}

pub struct EventClient {
    client: TestClient,
    #[allow(dead_code)]
    pub events: Events,
}

impl std::ops::Deref for EventClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.client.deref()
    }
}

impl EventClient {
    #[allow(dead_code)]
    pub fn new() -> Self {
        let handler = EventHandler::default();
        let events = handler.events.clone();
        let client = TestClient::with_handler(Some(handler));

        Self { client, events }
    }
}

#[test]
fn event_count() {
    let client = EventClient::new();
    let coll = client.database("foo").collection("bar");

    for i in 0..10 {
        coll.insert_one(doc! { "x": i }, None).unwrap();
    }

    assert_eq!(
        client
            .events
            .read()
            .unwrap()
            .iter()
            .filter(|event| event.command_name == "insert")
            .count(),
        10
    );
}
