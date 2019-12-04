use std::sync::{Arc, RwLock};

use bson::{bson, doc, Document};
use serde::Deserialize;

use super::{Matchable, TestClient};
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

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TestEvent {
    CommandStartedEvent {
        command_name: String,
        database_name: String,
        command: Document,
    },

    CommandSucceededEvent {
        command_name: String,
        reply: Document,
    },

    CommandFailedEvent {
        command_name: String,
    },
}

impl TestEvent {
    fn command_name(&self) -> &str {
        match self {
            TestEvent::CommandStartedEvent {
                ref command_name, ..
            } => command_name.as_str(),
            TestEvent::CommandSucceededEvent {
                ref command_name, ..
            } => command_name.as_str(),
            TestEvent::CommandFailedEvent { ref command_name } => command_name.as_str(),
        }
    }

    fn is_command_started(&self) -> bool {
        match self {
            TestEvent::CommandStartedEvent { .. } => true,
            _ => false,
        }
    }
}

impl Matchable for TestEvent {
    fn content_matches(&self, actual: &TestEvent) -> bool {
        match (self, actual) {
            (
                TestEvent::CommandStartedEvent {
                    command_name: actual_command_name,
                    database_name: actual_database_name,
                    command: actual_command,
                },
                TestEvent::CommandStartedEvent {
                    command_name: expected_command_name,
                    database_name: expected_database_name,
                    command: expected_command,
                },
            ) => {
                actual_command_name == expected_command_name
                    && actual_database_name == expected_database_name
                    && actual_command.matches(expected_command)
            }
            (
                TestEvent::CommandSucceededEvent {
                    command_name: actual_command_name,
                    reply: actual_reply,
                },
                TestEvent::CommandSucceededEvent {
                    command_name: expected_command_name,
                    reply: expected_reply,
                },
            ) => {
                actual_command_name == expected_command_name && actual_reply.matches(expected_reply)
            }
            (
                TestEvent::CommandFailedEvent {
                    command_name: actual_command_name,
                },
                TestEvent::CommandFailedEvent {
                    command_name: expected_command_name,
                },
            ) => actual_command_name == expected_command_name,
            _ => false,
        }
    }
}

impl From<CommandStartedEvent> for TestEvent {
    fn from(event: CommandStartedEvent) -> Self {
        TestEvent::CommandStartedEvent {
            command: event.command,
            command_name: event.command_name,
            database_name: event.db,
        }
    }
}

impl From<CommandFailedEvent> for TestEvent {
    fn from(event: CommandFailedEvent) -> Self {
        TestEvent::CommandFailedEvent {
            command_name: event.command_name,
        }
    }
}

impl From<CommandSucceededEvent> for TestEvent {
    fn from(event: CommandSucceededEvent) -> Self {
        TestEvent::CommandSucceededEvent {
            command_name: event.command_name,
            reply: event.reply,
        }
    }
}

#[derive(Default)]
pub struct EventHandler {
    pub command_events: EventQueue<TestEvent>,
    pub pool_cleared_events: EventQueue<PoolClearedEvent>,
}

impl CmapEventHandler for EventHandler {
    fn handle_pool_cleared_event(&self, event: PoolClearedEvent) {
        self.pool_cleared_events.write().unwrap().push(event)
    }
}

impl CommandEventHandler for EventHandler {
    fn handle_command_started_event(&self, event: CommandStartedEvent) {
        self.command_events.write().unwrap().push(event.into())
    }

    fn handle_command_failed_event(&self, event: CommandFailedEvent) {
        self.command_events.write().unwrap().push(event.into())
    }

    fn handle_command_succeeded_event(&self, event: CommandSucceededEvent) {
        self.command_events.write().unwrap().push(event.into())
    }
}

pub struct EventClient {
    client: TestClient,
    #[allow(dead_code)]
    pub command_events: EventQueue<TestEvent>,
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
    ) -> Vec<TestEvent> {
        function(self.database(database_name).collection(collection_name));
        self.command_events
            .write()
            .unwrap()
            .drain(..)
            .filter(|event| command_names.contains(&event.command_name()))
            .collect()
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
            .command_events
            .read()
            .unwrap()
            .iter()
            .filter(|event| event.is_command_started() && event.command_name() == "insert")
            .count(),
        10
    );
}
