use crate::{bson::Document, test::CommandEvent};
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum TestEvent {
    CommandStartedEvent {
        command_name: Option<String>,
        database_name: Option<String>,
        command: Option<Document>,
    },

    CommandSucceededEvent {
        command_name: Option<String>,
        reply: Option<Document>,
    },

    CommandFailedEvent {
        command_name: Option<String>,
    },
}

impl From<CommandEvent> for TestEvent {
    fn from(event: CommandEvent) -> Self {
        match event {
            CommandEvent::CommandStartedEvent(event) => TestEvent::CommandStartedEvent {
                command_name: Some(event.command_name),
                database_name: Some(event.db),
                command: Some(event.command),
            },
            CommandEvent::CommandFailedEvent(event) => TestEvent::CommandFailedEvent {
                command_name: Some(event.command_name),
            },
            CommandEvent::CommandSucceededEvent(event) => TestEvent::CommandSucceededEvent {
                command_name: Some(event.command_name),
                reply: Some(event.reply),
            },
        }
    }
}
