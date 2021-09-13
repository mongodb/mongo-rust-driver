use crate::{bson::Document, test::CommandEvent};
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
pub enum TestEvent {
    #[serde(rename = "commandStartedEvent")]
    Started {
        command_name: Option<String>,
        database_name: Option<String>,
        command: Option<Document>,
    },
    #[serde(rename = "commandSucceededEvent")]
    Succeeded {
        command_name: Option<String>,
        reply: Option<Document>,
    },
    #[serde(rename = "commandFailedEvent")]
    Failed { command_name: Option<String> },
}

impl From<CommandEvent> for TestEvent {
    fn from(event: CommandEvent) -> Self {
        match event {
            CommandEvent::Started(event) => TestEvent::Started {
                command_name: Some(event.command_name),
                database_name: Some(event.db),
                command: Some(event.command),
            },
            CommandEvent::Failed(event) => TestEvent::Failed {
                command_name: Some(event.command_name),
            },
            CommandEvent::Succeeded(event) => TestEvent::Succeeded {
                command_name: Some(event.command_name),
                reply: Some(event.reply),
            },
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ObserveEvent {
    CommandStartedEvent,
    CommandSucceededEvent,
    CommandFailedEvent,
}

impl ObserveEvent {
    pub fn matches(&self, event: &CommandEvent) -> bool {
        match (self, event) {
            (Self::CommandStartedEvent, CommandEvent::Started(_)) => true,
            (Self::CommandSucceededEvent, CommandEvent::Succeeded(_)) => true,
            (Self::CommandFailedEvent, CommandEvent::Failed(_)) => true,
            _ => false,
        }
    }
}