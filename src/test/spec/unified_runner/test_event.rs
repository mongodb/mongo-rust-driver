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
