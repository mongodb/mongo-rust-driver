use crate::{bson::Document, test::{Event, CommandEvent}};
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ExpectedEvent {
    Cmap,
    Command(ExpectedCommandEvent),
    Sdam,
}

impl From<Event> for ExpectedEvent {
    fn from(event: Event) -> Self {
        match event {
            Event::Command(sub) => ExpectedEvent::Command(sub.into()),
            Event::Cmap(_) => ExpectedEvent::Cmap,
            Event::Sdam(_) => ExpectedEvent::Sdam,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum ExpectedCommandEvent {
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

impl From<CommandEvent> for ExpectedCommandEvent {
    fn from(event: CommandEvent) -> Self {
        match event {
            CommandEvent::Started(event) => ExpectedCommandEvent::Started {
                command_name: Some(event.command_name),
                database_name: Some(event.db),
                command: Some(event.command),
            },
            CommandEvent::Failed(event) => ExpectedCommandEvent::Failed {
                command_name: Some(event.command_name),
            },
            CommandEvent::Succeeded(event) => ExpectedCommandEvent::Succeeded {
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
    pub fn matches(&self, event: &Event) -> bool {
        match (self, event) {
            (Self::CommandStartedEvent, Event::Command(CommandEvent::Started(_))) => true,
            (Self::CommandSucceededEvent, Event::Command(CommandEvent::Succeeded(_))) => true,
            (Self::CommandFailedEvent, Event::Command(CommandEvent::Failed(_))) => true,
            _ => false,
        }
    }
}