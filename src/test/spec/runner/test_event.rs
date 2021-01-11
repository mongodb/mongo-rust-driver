use crate::{bson::Document, event::command::CommandStartedEvent, test::Matchable};
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TestEvent {
    CommandStartedEvent {
        command_name: Option<String>,
        database_name: Option<String>,
        command: Document,
    },
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
                if expected_command_name.is_some() && actual_command_name != expected_command_name {
                    return false;
                }
                if expected_database_name.is_some()
                    && actual_database_name != expected_database_name
                {
                    return false;
                }
                actual_command.matches(expected_command)
            }
        }
    }
}

impl From<CommandStartedEvent> for TestEvent {
    fn from(event: CommandStartedEvent) -> Self {
        TestEvent::CommandStartedEvent {
            command_name: Some(event.command_name),
            database_name: Some(event.db),
            command: event.command,
        }
    }
}
