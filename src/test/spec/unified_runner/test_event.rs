use crate::{
    bson::Document,
    test::{CommandEvent, Matchable},
};
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum TestEvent {
    CommandStartedEvent {
        command_name: Option<String>,
        database_name: Option<String>,
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

impl From<CommandEvent> for TestEvent {
    fn from(event: CommandEvent) -> Self {
        match event {
            CommandEvent::CommandStartedEvent(event) => TestEvent::CommandStartedEvent {
                command_name: Some(event.command_name),
                database_name: Some(event.db),
                command: event.command,
            },
            CommandEvent::CommandFailedEvent(event) => TestEvent::CommandFailedEvent {
                command_name: event.command_name,
            },
            CommandEvent::CommandSucceededEvent(event) => TestEvent::CommandSucceededEvent {
                command_name: event.command_name,
                reply: event.reply,
            },
        }
    }
}
