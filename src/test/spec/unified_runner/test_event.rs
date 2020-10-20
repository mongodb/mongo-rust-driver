use crate::{
    bson::{Bson, Document},
    test::{CommandEvent, Matchable},
};
use serde::Deserialize;

use super::results_match;

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
                if let Some(expected_command) = expected_command {
                    let actual_command = actual_command
                        .as_ref()
                        .map(|doc| Bson::Document(doc.clone()));
                    results_match(
                        actual_command.as_ref(),
                        &Bson::Document(expected_command.clone()),
                    )
                } else {
                    true
                }
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
                if expected_command_name.is_some() && actual_command_name != expected_command_name {
                    return false;
                }
                if let Some(expected_reply) = expected_reply {
                    let actual_reply = actual_reply.as_ref().map(|doc| Bson::Document(doc.clone()));
                    results_match(
                        actual_reply.as_ref(),
                        &Bson::Document(expected_reply.clone()),
                    )
                } else {
                    true
                }
            }
            (
                TestEvent::CommandFailedEvent {
                    command_name: actual_command_name,
                },
                TestEvent::CommandFailedEvent {
                    command_name: expected_command_name,
                },
            ) => match (expected_command_name, actual_command_name) {
                (Some(expected), Some(actual)) => expected == actual,
                (Some(_), None) => false,
                _ => true,
            },
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
