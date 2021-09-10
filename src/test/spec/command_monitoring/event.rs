use serde::Deserialize;

use crate::{
    bson::Document,
    test::{CommandEvent, Matchable, MatchErrExt, eq_matches},
};

#[derive(Debug, Deserialize, PartialEq)]
pub enum TestEvent {
    #[serde(rename = "command_started_event")]
    Started {
        command_name: String,
        database_name: String,
        command: Document,
    },
    #[serde(rename = "command_succeeded_event")]
    Succeeded {
        command_name: String,
        reply: Document,
    },
    #[serde(rename = "command_failed_event")]
    Failed { command_name: String },
}

impl Matchable for TestEvent {
    fn content_matches(&self, actual: &TestEvent) -> Result<(), String> {
        match (self, actual) {
            (
                TestEvent::Started {
                    command_name: actual_command_name,
                    database_name: actual_database_name,
                    command: actual_command,
                },
                TestEvent::Started {
                    command_name: expected_command_name,
                    database_name: expected_database_name,
                    command: expected_command,
                },
            ) => {
                eq_matches("command_name", actual_command_name, expected_command_name)?;
                eq_matches("database_name", actual_database_name, expected_database_name)?;
                actual_command.matches(expected_command).prefix("command")?;
                Ok(())
            }
            (
                TestEvent::Succeeded {
                    command_name: actual_command_name,
                    reply: actual_reply,
                },
                TestEvent::Succeeded {
                    command_name: expected_command_name,
                    reply: expected_reply,
                },
            ) => {
                eq_matches("command_name", actual_command_name, expected_command_name)?;
                actual_reply.matches(expected_reply).prefix("reply")?;
                Ok(())
            }
            (
                TestEvent::Failed {
                    command_name: actual_command_name,
                },
                TestEvent::Failed {
                    command_name: expected_command_name,
                },
            ) => eq_matches("command_name", actual_command_name, expected_command_name),
            _ => Err(format!("expected event {:?}, got {:?}", self, actual)),
        }
    }
}

impl From<CommandEvent> for TestEvent {
    fn from(event: CommandEvent) -> Self {
        match event {
            CommandEvent::Started(event) => TestEvent::Started {
                command_name: event.command_name,
                database_name: event.db,
                command: event.command,
            },
            CommandEvent::Failed(event) => TestEvent::Failed {
                command_name: event.command_name,
            },
            CommandEvent::Succeeded(event) => TestEvent::Succeeded {
                command_name: event.command_name,
                reply: event.reply,
            },
        }
    }
}
