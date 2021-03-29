use crate::{bson::Document, event, test::Matchable};
use bson::Bson;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandStartedEvent {
    command_name: Option<String>,
    database_name: Option<String>,
    command: Document,
}

impl CommandStartedEvent {
    pub fn matches_expected(
        &self,
        expected: &CommandStartedEvent,
        session0_lsid: &Document,
        session1_lsid: &Document,
    ) -> bool {
        if expected.command_name.is_some() && self.command_name != expected.command_name {
            return false;
        }
        if expected.database_name.is_some() && self.database_name != expected.database_name {
            return false;
        }
        let mut expected = expected.command.clone();
        if let Some(Bson::String(session)) = expected.remove("lsid") {
            match session.as_str() {
                "session0" => {
                    expected.insert("lsid", session0_lsid.clone());
                }
                "session1" => {
                    expected.insert("lsid", session1_lsid.clone());
                }
                other => panic!("unknown session name: {}", other),
            }
        }
        self.command.content_matches(&expected)
    }
}

impl From<event::command::CommandStartedEvent> for CommandStartedEvent {
    fn from(event: event::command::CommandStartedEvent) -> Self {
        CommandStartedEvent {
            command_name: Some(event.command_name),
            database_name: Some(event.db),
            command: event.command,
        }
    }
}
