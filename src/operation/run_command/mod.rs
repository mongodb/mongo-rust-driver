#[cfg(test)]
mod test;

use bson::{Bson, Timestamp};

use super::Operation;
use crate::{
    bson::Document,
    client::{ClusterTime, SESSIONS_UNSUPPORTED_COMMANDS},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{ErrorKind, Result},
    options::WriteConcern,
    selection_criteria::SelectionCriteria,
};

#[derive(Debug)]
pub(crate) struct RunCommand {
    db: String,
    command: Document,
    selection_criteria: Option<SelectionCriteria>,
    write_concern: Option<WriteConcern>,
}

impl RunCommand {
    pub(crate) fn new(
        db: String,
        command: Document,
        selection_criteria: Option<SelectionCriteria>,
    ) -> Result<Self> {
        let write_concern = command
            .get("writeConcern")
            .map(|doc| bson::from_bson::<WriteConcern>(doc.clone()))
            .transpose()?;

        Ok(Self {
            db,
            command,
            selection_criteria,
            write_concern,
        })
    }

    fn command_name(&self) -> Option<&str> {
        self.command.keys().next().map(String::as_str)
    }
}

impl Operation for RunCommand {
    type O = Document;
    type Response = Response;

    // Since we can't actually specify a string statically here, we just put a descriptive string
    // that should fail loudly if accidentally passed to the server.
    const NAME: &'static str = "$genericRunCommand";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let command_name = self
            .command_name()
            .ok_or_else(|| ErrorKind::InvalidArgument {
                message: "an empty document cannot be passed to a run_command operation".into(),
            })?;

        Ok(Command::new(
            command_name.to_string(),
            self.db.clone(),
            self.command.clone(),
        ))
    }

    fn handle_response(
        &self,
        response: Document,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        Ok(response)
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.selection_criteria.as_ref()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.write_concern.as_ref()
    }

    fn supports_sessions(&self) -> bool {
        self.command_name()
            .map(|command_name| {
                !SESSIONS_UNSUPPORTED_COMMANDS.contains(command_name.to_lowercase().as_str())
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
pub(crate) struct Response {
    doc: Document,
    cluster_time: Option<ClusterTime>,
}

impl super::Response for Response {
    type Body = Document;

    fn deserialize_response(raw: &RawCommandResponse) -> Result<Self> {
        let doc: Document = raw.body()?;

        let cluster_time = doc
            .get_document("$clusterTime")
            .ok()
            .and_then(|doc| bson::from_document(doc.clone()).ok());

        Ok(Self { doc, cluster_time })
    }

    fn ok(&self) -> Option<&Bson> {
        self.doc.get("ok")
    }

    fn cluster_time(&self) -> Option<&ClusterTime> {
        self.cluster_time.as_ref()
    }

    fn at_cluster_time(&self) -> Option<Timestamp> {
        self.doc
            .get_timestamp("atClusterTime")
            .or_else(|_| {
                self.doc
                    .get_document("cursor")
                    .and_then(|subdoc| subdoc.get_timestamp("atClusterTime"))
            })
            .ok()
    }

    fn into_body(self) -> Self::Body {
        self.doc
    }
}
