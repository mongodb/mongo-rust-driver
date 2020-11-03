use serde::de::DeserializeOwned;

use super::wire::Message;
use crate::{
    bson::{Bson, Document},
    bson_util,
    client::{ClientSession, ClusterTime, options::ServerApi},
    error::{CommandError, ErrorKind, Result},
    options::StreamAddress,
    selection_criteria::ReadPreference,
};

/// `Command` is a driver side abstraction of a server command containing all the information
/// necessary to serialize it to a wire message.
#[derive(Debug, Clone)]
pub(crate) struct Command {
    pub(crate) name: String,
    pub(crate) target_db: String,
    pub(crate) read_pref: Option<ReadPreference>,
    pub(crate) body: Document,
}

impl Command {
    /// Constructs a read command.
    pub(crate) fn new_read(
        name: String,
        target_db: String,
        read_pref: Option<ReadPreference>,
        body: Document,
    ) -> Self {
        Self {
            name,
            target_db,
            read_pref,
            body,
        }
    }

    /// Constructs a new command.
    pub(crate) fn new(name: String, target_db: String, body: Document) -> Self {
        Self {
            name,
            target_db,
            read_pref: None,
            body,
        }
    }

    pub(crate) fn set_session(&mut self, session: &ClientSession) {
        self.body.insert("lsid", session.id());
    }

    pub(crate) fn set_cluster_time(&mut self, cluster_time: &ClusterTime) {
        // this should never fail.
        if let Ok(doc) = bson::to_bson(cluster_time) {
            self.body.insert("$clusterTime", doc);
        }
    }

    pub(crate) fn set_txn_number(&mut self, txn_number: u64) {
        self.body.insert("txnNumber", txn_number);
    }

    pub(crate) fn set_server_api(&mut self, server_api: &ServerApi) {
        if server_api.applies_to_command(self.name.as_str()) {
            server_api.append_to_command(&mut self.body);
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CommandResponse {
    source: StreamAddress,
    pub(crate) raw_response: Document,
    cluster_time: Option<ClusterTime>,
}

impl CommandResponse {
    #[cfg(test)]
    pub(crate) fn with_document_and_address(source: StreamAddress, doc: Document) -> Self {
        Self {
            source,
            raw_response: doc,
            cluster_time: None,
        }
    }

    /// Initialize a response from a document.
    #[cfg(test)]
    pub(crate) fn with_document(doc: Document) -> Self {
        Self::with_document_and_address(
            StreamAddress {
                hostname: "localhost".to_string(),
                port: None,
            },
            doc,
        )
    }

    pub(crate) fn new(source: StreamAddress, message: Message) -> Result<Self> {
        let raw_response = message.single_document_response()?;
        let cluster_time = raw_response
            .get("$clusterTime")
            .and_then(|subdoc| bson::from_bson(subdoc.clone()).ok());

        Ok(Self {
            source,
            raw_response,
            cluster_time,
        })
    }

    /// Returns whether this response indicates a success or not (i.e. if "ok: 1")
    pub(crate) fn is_success(&self) -> bool {
        match self.raw_response.get("ok") {
            Some(ref b) => bson_util::get_int(b) == Some(1),
            _ => false,
        }
    }

    /// Retunrs a result indicating whether this response corresponds to a command failure.
    pub(crate) fn validate(&self) -> Result<()> {
        if !self.is_success() {
            let command_error: CommandError =
                bson::from_bson(Bson::Document(self.raw_response.clone())).map_err(|_| {
                    ErrorKind::ResponseError {
                        message: "invalid server response".to_string(),
                    }
                })?;
            Err(ErrorKind::CommandError(command_error).into())
        } else {
            Ok(())
        }
    }

    /// Deserialize the body of the response.
    pub(crate) fn body<T: DeserializeOwned>(&self) -> Result<T> {
        match bson::from_bson(Bson::Document(self.raw_response.clone())) {
            Ok(body) => Ok(body),
            Err(e) => Err(ErrorKind::ResponseError {
                message: format!("{}", e),
            }
            .into()),
        }
    }

    /// Gets the cluster time from the response, if any.
    pub(crate) fn cluster_time(&self) -> Option<&ClusterTime> {
        self.cluster_time.as_ref()
    }

    /// The address of the server that sent this response.
    pub(crate) fn source_address(&self) -> &StreamAddress {
        &self.source
    }
}
