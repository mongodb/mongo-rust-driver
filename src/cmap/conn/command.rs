pub(crate) use serde::de::DeserializeOwned;

use super::wire::Message;
use crate::{
    bson::Document,
    client::{options::ServerApi, ClusterTime},
    error::{Error, ErrorKind, Result},
    operation::{CommandErrorBody, CommandResponse},
    options::ServerAddress,
    selection_criteria::ReadPreference,
    ClientSession,
};

/// `Command` is a driver side abstraction of a server command containing all the information
/// necessary to serialize it to a wire message.
#[derive(Debug, Clone)]
pub(crate) struct Command {
    pub(crate) name: String,
    pub(crate) target_db: String,
    pub(crate) body: Document,
}

impl Command {
    /// Constructs a new command.
    pub(crate) fn new(name: String, target_db: String, body: Document) -> Self {
        Self {
            name,
            target_db,
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
        if matches!(self.name.as_str(), "getMore") {
            return;
        }

        self.body
            .insert("apiVersion", format!("{}", server_api.version));

        if let Some(strict) = server_api.strict {
            self.body.insert("apiStrict", strict);
        }

        if let Some(deprecation_errors) = server_api.deprecation_errors {
            self.body.insert("apiDeprecationErrors", deprecation_errors);
        }
    }

    pub(crate) fn set_read_preference(&mut self, read_preference: ReadPreference) {
        self.body
            .insert("$readPreference", read_preference.into_document());
    }

    pub(crate) fn set_start_transaction(&mut self) {
        self.body.insert("startTransaction", true);
    }

    pub(crate) fn set_autocommit(&mut self) {
        self.body.insert("autocommit", false);
    }

    pub(crate) fn set_txn_read_concern(&mut self, session: &ClientSession) -> Result<()> {
        if let Some(ref options) = session.transaction.options {
            if let Some(ref read_concern) = options.read_concern {
                self.body
                    .insert("readConcern", bson::to_document(read_concern)?);
            }
        }
        Ok(())
    }

    pub(crate) fn set_snapshot_read_concern(&mut self, session: &ClientSession) -> Result<()> {
        let mut concern = ReadConcern::snapshot();
        concern.at_cluster_time = session.snapshot_time;
        self.body
            .insert("readConcern", bson::to_document(&concern)?);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RawCommandResponse {
    pub(crate) source: ServerAddress,
    raw: Vec<u8>,
}

impl RawCommandResponse {
    #[cfg(test)]
    pub(crate) fn with_document_and_address(source: ServerAddress, doc: Document) -> Result<Self> {
        let mut raw = Vec::new();
        doc.to_writer(&mut raw)?;
        Ok(Self { source, raw })
    }

    /// Initialize a response from a document.
    #[cfg(test)]
    pub(crate) fn with_document(doc: Document) -> Result<Self> {
        Self::with_document_and_address(
            ServerAddress::Tcp {
                host: "localhost".to_string(),
                port: None,
            },
            doc,
        )
    }

    pub(crate) fn new(source: ServerAddress, message: Message) -> Result<Self> {
        let raw = message.single_document_response()?;
        Ok(Self { source, raw })
    }

    pub(crate) fn body<T: DeserializeOwned>(&self) -> Result<T> {
        bson::from_slice(self.raw.as_slice()).map_err(|e| {
            Error::from(ErrorKind::InvalidResponse {
                message: format!("{}", e),
            })
        })
    }

    /// Deserialize the body of this response, returning an authentication error if it fails.
    pub(crate) fn auth_response_body<T: DeserializeOwned>(
        &self,
        mechanism_name: &str,
    ) -> Result<T> {
        self.body()
            .map_err(|_| Error::invalid_authentication_response(mechanism_name))
    }

    /// Deserialize the raw bytes into a response backed by a `Document` for further processing.
    pub(crate) fn into_document_response(self) -> Result<DocumentCommandResponse> {
        let response: CommandResponse<Document> = self.body()?;
        Ok(DocumentCommandResponse { response })
    }

    /// The address of the server that sent this response.
    pub(crate) fn source_address(&self) -> &ServerAddress {
        &self.source
    }
}

/// A command response backed by a `Document` rather than raw bytes.
/// Use this for simple command responses where deserialization performance is not a high priority.
pub(crate) struct DocumentCommandResponse {
    response: CommandResponse<Document>,
}

impl DocumentCommandResponse {
    /// Returns a result indicating whether this response corresponds to a command failure.
    pub(crate) fn validate(&self) -> Result<()> {
        if !self.response.is_success() {
            let error_response: CommandErrorBody = bson::from_document(self.response.body.clone())
                .map_err(|_| ErrorKind::InvalidResponse {
                    message: "invalid server response".to_string(),
                })?;
            Err(Error::new(
                ErrorKind::Command(error_response.command_error),
                error_response.error_labels,
            ))
        } else {
            Ok(())
        }
    }

    /// Deserialize the body of the response.
    pub(crate) fn body<T: DeserializeOwned>(self) -> Result<T> {
        match bson::from_document(self.response.body) {
            Ok(body) => Ok(body),
            Err(e) => Err(ErrorKind::InvalidResponse {
                message: format!("{}", e),
            }
            .into()),
        }
    }

    pub(crate) fn cluster_time(&self) -> Option<&ClusterTime> {
        self.response.cluster_time.as_ref()
    }
}
