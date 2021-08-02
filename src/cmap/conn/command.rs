use serde::{de::DeserializeOwned, Serialize};

use super::wire::Message;
use crate::{
    bson::Document,
    client::{options::ServerApi, ClusterTime},
    error::{Error, ErrorKind, Result},
    operation::{CommandErrorBody, CommandResponse},
    options::{ReadConcern, ServerAddress},
    selection_criteria::ReadPreference,
    ClientSession,
};

/// A command that has been serialized to BSON.
#[derive(Debug)]
pub(crate) struct RawCommand {
    pub(crate) name: String,
    pub(crate) target_db: String,
    pub(crate) bytes: Vec<u8>,
}

/// Driver-side model of a database command.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Command<T = Document> {
    #[serde(skip)]
    pub(crate) name: String,

    #[serde(flatten)]
    pub(crate) body: T,

    #[serde(rename = "$db")]
    pub(crate) target_db: String,

    lsid: Option<Document>,

    #[serde(rename = "$clusterTime")]
    cluster_time: Option<ClusterTime>,

    #[serde(flatten)]
    server_api: Option<ServerApi>,

    #[serde(rename = "$readPreference")]
    read_preference: Option<ReadPreference>,

    txn_number: Option<i64>,

    start_transaction: Option<bool>,

    autocommit: Option<bool>,

    read_concern: Option<ReadConcern>,

    recovery_token: Option<Document>,
}

impl<T> Command<T> {
    pub(crate) fn new(name: String, target_db: String, body: T) -> Self {
        Self {
            name,
            target_db,
            body,
            lsid: None,
            cluster_time: None,
            server_api: None,
            read_preference: None,
            txn_number: None,
            start_transaction: None,
            autocommit: None,
            read_concern: None,
            recovery_token: None,
        }
    }

    pub(crate) fn set_session(&mut self, session: &ClientSession) {
        self.lsid = Some(session.id().clone())
    }

    pub(crate) fn set_cluster_time(&mut self, cluster_time: &ClusterTime) {
        self.cluster_time = Some(cluster_time.clone());
    }

    pub(crate) fn set_recovery_token(&mut self, recovery_token: &Document) {
        self.recovery_token = Some(recovery_token.clone());
    }

    pub(crate) fn set_txn_number(&mut self, txn_number: i64) {
        self.txn_number = Some(txn_number);
    }

    pub(crate) fn set_server_api(&mut self, server_api: &ServerApi) {
        self.server_api = Some(server_api.clone());
    }

    pub(crate) fn set_read_preference(&mut self, read_preference: ReadPreference) -> Result<()> {
        self.read_preference = Some(read_preference);
        Ok(())
    }

    pub(crate) fn set_start_transaction(&mut self) {
        self.start_transaction = Some(true);
    }

    pub(crate) fn set_autocommit(&mut self) {
        self.autocommit = Some(false);
    }

    pub(crate) fn set_txn_read_concern(&mut self, session: &ClientSession) -> Result<()> {
        if let Some(ref options) = session.transaction.options {
            if let Some(ref read_concern) = options.read_concern {
                self.read_concern = Some(read_concern.clone());
            }
        }
        Ok(())
    }

    pub(crate) fn set_snapshot_read_concern(&mut self, session: &ClientSession) -> Result<()> {
        let mut concern = ReadConcern::snapshot();
        concern.at_cluster_time = session.snapshot_time;
        self.read_concern = Some(concern);
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
