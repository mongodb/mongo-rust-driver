use std::time::Duration;

use bson::{RawDocument, RawDocumentBuf};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::wire::Message;
use crate::{
    bson::Document,
    client::{options::ServerApi, ClusterTime, HELLO_COMMAND_NAMES, REDACTED_COMMANDS},
    error::{Error, ErrorKind, Result},
    is_master::{IsMasterCommandResponse, IsMasterReply},
    operation::{CommandErrorBody, CommandResponse},
    options::{ReadConcern, ReadConcernInternal, ReadConcernLevel, ServerAddress},
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

impl RawCommand {
    pub(crate) fn should_compress(&self) -> bool {
        let name = self.name.to_lowercase();
        !REDACTED_COMMANDS.contains(name.as_str()) && !HELLO_COMMAND_NAMES.contains(name.as_str())
    }
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

    read_concern: Option<ReadConcernInternal>,

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

    pub(crate) fn new_read(
        name: String,
        target_db: String,
        read_concern: Option<ReadConcern>,
        body: T,
    ) -> Self {
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
            read_concern: read_concern.map(Into::into),
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

    pub(crate) fn set_read_preference(&mut self, read_preference: ReadPreference) {
        self.read_preference = Some(read_preference);
    }

    pub(crate) fn set_start_transaction(&mut self) {
        self.start_transaction = Some(true);
    }

    pub(crate) fn set_autocommit(&mut self) {
        self.autocommit = Some(false);
    }

    /// Sets the read concern level for this command.
    /// This does not overwrite any other read concern options.
    pub(crate) fn set_read_concern_level(&mut self, level: ReadConcernLevel) {
        let inner = self.read_concern.get_or_insert(ReadConcernInternal {
            level: None,
            at_cluster_time: None,
            after_cluster_time: None,
        });
        inner.level = Some(level);
    }

    /// Sets the read concern level for this command to "snapshot" and sets the `atClusterTime`
    /// field.
    pub(crate) fn set_snapshot_read_concern(&mut self, session: &ClientSession) {
        let inner = self.read_concern.get_or_insert(ReadConcernInternal {
            level: Some(ReadConcernLevel::Snapshot),
            at_cluster_time: None,
            after_cluster_time: None,
        });
        inner.at_cluster_time = session.snapshot_time;
    }

    pub(crate) fn set_after_cluster_time(&mut self, session: &ClientSession) {
        if let Some(operation_time) = session.operation_time {
            let inner = self.read_concern.get_or_insert(ReadConcernInternal {
                level: None,
                at_cluster_time: None,
                after_cluster_time: None,
            });
            inner.after_cluster_time = Some(operation_time);
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RawCommandResponse {
    pub(crate) source: ServerAddress,
    raw: RawDocumentBuf,
}

impl RawCommandResponse {
    #[cfg(test)]
    pub(crate) fn with_document_and_address(source: ServerAddress, doc: Document) -> Result<Self> {
        let mut raw = Vec::new();
        doc.to_writer(&mut raw)?;
        Ok(Self {
            source,
            raw: RawDocumentBuf::from_bytes(raw)?,
        })
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
        Ok(Self {
            source,
            raw: RawDocumentBuf::from_bytes(raw)?,
        })
    }

    pub(crate) fn body<'a, T: Deserialize<'a>>(&'a self) -> Result<T> {
        bson::from_slice(self.raw.as_bytes()).map_err(|e| {
            Error::from(ErrorKind::InvalidResponse {
                message: format!("{}", e),
            })
        })
    }

    /// Used to handle decoding responses where the server may return invalid UTF-8 in error
    /// messages.
    pub(crate) fn body_utf8_lossy<'a, T: Deserialize<'a>>(&'a self) -> Result<T> {
        bson::from_slice_utf8_lossy(self.raw.as_bytes()).map_err(|e| {
            Error::from(ErrorKind::InvalidResponse {
                message: format!("{}", e),
            })
        })
    }

    pub(crate) fn raw_body(&self) -> &RawDocument {
        &self.raw
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.raw.as_bytes()
    }

    /// Deserialize the body of this response, returning an authentication error if it fails.
    pub(crate) fn auth_response_body<T: DeserializeOwned>(
        &self,
        mechanism_name: &str,
    ) -> Result<T> {
        self.body()
            .map_err(|_| Error::invalid_authentication_response(mechanism_name))
    }

    pub(crate) fn to_is_master_response(&self, round_trip_time: Duration) -> Result<IsMasterReply> {
        match self.body::<CommandResponse<IsMasterCommandResponse>>() {
            Ok(response) if response.is_success() => {
                let server_address = self.source_address().clone();
                let cluster_time = response.cluster_time().cloned();
                Ok(IsMasterReply {
                    server_address,
                    command_response: response.body,
                    round_trip_time,
                    cluster_time,
                })
            }
            _ => match self.body::<CommandResponse<CommandErrorBody>>() {
                Ok(command_error_body) => Err(Error::new(
                    ErrorKind::Command(command_error_body.body.command_error),
                    command_error_body.body.error_labels,
                )),
                Err(_) => Err(ErrorKind::InvalidResponse {
                    message: "invalid server response".into(),
                }
                .into()),
            },
        }
    }

    /// The address of the server that sent this response.
    pub(crate) fn source_address(&self) -> &ServerAddress {
        &self.source
    }

    pub(crate) fn into_raw_document_buf(self) -> RawDocumentBuf {
        self.raw
    }
}
