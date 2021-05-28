use serde::{de::DeserializeOwned, Deserialize};

use super::wire::Message;
use crate::{
    bson::{Bson, Document},
    bson_util,
    client::{options::ServerApi, ClusterTime},
    error::{CommandError, Error, ErrorKind, Result},
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
}

#[derive(Debug, Clone)]
pub(crate) struct CommandResponse {
    source: ServerAddress,
    pub(crate) raw_response: Document,
    cluster_time: Option<ClusterTime>,
}

impl CommandResponse {
    #[cfg(test)]
    pub(crate) fn with_document_and_address(source: ServerAddress, doc: Document) -> Self {
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
            ServerAddress::Tcp {
                host: "localhost".to_string(),
                port: None,
            },
            doc,
        )
    }

    pub(crate) fn new(source: ServerAddress, message: Message) -> Result<Self> {
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

    /// Returns a result indicating whether this response corresponds to a command failure.
    pub(crate) fn validate(&self) -> Result<()> {
        if !self.is_success() {
            let error_response: CommandErrorResponse =
                bson::from_bson(Bson::Document(self.raw_response.clone())).map_err(|_| {
                    ErrorKind::InvalidResponse {
                        message: "invalid server response".to_string(),
                    }
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
    pub(crate) fn body<T: DeserializeOwned>(&self) -> Result<T> {
        match bson::from_bson(Bson::Document(self.raw_response.clone())) {
            Ok(body) => Ok(body),
            Err(e) => Err(ErrorKind::InvalidResponse {
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
    pub(crate) fn source_address(&self) -> &ServerAddress {
        &self.source
    }
}

#[derive(Deserialize, Debug)]
struct CommandErrorResponse {
    #[serde(rename = "errorLabels")]
    error_labels: Option<Vec<String>>,

    #[serde(flatten)]
    command_error: CommandError,
}
