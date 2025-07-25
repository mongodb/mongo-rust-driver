mod abort_transaction;
pub(crate) mod aggregate;
pub(crate) mod bulk_write;
mod commit_transaction;
pub(crate) mod count;
pub(crate) mod count_documents;
pub(crate) mod create;
mod create_indexes;
mod delete;
mod distinct;
pub(crate) mod drop_collection;
pub(crate) mod drop_database;
mod drop_indexes;
mod find;
pub(crate) mod find_and_modify;
mod get_more;
mod insert;
pub(crate) mod list_collections;
pub(crate) mod list_databases;
mod list_indexes;
#[cfg(feature = "in-use-encryption")]
mod raw_output;
pub(crate) mod run_command;
pub(crate) mod run_cursor_command;
mod search_index;
mod update;

use std::{collections::VecDeque, fmt::Debug, ops::Deref};

use bson::{RawBsonRef, RawDocument, RawDocumentBuf, Timestamp};
use futures_util::FutureExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    bson::{self, Bson, Document},
    bson_compat::CStr,
    bson_util::{self, extend_raw_document_buf},
    client::{ClusterTime, HELLO_COMMAND_NAMES, REDACTED_COMMANDS},
    cmap::{
        conn::{pooled::PooledConnection, PinnedConnectionHandle},
        Command,
        RawCommandResponse,
        StreamDescription,
    },
    error::{
        CommandError,
        Error,
        ErrorKind,
        IndexedWriteError,
        InsertManyError,
        Result,
        WriteConcernError,
        WriteFailure,
    },
    options::{ClientOptions, WriteConcern},
    selection_criteria::SelectionCriteria,
    BoxFuture,
    ClientSession,
    Namespace,
};

pub(crate) use abort_transaction::AbortTransaction;
pub(crate) use commit_transaction::CommitTransaction;
pub(crate) use create_indexes::CreateIndexes;
pub(crate) use delete::Delete;
pub(crate) use distinct::Distinct;
pub(crate) use drop_indexes::DropIndexes;
pub(crate) use find::Find;
pub(crate) use find_and_modify::FindAndModify;
pub(crate) use get_more::GetMore;
pub(crate) use insert::Insert;
pub(crate) use list_indexes::ListIndexes;
#[cfg(feature = "in-use-encryption")]
pub(crate) use raw_output::RawOutput;
pub(crate) use search_index::{CreateSearchIndexes, DropSearchIndex, UpdateSearchIndex};
pub(crate) use update::{Update, UpdateOrReplace};

const SERVER_4_2_0_WIRE_VERSION: i32 = 8;
const SERVER_4_4_0_WIRE_VERSION: i32 = 9;
const SERVER_5_0_0_WIRE_VERSION: i32 = 13;
const SERVER_8_0_0_WIRE_VERSION: i32 = 25;
// The maximum number of bytes that may be included in a write payload when auto-encryption is
// enabled.
const MAX_ENCRYPTED_WRITE_SIZE: usize = 2_097_152;
// The amount of message overhead (OP_MSG bytes and command-agnostic fields) to account for when
// building a multi-write operation using document sequences.
const OP_MSG_OVERHEAD_BYTES: usize = 1_000;

/// Context about the execution of the operation.
pub(crate) struct ExecutionContext<'a> {
    pub(crate) connection: &'a mut PooledConnection,
    pub(crate) session: Option<&'a mut ClientSession>,
    pub(crate) effective_criteria: SelectionCriteria,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) enum Retryability {
    Write,
    Read,
    None,
}

impl Retryability {
    /// Returns this level of retryability in tandem with the client options.
    pub(crate) fn with_options(&self, options: &ClientOptions) -> Self {
        match self {
            Self::Write if options.retry_writes != Some(false) => Self::Write,
            Self::Read if options.retry_reads != Some(false) => Self::Read,
            _ => Self::None,
        }
    }

    /// Whether this level of retryability can retry the given error.
    pub(crate) fn can_retry_error(&self, error: &Error) -> bool {
        match self {
            Self::Write => error.is_write_retryable(),
            Self::Read => error.is_read_retryable(),
            Self::None => false,
        }
    }
}

/// A trait modeling the behavior of a server side operation.
///
/// No methods in this trait should have default behaviors to ensure that wrapper operations
/// replicate all behavior.  Default behavior is provided by the `OperationDefault` trait.
pub(crate) trait Operation {
    /// The output type of this operation.
    type O;

    /// The name of the server side command associated with this operation.
    const NAME: &'static CStr;

    /// Returns the command that should be sent to the server as part of this operation.
    /// The operation may store some additional state that is required for handling the response.
    fn build(&mut self, description: &StreamDescription) -> Result<Command>;

    /// Parse the response for the atClusterTime field.
    /// Depending on the operation, this may be found in different locations.
    fn extract_at_cluster_time(&self, _response: &RawDocument) -> Result<Option<Timestamp>>;

    /// Interprets the server response to the command.
    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        context: ExecutionContext<'a>,
    ) -> BoxFuture<'a, Result<Self::O>>;

    /// Interpret an error encountered while sending the built command to the server, potentially
    /// recovering.
    fn handle_error(&self, error: Error) -> Result<Self::O>;

    /// Criteria to use for selecting the server that this operation will be executed on.
    fn selection_criteria(&self) -> Option<&SelectionCriteria>;

    /// Whether or not this operation will request acknowledgment from the server.
    fn is_acknowledged(&self) -> bool;

    /// The write concern to use for this operation, if any.
    fn write_concern(&self) -> Option<&WriteConcern>;

    /// Returns whether or not this command supports the `readConcern` field.
    fn supports_read_concern(&self, description: &StreamDescription) -> bool;

    /// Whether this operation supports sessions or not.
    fn supports_sessions(&self) -> bool;

    /// The level of retryability the operation supports.
    fn retryability(&self) -> Retryability;

    /// Updates this operation as needed for a retry.
    fn update_for_retry(&mut self);

    /// Returns a function handle to potentially override selection criteria based on server
    /// topology.
    fn override_criteria(&self) -> OverrideCriteriaFn;

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle>;

    /// The name of the server side command associated with this operation.
    fn name(&self) -> &CStr;
}

pub(crate) type OverrideCriteriaFn =
    fn(&SelectionCriteria, &crate::sdam::TopologyDescription) -> Option<SelectionCriteria>;

// A mirror of the `Operation` trait, with default behavior where appropriate.  Should only be
// implemented by operation types that do not delegate to other operations.
pub(crate) trait OperationWithDefaults: Send + Sync {
    /// The output type of this operation.
    type O;

    /// The name of the server side command associated with this operation.
    const NAME: &'static CStr;

    /// Returns the command that should be sent to the server as part of this operation.
    /// The operation may store some additional state that is required for handling the response.
    fn build(&mut self, description: &StreamDescription) -> Result<Command>;

    /// Parse the response for the atClusterTime field.
    /// Depending on the operation, this may be found in different locations.
    fn extract_at_cluster_time(&self, _response: &RawDocument) -> Result<Option<Timestamp>> {
        Ok(None)
    }

    /// Interprets the server response to the command.
    fn handle_response<'a>(
        &'a self,
        _response: RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        Err(ErrorKind::Internal {
            message: format!("operation handling not implemented for {}", Self::NAME),
        }
        .into())
    }

    /// Interprets the server response to the command. This method should only be implemented when
    /// async code is required to handle the response.
    fn handle_response_async<'a>(
        &'a self,
        response: RawCommandResponse,
        context: ExecutionContext<'a>,
    ) -> BoxFuture<'a, Result<Self::O>> {
        async move { self.handle_response(response, context) }.boxed()
    }

    /// Interpret an error encountered while sending the built command to the server, potentially
    /// recovering.
    fn handle_error(&self, error: Error) -> Result<Self::O> {
        Err(error)
    }

    /// Criteria to use for selecting the server that this operation will be executed on.
    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        None
    }

    /// Whether or not this operation will request acknowledgment from the server.
    fn is_acknowledged(&self) -> bool {
        self.write_concern()
            .map(WriteConcern::is_acknowledged)
            .unwrap_or(true)
    }

    /// The write concern to use for this operation, if any.
    fn write_concern(&self) -> Option<&WriteConcern> {
        None
    }

    /// Returns whether or not this command supports the `readConcern` field.
    fn supports_read_concern(&self, _description: &StreamDescription) -> bool {
        false
    }

    /// Whether this operation supports sessions or not.
    fn supports_sessions(&self) -> bool {
        true
    }

    /// The level of retryability the operation supports.
    fn retryability(&self) -> Retryability {
        Retryability::None
    }

    /// Updates this operation as needed for a retry.
    fn update_for_retry(&mut self) {}

    /// Returns a function handle to potentially override selection criteria based on server
    /// topology.
    fn override_criteria(&self) -> OverrideCriteriaFn {
        |_, _| None
    }

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        None
    }

    /// The name of the server side command associated with this operation.
    fn name(&self) -> &CStr {
        Self::NAME
    }
}

impl<T: OperationWithDefaults> Operation for T
where
    T: Send + Sync,
{
    type O = T::O;
    const NAME: &'static CStr = T::NAME;
    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        self.build(description)
    }
    fn extract_at_cluster_time(&self, response: &RawDocument) -> Result<Option<Timestamp>> {
        self.extract_at_cluster_time(response)
    }
    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        context: ExecutionContext<'a>,
    ) -> BoxFuture<'a, Result<Self::O>> {
        self.handle_response_async(response, context)
    }
    fn handle_error(&self, error: Error) -> Result<Self::O> {
        self.handle_error(error)
    }
    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.selection_criteria()
    }
    fn is_acknowledged(&self) -> bool {
        self.is_acknowledged()
    }
    fn write_concern(&self) -> Option<&WriteConcern> {
        self.write_concern()
    }
    fn supports_read_concern(&self, description: &StreamDescription) -> bool {
        self.supports_read_concern(description)
    }
    fn supports_sessions(&self) -> bool {
        self.supports_sessions()
    }
    fn retryability(&self) -> Retryability {
        self.retryability()
    }
    fn update_for_retry(&mut self) {
        self.update_for_retry()
    }
    fn override_criteria(&self) -> OverrideCriteriaFn {
        self.override_criteria()
    }
    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        self.pinned_connection()
    }
    fn name(&self) -> &CStr {
        self.name()
    }
}

fn should_redact_body(body: &RawDocumentBuf) -> bool {
    if let Some(Ok((command_name, _))) = body.into_iter().next() {
        HELLO_COMMAND_NAMES.contains(command_name.to_lowercase().as_str())
            && body.get("speculativeAuthenticate").ok().flatten().is_some()
    } else {
        false
    }
}

impl Command {
    pub(crate) fn should_redact(&self) -> bool {
        let name = self.name.to_lowercase();
        REDACTED_COMMANDS.contains(name.as_str()) || should_redact_body(&self.body)
    }

    #[cfg(any(
        feature = "zstd-compression",
        feature = "zlib-compression",
        feature = "snappy-compression"
    ))]
    pub(crate) fn should_compress(&self) -> bool {
        let name = self.name.to_lowercase();
        !REDACTED_COMMANDS.contains(name.as_str()) && !HELLO_COMMAND_NAMES.contains(name.as_str())
    }
}

/// A response to a command with a body shaped deserialized to a `T`.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CommandResponse<T> {
    pub(crate) ok: Bson,

    #[serde(rename = "$clusterTime")]
    pub(crate) cluster_time: Option<ClusterTime>,

    #[serde(flatten)]
    pub(crate) body: T,
}

impl<T: DeserializeOwned> CommandResponse<T> {
    /// Whether the command succeeeded or not (i.e. if this response is ok: 1).
    pub(crate) fn is_success(&self) -> bool {
        bson_util::get_int(&self.ok) == Some(1)
    }

    pub(crate) fn cluster_time(&self) -> Option<&ClusterTime> {
        self.cluster_time.as_ref()
    }
}

/// A response body useful for deserializing command errors.
#[derive(Deserialize, Debug)]
pub(crate) struct CommandErrorBody {
    #[serde(rename = "errorLabels")]
    pub(crate) error_labels: Option<Vec<String>>,

    #[serde(flatten)]
    pub(crate) command_error: CommandError,
}

impl From<CommandErrorBody> for Error {
    fn from(command_error_response: CommandErrorBody) -> Error {
        Error::new(
            ErrorKind::Command(command_error_response.command_error),
            command_error_response.error_labels,
        )
    }
}

/// Appends a serializable struct to the input document. The serializable struct MUST serialize to a
/// Document; otherwise, an error will be thrown.
pub(crate) fn append_options<T: Serialize + Debug>(
    doc: &mut Document,
    options: Option<&T>,
) -> Result<()> {
    if let Some(options) = options {
        let options_doc = crate::bson_compat::serialize_to_document(options)?;
        doc.extend(options_doc);
    }
    Ok(())
}

pub(crate) fn append_options_to_raw_document<T: Serialize>(
    doc: &mut RawDocumentBuf,
    options: Option<&T>,
) -> Result<()> {
    if let Some(options) = options {
        let options_raw_doc = crate::bson_compat::serialize_to_raw_document_buf(options)?;
        extend_raw_document_buf(doc, options_raw_doc)?;
    }
    Ok(())
}

#[derive(Deserialize, Debug)]
pub(crate) struct SingleWriteBody {
    n: u64,
}

/// Body of a write response that could possibly have a write concern error but not write errors.
#[derive(Debug, Deserialize, Default, Clone)]
pub(crate) struct WriteConcernOnlyBody {
    #[serde(rename = "writeConcernError")]
    write_concern_error: Option<WriteConcernError>,

    #[serde(rename = "errorLabels")]
    labels: Option<Vec<String>>,
}

impl WriteConcernOnlyBody {
    fn validate(&self) -> Result<()> {
        match self.write_concern_error {
            Some(ref wc_error) => Err(Error::new(
                ErrorKind::Write(WriteFailure::WriteConcernError(wc_error.clone())),
                self.labels.clone(),
            )),
            None => Ok(()),
        }
    }
}

#[derive(Debug)]
pub(crate) struct WriteResponseBody<T = SingleWriteBody> {
    body: T,
    write_errors: Option<Vec<IndexedWriteError>>,
    write_concern_error: Option<WriteConcernError>,
    labels: Option<Vec<String>>,
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for WriteResponseBody<T> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use bson::serde_helpers::Utf8LossyDeserialization;
        #[derive(Deserialize)]
        struct Helper<T> {
            #[serde(flatten)]
            body: T,
            #[serde(rename = "writeErrors")]
            write_errors: Option<Utf8LossyDeserialization<Vec<IndexedWriteError>>>,
            #[serde(rename = "writeConcernError")]
            write_concern_error: Option<Utf8LossyDeserialization<WriteConcernError>>,
            #[serde(rename = "errorLabels")]
            labels: Option<Vec<String>>,
        }
        let helper = Helper::deserialize(deserializer)?;
        Ok(Self {
            body: helper.body,
            write_errors: helper.write_errors.map(|l| l.0),
            write_concern_error: helper.write_concern_error.map(|l| l.0),
            labels: helper.labels,
        })
    }
}

impl<T> WriteResponseBody<T> {
    fn validate(&self) -> Result<()> {
        if self.write_errors.is_none() && self.write_concern_error.is_none() {
            return Ok(());
        };

        let failure = InsertManyError {
            write_errors: self.write_errors.clone(),
            write_concern_error: self.write_concern_error.clone(),
            inserted_ids: Default::default(),
        };

        Err(Error::new(
            ErrorKind::InsertMany(failure),
            self.labels.clone(),
        ))
    }
}

impl<T> Deref for WriteResponseBody<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct CursorBody {
    cursor: CursorInfo,
}

impl CursorBody {
    fn extract_at_cluster_time(response: &RawDocument) -> Result<Option<Timestamp>> {
        Ok(response
            .get("cursor")?
            .and_then(RawBsonRef::as_document)
            .map(|d| d.get("atClusterTime"))
            .transpose()?
            .flatten()
            .and_then(RawBsonRef::as_timestamp))
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CursorInfo {
    pub(crate) id: i64,

    pub(crate) ns: Namespace,

    pub(crate) first_batch: VecDeque<RawDocumentBuf>,

    pub(crate) post_batch_resume_token: Option<RawDocumentBuf>,
}

/// Type used to deserialize just the first result from a cursor, if any.
#[derive(Debug, Clone)]
pub(crate) struct SingleCursorResult<T>(Option<T>);

impl<'de, T> Deserialize<'de> for SingleCursorResult<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct FullCursorBody<T> {
            cursor: InteriorBody<T>,
        }

        #[derive(Deserialize)]
        struct InteriorBody<T> {
            #[serde(rename = "firstBatch")]
            first_batch: Vec<T>,
        }

        let mut full_body = FullCursorBody::deserialize(deserializer)?;
        Ok(SingleCursorResult(full_body.cursor.first_batch.pop()))
    }
}
