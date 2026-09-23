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
pub(crate) mod raw_output;
pub(crate) mod run_command;
pub(crate) mod run_cursor_command;
mod search_index;
mod update;

use std::fmt::Debug;

use bson::{RawBsonRef, RawDocument, RawDocumentBuf, Timestamp};
use futures_util::FutureExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    bson::{self, Bson, Document},
    bson_compat::CStr,
    bson_util::{self, extend_raw_document_buf},
    client::{ClusterTime, Retry, HELLO_COMMAND_NAMES, REDACTED_COMMANDS},
    cmap::{
        conn::{pooled::PooledConnection, PinnedConnectionHandle},
        Command,
        RawCommandResponse,
        StreamDescription,
    },
    error::{CommandError, Error, ErrorKind, Result},
    options::{ClientOptions, ReadConcern, WriteConcern},
    selection_criteria::SelectionCriteria,
    BoxFuture,
    ClientSession,
    Collection,
    Database,
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
pub(crate) use search_index::{CreateSearchIndexes, DropSearchIndex, UpdateSearchIndex};
pub(crate) use update::{Update, UpdateOrReplace};

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
    pub(crate) fn write(options: &ClientOptions) -> Self {
        if options.retry_writes != Some(false) {
            Self::Write
        } else {
            Self::None
        }
    }

    pub(crate) fn read(options: &ClientOptions) -> Self {
        if options.retry_reads != Some(false) {
            Self::Read
        } else {
            Self::None
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

pub(crate) struct OperationDetails {
    pub(crate) response_handling_kind: ResponseHandlingKind,
    pub(crate) selection_criteria: Feature<SelectionCriteria>,
    pub(crate) read_concern: Feature<ReadConcern>,
    pub(crate) write_concern: Feature<WriteConcern>,
    pub(crate) supports_sessions: bool,
    pub(crate) retryability: Retryability,
    pub(crate) is_backpressure_retryable: bool,
    pub(crate) override_criteria: Option<OverrideCriteriaFn>,
    pub(crate) target: OperationTarget,
    pub(crate) is_after_cluster_time_write: bool,
}

pub(crate) enum ResponseHandlingKind {
    /// [Operation::handle_response] should be implemented when this variant is set.
    Borrowed,
    /// [Operation::handle_response_owned] should be implemented when this variant is set.
    Owned,
    /// [Operation::handle_response_async] should be implemented when this variant is set.
    Async,
}

pub(crate) trait Operation: Send + Sync {
    /// The output type of this operation.
    type O;

    /// The name of the server side command associated with this operation.
    const NAME: &'static CStr;

    /// The details associated with the execution of this operation.
    fn details(&self, options: &ClientOptions) -> OperationDetails;

    fn name(&self) -> &CStr;

    /// Returns the command that should be sent to the server as part of this operation.
    /// The operation may store some additional state that is required for handling the response.
    fn build(
        &mut self,
        description: &StreamDescription,
        op_details: &OperationDetails,
    ) -> Result<Command>;

    /// Parse the response for the atClusterTime field.
    /// Depending on the operation, this may be found in different locations.
    fn extract_at_cluster_time(&self, response: &RawDocument) -> Result<Option<Timestamp>>;

    /// Interprets the server response to the command.
    fn handle_response<'a>(
        &'a self,
        _response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        Err(Error::internal(format!(
            "response handling not implemented for {}",
            Self::NAME
        )))
    }

    /// Interprets the server response to the command, taking ownership of the body to enable
    /// zero-copy handling.
    fn handle_response_owned<'a>(
        &'a self,
        _response: RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        Err(Error::internal(format!(
            "response handling not implemented for {}",
            Self::NAME
        )))
    }

    /// Interprets the server response to the command. This method should only be implemented when
    /// async code is required to handle the response.
    fn handle_response_async<'a>(
        &'a self,
        _response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> BoxFuture<'a, Result<Self::O>> {
        async move {
            Err(Error::internal(format!(
                "response handling not implemented for {}",
                Self::NAME
            )))
        }
        .boxed()
    }

    /// Interpret an error encountered while sending the built command to the server, potentially
    /// recovering.
    fn handle_error(&self, error: Error) -> Result<Self::O>;

    /// Updates this operation as needed for a retry.
    fn update_for_retry(&mut self, retry: Option<&Retry>);

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle>;

    #[cfg(feature = "opentelemetry")]
    type Otel: crate::otel::OtelWitness<Op = Self>;

    #[cfg(feature = "opentelemetry")]
    fn otel(&self) -> &impl crate::otel::OtelInfo {
        <Self::Otel as crate::otel::OtelWitness>::otel(self)
    }
}

pub(crate) fn op_is_commit<T: Operation>(op: &T) -> bool {
    op.name() == CommitTransaction::NAME
}

pub(crate) fn op_is_abort<T: Operation>(op: &T) -> bool {
    op.name() == AbortTransaction::NAME
}

/// Expands into the default behavior for each specified [Operation] method.
macro_rules! default_impl {
    () => {};
    (name $(, $rest:ident)*) => {
        fn name(&self) -> &crate::bson_compat::CStr {
            Self::NAME
        }
        $crate::operation::default_impl!($($rest),*);
    };
    (extract_at_cluster_time $(, $rest:ident)*) => {
        fn extract_at_cluster_time(
            &self,
            _response: &crate::bson::RawDocument,
        ) -> crate::error::Result<Option<crate::bson::Timestamp>> {
            Ok(None)
        }
        $crate::operation::default_impl!($($rest),*);
    };
    (handle_error $(, $rest:ident)*) => {
        fn handle_error(&self, error: crate::error::Error) -> crate::error::Result<Self::O> {
            Err(error)
        }
        $crate::operation::default_impl!($($rest),*);
    };
    (update_for_retry $(, $rest:ident)*) => {
        fn update_for_retry(&mut self, _retry: Option<&crate::client::Retry>) {}
        $crate::operation::default_impl!($($rest),*);
    };
    (pinned_connection $(, $rest:ident)*) => {
        fn pinned_connection(&self) -> Option<&crate::cmap::conn::PinnedConnectionHandle> {
            None
        }
        $crate::operation::default_impl!($($rest),*);
    };
}
pub(crate) use default_impl;

/// Expands into [Operation] methods that forward to the wrapped value.
macro_rules! forward_impl {
    ($field:tt) => {};
    ($field:tt, name $(, $rest:ident)*) => {
        fn name(&self) -> &crate::bson_compat::CStr {
            self.$field.name()
        }
        $crate::operation::forward_impl!($field $(, $rest)*);
    };
    ($field:tt, build $(, $rest:ident)*) => {
        fn build(
            &mut self,
            description: &crate::cmap::StreamDescription,
            spec: &crate::operation::OperationDetails,
        ) -> crate::error::Result<crate::cmap::Command> {
            self.$field.build(description, spec)
        }
        $crate::operation::forward_impl!($field $(, $rest)*);
    };
    ($field:tt, extract_at_cluster_time $(, $rest:ident)*) => {
        fn extract_at_cluster_time(
            &self,
            response: &crate::bson::RawDocument,
        ) -> crate::error::Result<Option<crate::bson::Timestamp>> {
            self.$field.extract_at_cluster_time(response)
        }
        $crate::operation::forward_impl!($field $(, $rest)*);
    };
    ($field:tt, update_for_retry $(, $rest:ident)*) => {
        fn update_for_retry(&mut self, retry: Option<&crate::client::Retry>) {
            self.$field.update_for_retry(retry)
        }
        $crate::operation::forward_impl!($field $(, $rest)*);
    };
    ($field:tt, pinned_connection $(, $rest:ident)*) => {
        fn pinned_connection(&self) -> Option<&crate::cmap::conn::PinnedConnectionHandle> {
            self.$field.pinned_connection()
        }
        $crate::operation::forward_impl!($field $(, $rest)*);
    };
}
pub(crate) use forward_impl;

#[derive(Debug, Copy, Clone)]
pub(crate) enum Feature<T> {
    Set(T),
    Inherit,
    NotSupported,
}

impl<T> From<Option<T>> for Feature<T> {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(c) => Self::Set(c),
            None => Self::Inherit,
        }
    }
}

impl<T> Feature<T> {
    pub(crate) fn is_set(&self) -> bool {
        matches!(self, Self::Set(_))
    }

    pub(crate) fn supported(&self) -> bool {
        match self {
            Self::NotSupported => false,
            _ => true,
        }
    }
}

macro_rules! to_feature {
    ($options:expr,$field:ident) => {
        $options
            .as_ref()
            .and_then(|options| options.$field.clone())
            .into()
    };
}
pub(crate) use to_feature;

pub(crate) type OverrideCriteriaFn =
    fn(&SelectionCriteria, &crate::sdam::TopologyDescription) -> Option<SelectionCriteria>;

#[derive(Debug, Clone)]
pub(crate) enum OperationTarget {
    Database(Database),
    Collection(crate::Collection<Document>),
    Namespace(Namespace),
}

impl OperationTarget {
    pub(crate) fn admin(client: &crate::Client) -> Self {
        Self::Database(client.database("admin"))
    }

    pub(crate) fn db_name(&self) -> &str {
        match self {
            Self::Database(db) => db.name(),
            Self::Collection(coll) => coll.db().name(),
            Self::Namespace(ns) => &ns.db,
        }
    }

    pub(crate) fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        match self {
            Self::Database(db) => db.selection_criteria(),
            Self::Collection(coll) => coll.selection_criteria(),
            Self::Namespace(_) => None,
        }
    }

    pub(crate) fn read_concern(&self) -> Option<&ReadConcern> {
        match self {
            Self::Database(db) => db.read_concern(),
            Self::Collection(coll) => coll.read_concern(),
            Self::Namespace(_) => None,
        }
    }

    pub(crate) fn write_concern(&self) -> Option<&WriteConcern> {
        match self {
            Self::Database(db) => db.write_concern(),
            Self::Collection(coll) => coll.write_concern(),
            Self::Namespace(_) => None,
        }
    }
}

impl From<&Database> for OperationTarget {
    fn from(value: &Database) -> Self {
        Self::Database(value.clone())
    }
}

impl<T: Send + Sync> From<&Collection<T>> for OperationTarget {
    fn from(value: &Collection<T>) -> Self {
        Self::Collection(value.clone_with_type())
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

fn cursor_get_at_cluster_time(response: &RawDocument) -> Result<Option<Timestamp>> {
    Ok(response
        .get("cursor")?
        .and_then(RawBsonRef::as_document)
        .map(|d| d.get("atClusterTime"))
        .transpose()?
        .flatten()
        .and_then(RawBsonRef::as_timestamp))
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
