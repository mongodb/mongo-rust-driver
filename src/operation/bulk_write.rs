mod server_responses;

use std::collections::HashMap;

use futures_core::TryStream;
use futures_util::{FutureExt, TryStreamExt};

use crate::{
    bson::{rawdoc, Bson, RawDocumentBuf},
    bson_util::{self, extend_raw_document_buf},
    checked::Checked,
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::{ClientBulkWriteError, Error, ErrorKind, Result},
    operation::OperationWithDefaults,
    options::{BulkWriteOptions, OperationType, WriteModel},
    results::{BulkWriteResult, DeleteResult, InsertOneResult, UpdateResult},
    BoxFuture,
    Client,
    Cursor,
    Namespace,
    SessionCursor,
};

use super::{ExecutionContext, Retryability, WriteResponseBody, OP_MSG_OVERHEAD_BYTES};

use server_responses::*;

pub(crate) struct BulkWrite<'a> {
    client: Client,
    models: &'a [WriteModel],
    offset: usize,
    options: Option<&'a BulkWriteOptions>,
    /// The _ids of the inserted documents. This value is populated in `build`.
    inserted_ids: HashMap<usize, Bson>,
    /// The number of writes that were sent to the server. This value is populated in `build`.
    pub(crate) n_attempted: usize,
}

impl<'a> BulkWrite<'a> {
    pub(crate) async fn new(
        client: Client,
        models: &'a [WriteModel],
        offset: usize,
        options: Option<&'a BulkWriteOptions>,
    ) -> BulkWrite<'a> {
        Self {
            client,
            models,
            offset,
            options,
            n_attempted: 0,
            inserted_ids: HashMap::new(),
        }
    }

    fn is_verbose(&self) -> bool {
        self.options
            .as_ref()
            .and_then(|o| o.verbose_results)
            .unwrap_or(false)
    }

    async fn iterate_results_cursor(
        &self,
        mut stream: impl TryStream<Ok = SingleOperationResponse, Error = Error> + Unpin,
        error: &mut ClientBulkWriteError,
    ) -> Result<()> {
        let result = &mut error.partial_result;

        while let Some(response) = stream.try_next().await? {
            let index = response.index + self.offset;
            match response.result {
                SingleOperationResult::Success {
                    n,
                    n_modified,
                    upserted,
                } => {
                    let model = self.get_model(response.index)?;
                    match model.operation_type() {
                        OperationType::Insert => {
                            let inserted_id = self.get_inserted_id(index)?;
                            let insert_result = InsertOneResult { inserted_id };
                            result
                                .get_or_insert_with(|| BulkWriteResult::new(self.is_verbose()))
                                .add_insert_result(index, insert_result);
                        }
                        OperationType::Update => {
                            let modified_count =
                                n_modified.ok_or_else(|| ErrorKind::InvalidResponse {
                                    message: "nModified value not returned for update bulkWrite \
                                              operation"
                                        .into(),
                                })?;
                            let update_result = UpdateResult {
                                matched_count: n,
                                modified_count,
                                upserted_id: upserted.map(|upserted| upserted.id),
                            };
                            result
                                .get_or_insert_with(|| BulkWriteResult::new(self.is_verbose()))
                                .add_update_result(index, update_result);
                        }
                        OperationType::Delete => {
                            let delete_result = DeleteResult { deleted_count: n };
                            result
                                .get_or_insert_with(|| BulkWriteResult::new(self.is_verbose()))
                                .add_delete_result(index, delete_result);
                        }
                    }
                }
                SingleOperationResult::Error(write_error) => {
                    error.write_errors.insert(index, write_error);
                }
            }
        }

        Ok(())
    }

    fn get_model(&self, index: usize) -> Result<&WriteModel> {
        self.models.get(index).ok_or_else(|| {
            ErrorKind::InvalidResponse {
                message: format!("invalid operation index returned from bulkWrite: {}", index),
            }
            .into()
        })
    }

    fn get_inserted_id(&self, index: usize) -> Result<Bson> {
        match self.inserted_ids.get(&index) {
            Some(inserted_id) => Ok(inserted_id.clone()),
            None => Err(ErrorKind::InvalidResponse {
                message: format!("invalid index returned for insert operation: {}", index),
            }
            .into()),
        }
    }
}

/// A helper struct for tracking namespace information.
struct NamespaceInfo<'a> {
    namespaces: Vec<RawDocumentBuf>,
    /// Cache the namespaces and their indexes to avoid traversing the namespaces array each time a
    /// namespace is looked up or added.
    cache: HashMap<&'a Namespace, usize>,
}

impl<'a> NamespaceInfo<'a> {
    fn new() -> Self {
        Self {
            namespaces: Vec::new(),
            cache: HashMap::new(),
        }
    }

    /// Gets the index for the given namespace in the nsInfo list, adding it to the list if it is
    /// not already present.
    fn get_index(&mut self, namespace: &'a Namespace) -> (usize, usize) {
        match self.cache.get(namespace) {
            Some(index) => (*index, 0),
            None => {
                let namespace_doc = rawdoc! { "ns": namespace.to_string() };
                let length_added = namespace_doc.as_bytes().len();
                self.namespaces.push(namespace_doc);
                let next_index = self.cache.len();
                self.cache.insert(namespace, next_index);
                (next_index, length_added)
            }
        }
    }
}

impl<'a> OperationWithDefaults for BulkWrite<'a> {
    type O = BulkWriteResult;

    type Command = RawDocumentBuf;

    const NAME: &'static str = "bulkWrite";

    fn build(&mut self, description: &StreamDescription) -> Result<Command<Self::Command>> {
        let max_doc_size: usize = Checked::new(description.max_bson_object_size).try_into()?;
        let max_message_size: usize =
            Checked::new(description.max_message_size_bytes).try_into()?;
        let max_operations: usize = Checked::new(description.max_write_batch_size).try_into()?;

        let mut command_body = rawdoc! { Self::NAME: 1 };
        let options = match self.options {
            Some(options) => bson::to_raw_document_buf(options),
            None => bson::to_raw_document_buf(&BulkWriteOptions::default()),
        }?;
        bson_util::extend_raw_document_buf(&mut command_body, options)?;

        let max_document_sequences_size =
            max_message_size - OP_MSG_OVERHEAD_BYTES - command_body.as_bytes().len();

        let mut namespace_info = NamespaceInfo::new();
        let mut ops = Vec::new();
        let mut current_size = 0;
        for (i, model) in self.models.iter().take(max_operations).enumerate() {
            let (namespace_index, namespace_size) = namespace_info.get_index(model.namespace());

            let operation_namespace_index: i32 = Checked::new(namespace_index).try_into()?;
            let mut operation = rawdoc! { model.operation_name(): operation_namespace_index };
            let (model_doc, inserted_id) = model.get_ops_document_contents()?;
            extend_raw_document_buf(&mut operation, model_doc)?;

            let operation_size = operation.as_bytes().len();
            if operation_size > max_doc_size {
                return Err(ErrorKind::InvalidArgument {
                    message: format!(
                        "bulk write operations must be within {} bytes, but document provided is \
                         {} bytes",
                        max_doc_size, operation_size
                    ),
                }
                .into());
            }

            if current_size + namespace_size + operation_size > max_document_sequences_size {
                // Remove the namespace doc from the list if one was added for this operation.
                if namespace_size > 0 {
                    let last_index = namespace_info.namespaces.len() - 1;
                    namespace_info.namespaces.remove(last_index);
                }
                break;
            }

            if let Some(inserted_id) = inserted_id {
                self.inserted_ids.insert(i, inserted_id);
            }
            current_size += namespace_size + operation_size;
            ops.push(operation);
        }

        self.n_attempted = ops.len();

        let mut command = Command::new(Self::NAME, "admin", command_body);
        command.add_document_sequence("nsInfo", namespace_info.namespaces);
        command.add_document_sequence("ops", ops);
        Ok(command)
    }

    fn handle_response<'b>(
        &'b self,
        response: RawCommandResponse,
        context: ExecutionContext<'b>,
    ) -> BoxFuture<'b, Result<Self::O>> {
        async move {
            let response: WriteResponseBody<Response> = response.body()?;

            let mut bulk_write_error = ClientBulkWriteError::default();

            // A partial result with summary info should only be created if one or more
            // operations were successful.
            let n_errors: usize = Checked::new(response.summary.n_errors).try_into()?;
            if n_errors < self.n_attempted {
                bulk_write_error
                    .partial_result
                    .get_or_insert_with(|| BulkWriteResult::new(self.is_verbose()))
                    .populate_summary_info(&response.summary);
            }

            if let Some(write_concern_error) = response.write_concern_error {
                bulk_write_error
                    .write_concern_errors
                    .push(write_concern_error);
            }

            let specification = CursorSpecification::new(
                response.body.cursor,
                context
                    .connection
                    .stream_description()?
                    .server_address
                    .clone(),
                None,
                None,
                self.options.and_then(|options| options.comment.clone()),
            );
            let pinned_connection = self
                .client
                .pin_connection_for_cursor(&specification, context.connection)?;
            let iteration_result = match context.session {
                Some(session) => {
                    let mut session_cursor =
                        SessionCursor::new(self.client.clone(), specification, pinned_connection);
                    self.iterate_results_cursor(
                        session_cursor.stream(session),
                        &mut bulk_write_error,
                    )
                    .await
                }
                None => {
                    let cursor =
                        Cursor::new(self.client.clone(), specification, None, pinned_connection);
                    self.iterate_results_cursor(cursor, &mut bulk_write_error)
                        .await
                }
            };

            match iteration_result {
                Ok(()) => {
                    if bulk_write_error.write_errors.is_empty()
                        && bulk_write_error.write_concern_errors.is_empty()
                    {
                        Ok(bulk_write_error
                            .partial_result
                            .unwrap_or_else(|| BulkWriteResult::new(self.is_verbose())))
                    } else {
                        let error = Error::new(
                            ErrorKind::ClientBulkWrite(bulk_write_error),
                            response.labels,
                        );
                        Err(error)
                    }
                }
                Err(error) => {
                    let error = Error::new(
                        ErrorKind::ClientBulkWrite(bulk_write_error),
                        response.labels,
                    )
                    .with_source(error);
                    Err(error)
                }
            }
        }
        .boxed()
    }

    fn retryability(&self) -> Retryability {
        if self.models.iter().any(|model| model.multi() == Some(true)) {
            Retryability::None
        } else {
            Retryability::Write
        }
    }
}
