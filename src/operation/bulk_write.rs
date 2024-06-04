mod server_responses;

use std::{collections::HashMap, marker::PhantomData};

use futures_core::TryStream;
use futures_util::{FutureExt, TryStreamExt};

use crate::{
    bson::{rawdoc, Bson, RawDocumentBuf},
    bson_util::{self, extend_raw_document_buf},
    checked::Checked,
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::{BulkWriteError, Error, ErrorKind, Result},
    operation::OperationWithDefaults,
    options::{BulkWriteOptions, OperationType, WriteModel},
    results::{BulkWriteResult, DeleteResult, InsertOneResult, UpdateResult},
    BoxFuture,
    Client,
    Cursor,
    Namespace,
    SessionCursor,
};

use super::{
    ExecutionContext,
    Retryability,
    WriteResponseBody,
    OP_MSG_OVERHEAD_BYTES,
    SERVER_8_0_0_WIRE_VERSION,
};

use server_responses::*;

pub(crate) struct BulkWrite<'a, R>
where
    R: BulkWriteResult,
{
    client: Client,
    models: &'a [WriteModel],
    offset: usize,
    options: Option<&'a BulkWriteOptions>,
    /// The _ids of the inserted documents. This value is populated in `build`.
    inserted_ids: HashMap<usize, Bson>,
    /// The number of writes that were sent to the server. This value is populated in `build`.
    pub(crate) n_attempted: usize,
    _phantom: PhantomData<R>,
}

impl<'a, R> BulkWrite<'a, R>
where
    R: BulkWriteResult,
{
    pub(crate) async fn new(
        client: Client,
        models: &'a [WriteModel],
        offset: usize,
        options: Option<&'a BulkWriteOptions>,
    ) -> BulkWrite<'a, R> {
        Self {
            client,
            models,
            offset,
            options,
            n_attempted: 0,
            inserted_ids: HashMap::new(),
            _phantom: PhantomData,
        }
    }

    async fn iterate_results_cursor(
        &self,
        mut stream: impl TryStream<Ok = SingleOperationResponse, Error = Error> + Unpin,
        result: &mut impl BulkWriteResult,
        error: &mut BulkWriteError,
    ) -> Result<()> {
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
                            result.add_insert_result(index, insert_result);
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
                            result.add_update_result(index, update_result);
                        }
                        OperationType::Delete => {
                            let delete_result = DeleteResult { deleted_count: n };
                            result.add_delete_result(index, delete_result);
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
    // Cache the namespaces and their indexes to avoid traversing the namespaces array each time a
    // namespace is looked up or added.
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

impl<'a, R> OperationWithDefaults for BulkWrite<'a, R>
where
    R: BulkWriteResult,
{
    type O = R;

    const NAME: &'static str = "bulkWrite";

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        if description.max_wire_version.unwrap_or(0) < SERVER_8_0_0_WIRE_VERSION {
            return Err(ErrorKind::IncompatibleServer {
                message: "the bulk write feature is only supported on MongoDB 8.0+".to_string(),
            }
            .into());
        }

        let max_message_size: usize =
            Checked::new(description.max_message_size_bytes).try_into()?;
        let max_operations: usize = Checked::new(description.max_write_batch_size).try_into()?;

        let mut command_body = rawdoc! { Self::NAME: 1 };
        let mut options = match self.options {
            Some(options) => bson::to_raw_document_buf(options),
            None => bson::to_raw_document_buf(&BulkWriteOptions::default()),
        }?;
        options.append("errorsOnly", R::errors_only());
        bson_util::extend_raw_document_buf(&mut command_body, options)?;

        let max_document_sequences_size: usize = (Checked::new(max_message_size)
            - OP_MSG_OVERHEAD_BYTES
            - command_body.as_bytes().len())
        .try_into()?;

        let mut namespace_info = NamespaceInfo::new();
        let mut ops = Vec::new();
        let mut current_size = Checked::new(0);
        for (i, model) in self.models.iter().take(max_operations).enumerate() {
            let (namespace_index, namespace_size) = namespace_info.get_index(model.namespace());

            let operation_namespace_index: i32 = Checked::new(namespace_index).try_into()?;
            let mut operation = rawdoc! { model.operation_name(): operation_namespace_index };
            let (model_doc, inserted_id) = model.get_ops_document_contents()?;
            extend_raw_document_buf(&mut operation, model_doc)?;

            let operation_size = operation.as_bytes().len();

            current_size += namespace_size + operation_size;
            if current_size.get()? > max_document_sequences_size {
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
            ops.push(operation);
        }

        if ops.is_empty() {
            return Err(ErrorKind::InvalidArgument {
                message: format!(
                    "operation at index {} exceeds the maximum message size ({} bytes)",
                    self.offset, max_message_size
                ),
            }
            .into());
        }

        self.n_attempted = ops.len();

        let mut command = Command::new(Self::NAME, "admin", command_body);
        command.add_document_sequence("nsInfo", namespace_info.namespaces);
        command.add_document_sequence("ops", ops);
        Ok(command)
    }

    fn handle_response_async<'b>(
        &'b self,
        response: RawCommandResponse,
        context: ExecutionContext<'b>,
    ) -> BoxFuture<'b, Result<Self::O>> {
        async move {
            let response: WriteResponseBody<Response> = response.body()?;
            let n_errors: usize = Checked::new(response.summary.n_errors).try_into()?;

            let mut error: BulkWriteError = Default::default();
            let mut result: R = Default::default();

            result.populate_summary_info(
                response.summary.n_inserted,
                response.summary.n_matched,
                response.summary.n_modified,
                response.summary.n_upserted,
                response.summary.n_deleted,
            );

            if let Some(write_concern_error) = response.write_concern_error {
                error.write_concern_errors.push(write_concern_error);
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
                        &mut result,
                        &mut error,
                    )
                    .await
                }
                None => {
                    let cursor =
                        Cursor::new(self.client.clone(), specification, None, pinned_connection);
                    self.iterate_results_cursor(cursor, &mut result, &mut error)
                        .await
                }
            };

            if iteration_result.is_ok()
                && error.write_errors.is_empty()
                && error.write_concern_errors.is_empty()
            {
                Ok(result)
            } else {
                // The partial result should only be populated if one or more operations succeeded.
                if n_errors < self.n_attempted {
                    error.partial_result = Some(result.into_partial_result());
                }

                let error = Error::new(ErrorKind::BulkWrite(error), response.labels)
                    .with_source(iteration_result.err());
                Err(error)
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
