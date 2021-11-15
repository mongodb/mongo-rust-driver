use bson::{doc, RawBson, RawDocument, Timestamp};
use lazy_static::lazy_static;
use serde::de::DeserializeOwned;

use std::{collections::HashSet, sync::Arc, time::Instant};

use super::{session::TransactionState, Client, ClientSession};
use crate::{
    bson::Document,
    cmap::{
        conn::PinnedConnectionHandle,
        Connection,
        ConnectionPool,
        RawCommand,
        RawCommandResponse,
    },
    cursor::{session::SessionCursor, Cursor, CursorSpecification},
    error::{
        Error,
        ErrorKind,
        Result,
        RETRYABLE_WRITE_ERROR,
        TRANSIENT_TRANSACTION_ERROR,
        UNKNOWN_TRANSACTION_COMMIT_RESULT,
    },
    event::command::{CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent},
    operation::{AbortTransaction, CommandErrorBody, CommitTransaction, Operation, Retryability},
    options::SelectionCriteria,
    sdam::{
        HandshakePhase,
        SelectedServer,
        ServerType,
        SessionSupportStatus,
        TopologyType,
        TransactionSupportStatus,
    },
    selection_criteria::ReadPreference,
    ClusterTime,
};

lazy_static! {
    pub(crate) static ref REDACTED_COMMANDS: HashSet<&'static str> = {
        let mut hash_set = HashSet::new();
        hash_set.insert("authenticate");
        hash_set.insert("saslstart");
        hash_set.insert("saslcontinue");
        hash_set.insert("getnonce");
        hash_set.insert("createuser");
        hash_set.insert("updateuser");
        hash_set.insert("copydbgetnonce");
        hash_set.insert("copydbsaslstart");
        hash_set.insert("copydb");
        hash_set
    };
    pub(crate) static ref HELLO_COMMAND_NAMES: HashSet<&'static str> = {
        let mut hash_set = HashSet::new();
        hash_set.insert("hello");
        hash_set.insert("ismaster");
        hash_set
    };
}

impl Client {
    /// Execute the given operation.
    ///
    /// Server selection will performed using the criteria specified on the operation, if any, and
    /// an implicit session will be created if the operation and write concern are compatible with
    /// sessions and an explicit session is not provided.
    pub(crate) async fn execute_operation<T: Operation>(
        &self,
        op: T,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<T::O> {
        self.execute_operation_with_details(op, session)
            .await
            .map(|details| details.output.operation_output)
    }

    async fn execute_operation_with_details<T: Operation>(
        &self,
        op: T,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<ExecutionDetails<T>> {
        Box::pin(async {
            // TODO RUST-9: allow unacknowledged write concerns
            if !op.is_acknowledged() {
                return Err(ErrorKind::InvalidArgument {
                    message: "Unacknowledged write concerns are not supported".to_string(),
                }
                .into());
            }
            let mut implicit_session = None;
            let session = match session.into() {
                Some(session) => {
                    if !Arc::ptr_eq(&self.inner, &session.client().inner) {
                        return Err(ErrorKind::InvalidArgument {
                            message: "the session provided to an operation must be created from \
                                      the same client as the collection/database"
                                .into(),
                        }
                        .into());
                    }

                    if let Some(SelectionCriteria::ReadPreference(read_preference)) =
                        op.selection_criteria()
                    {
                        if session.in_transaction() && read_preference != &ReadPreference::Primary {
                            return Err(ErrorKind::Transaction {
                                message: "read preference in a transaction must be primary".into(),
                            }
                            .into());
                        }
                    }
                    Some(session)
                }
                None => {
                    implicit_session = self.start_implicit_session(&op).await?;
                    implicit_session.as_mut()
                }
            };
            let output = self.execute_operation_with_retry(op, session).await?;
            Ok(ExecutionDetails {
                output,
                implicit_session,
            })
        })
        .await
    }

    /// Execute the given operation, returning the cursor created by the operation.
    ///
    /// Server selection be will performed using the criteria specified on the operation, if any.
    pub(crate) async fn execute_cursor_operation<Op, T>(&self, op: Op) -> Result<Cursor<T>>
    where
        Op: Operation<O = CursorSpecification>,
        T: DeserializeOwned + Unpin + Send + Sync,
    {
        Box::pin(async {
            let mut details = self.execute_operation_with_details(op, None).await?;
            let pinned = self.pin_connection_for_cursor(&mut details.output)?;
            Ok(Cursor::new(
                self.clone(),
                details.output.operation_output,
                details.implicit_session,
                pinned,
            ))
        })
        .await
    }

    pub(crate) async fn execute_session_cursor_operation<Op, T>(
        &self,
        op: Op,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<T>>
    where
        Op: Operation<O = CursorSpecification>,
        T: DeserializeOwned + Unpin + Send + Sync,
    {
        let mut details = self
            .execute_operation_with_details(op, &mut *session)
            .await?;

        let pinned = if let Some(handle) = session.transaction.pinned_connection() {
            // Cursor operations on a transaction share the same pinned connection.
            Some(handle.replicate())
        } else {
            self.pin_connection_for_cursor(&mut details.output)?
        };
        Ok(SessionCursor::new(
            self.clone(),
            details.output.operation_output,
            pinned,
        ))
    }

    fn is_load_balanced(&self) -> bool {
        self.inner.options.load_balanced.unwrap_or(false)
    }

    fn pin_connection_for_cursor<Op>(
        &self,
        details: &mut ExecutionOutput<Op>,
    ) -> Result<Option<PinnedConnectionHandle>>
    where
        Op: Operation<O = CursorSpecification>,
    {
        if self.is_load_balanced() && details.operation_output.info.id != 0 {
            Ok(Some(details.connection.pin()?))
        } else {
            Ok(None)
        }
    }

    /// Selects a server and executes the given operation on it, optionally using a provided
    /// session. Retries the operation upon failure if retryability is supported.
    async fn execute_operation_with_retry<T: Operation>(
        &self,
        mut op: T,
        mut session: Option<&mut ClientSession>,
    ) -> Result<ExecutionOutput<T>> {
        // If the current transaction has been committed/aborted and it is not being
        // re-committed/re-aborted, reset the transaction's state to TransactionState::None.
        if let Some(ref mut session) = session {
            if matches!(
                session.transaction.state,
                TransactionState::Committed { .. }
            ) && op.name() != CommitTransaction::NAME
                || session.transaction.state == TransactionState::Aborted
                    && op.name() != AbortTransaction::NAME
            {
                session.transaction.reset();
            }
        }

        let selection_criteria = session
            .as_ref()
            .and_then(|s| s.transaction.pinned_mongos())
            .or_else(|| op.selection_criteria());

        let server = match self.select_server(selection_criteria).await {
            Ok(server) => server,
            Err(mut err) => {
                err.add_labels_and_update_pin(None, &mut session, None)?;
                return Err(err);
            }
        };

        let mut conn = match get_connection(&session, &op, &server.pool).await {
            Ok(c) => c,
            Err(mut err) => {
                err.add_labels_and_update_pin(None, &mut session, None)?;

                if err.is_pool_cleared() {
                    return self.execute_retry(&mut op, &mut session, None, err).await;
                } else {
                    return Err(err);
                }
            }
        };

        let retryability = self.get_retryability(&conn, &op, &session).await?;

        let txn_number = match session {
            Some(ref mut session) => {
                if session.transaction.state != TransactionState::None {
                    Some(session.txn_number())
                } else {
                    match retryability {
                        Retryability::Write => Some(session.get_and_increment_txn_number()),
                        _ => None,
                    }
                }
            }
            None => None,
        };

        match self
            .execute_operation_on_connection(
                &mut op,
                &mut conn,
                &mut session,
                txn_number,
                &retryability,
            )
            .await
        {
            Ok(operation_output) => Ok(ExecutionOutput {
                operation_output,
                connection: conn,
            }),
            Err(mut err) => {
                // Retryable writes are only supported by storage engines with document-level
                // locking, so users need to disable retryable writes if using mmapv1.
                if let ErrorKind::Command(ref mut command_error) = *err.kind {
                    if command_error.code == 20
                        && command_error.message.starts_with("Transaction numbers")
                    {
                        command_error.message = "This MongoDB deployment does not support \
                                                 retryable writes. Please add retryWrites=false \
                                                 to your connection string."
                            .to_string();
                    }
                }

                self.inner
                    .topology
                    .handle_application_error(
                        err.clone(),
                        HandshakePhase::after_completion(&conn),
                        &server,
                    )
                    .await;
                // release the connection to be processed by the connection pool
                drop(conn);
                // release the selected server to decrement its operation count
                drop(server);

                if retryability == Retryability::Read && err.is_read_retryable()
                    || retryability == Retryability::Write && err.is_write_retryable()
                {
                    self.execute_retry(&mut op, &mut session, txn_number, err)
                        .await
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn execute_retry<T: Operation>(
        &self,
        op: &mut T,
        session: &mut Option<&mut ClientSession>,
        txn_number: Option<i64>,
        first_error: Error,
    ) -> Result<ExecutionOutput<T>> {
        op.update_for_retry();

        let server = match self.select_server(op.selection_criteria()).await {
            Ok(server) => server,
            Err(_) => {
                return Err(first_error);
            }
        };

        let mut conn = match get_connection(session, op, &server.pool).await {
            Ok(c) => c,
            Err(_) => return Err(first_error),
        };

        let retryability = self.get_retryability(&conn, op, session).await?;
        if retryability == Retryability::None {
            return Err(first_error);
        }

        match self
            .execute_operation_on_connection(op, &mut conn, session, txn_number, &retryability)
            .await
        {
            Ok(operation_output) => Ok(ExecutionOutput {
                operation_output,
                connection: conn,
            }),
            Err(err) => {
                self.inner
                    .topology
                    .handle_application_error(
                        err.clone(),
                        HandshakePhase::after_completion(&conn),
                        &server,
                    )
                    .await;
                drop(server);

                if err.is_server_error() || err.is_read_retryable() || err.is_write_retryable() {
                    Err(err)
                } else {
                    Err(first_error)
                }
            }
        }
    }

    /// Executes an operation on a given connection, optionally using a provided session.
    async fn execute_operation_on_connection<T: Operation>(
        &self,
        op: &mut T,
        connection: &mut Connection,
        session: &mut Option<&mut ClientSession>,
        txn_number: Option<i64>,
        retryability: &Retryability,
    ) -> Result<T::O> {
        if let Some(wc) = op.write_concern() {
            wc.validate()?;
        }

        let stream_description = connection.stream_description()?;
        let is_sharded = stream_description.initial_server_type == ServerType::Mongos;
        let mut cmd = op.build(stream_description)?;
        self.inner
            .topology
            .update_command_with_read_pref(connection.address(), &mut cmd, op.selection_criteria())
            .await;

        match session {
            Some(ref mut session) if op.supports_sessions() && op.is_acknowledged() => {
                cmd.set_session(session);
                if let Some(txn_number) = txn_number {
                    cmd.set_txn_number(txn_number);
                }
                if session
                    .options()
                    .and_then(|opts| opts.snapshot)
                    .unwrap_or(false)
                {
                    if connection
                        .stream_description()?
                        .max_wire_version
                        .unwrap_or(0)
                        < 13
                    {
                        let labels: Option<Vec<_>> = None;
                        return Err(Error::new(
                            ErrorKind::IncompatibleServer {
                                message: "Snapshot reads require MongoDB 5.0 or later".into(),
                            },
                            labels,
                        ));
                    }
                    cmd.set_snapshot_read_concern(session);
                }
                // If this is a causally consistent session, set `readConcern.afterClusterTime`.
                // Causal consistency defaults to true, unless snapshot is true.
                else if session
                    .options()
                    .and_then(|opts| opts.causal_consistency)
                    .unwrap_or(true)
                    && matches!(
                        session.transaction.state,
                        TransactionState::None | TransactionState::Starting
                    )
                    && op.supports_read_concern(stream_description)
                {
                    cmd.set_after_cluster_time(session);
                }

                match session.transaction.state {
                    TransactionState::Starting => {
                        cmd.set_start_transaction();
                        cmd.set_autocommit();

                        if let Some(ref options) = session.transaction.options {
                            if let Some(ref read_concern) = options.read_concern {
                                cmd.set_read_concern_level(read_concern.level.clone());
                            }
                        }
                        if self.is_load_balanced() {
                            session.pin_connection(connection.pin()?);
                        } else if is_sharded {
                            session.pin_mongos(connection.address().clone());
                        }
                        session.transaction.state = TransactionState::InProgress;
                    }
                    TransactionState::InProgress => cmd.set_autocommit(),
                    TransactionState::Committed { .. } | TransactionState::Aborted => {
                        cmd.set_autocommit();

                        // Append the recovery token to the command if we are committing or aborting
                        // on a sharded transaction.
                        if is_sharded {
                            if let Some(ref recovery_token) = session.transaction.recovery_token {
                                cmd.set_recovery_token(recovery_token);
                            }
                        }
                    }
                    _ => {}
                }
                session.update_last_use();
            }
            Some(ref session) if !op.supports_sessions() && !session.is_implicit() => {
                return Err(ErrorKind::InvalidArgument {
                    message: format!("{} does not support sessions", cmd.name),
                }
                .into());
            }
            Some(ref session) if !op.is_acknowledged() && !session.is_implicit() => {
                return Err(ErrorKind::InvalidArgument {
                    message: "Cannot use ClientSessions with unacknowledged write concern"
                        .to_string(),
                }
                .into());
            }
            _ => {}
        }

        let session_cluster_time = session.as_ref().and_then(|session| session.cluster_time());
        let client_cluster_time = self.inner.topology.cluster_time().await;
        let max_cluster_time = std::cmp::max(session_cluster_time, client_cluster_time.as_ref());
        if let Some(cluster_time) = max_cluster_time {
            cmd.set_cluster_time(cluster_time);
        }

        let connection_info = connection.info();
        let service_id = connection.service_id();
        let request_id = crate::cmap::conn::next_request_id();

        if let Some(ref server_api) = self.inner.options.server_api {
            cmd.set_server_api(server_api);
        }

        let should_redact = cmd.should_redact();

        let cmd_name = cmd.name.clone();
        let target_db = cmd.target_db.clone();

        let serialized = op.serialize_command(cmd)?;
        let raw_cmd = RawCommand {
            name: cmd_name.clone(),
            target_db,
            bytes: serialized,
        };

        self.emit_command_event(|handler| {
            let command_body = if should_redact {
                Document::new()
            } else {
                Document::from_reader(raw_cmd.bytes.as_slice())
                    .unwrap_or_else(|e| doc! { "serialization error": e.to_string() })
            };
            let command_started_event = CommandStartedEvent {
                command: command_body,
                db: raw_cmd.target_db.clone(),
                command_name: raw_cmd.name.clone(),
                request_id,
                connection: connection_info.clone(),
                service_id,
            };

            handler.handle_command_started_event(command_started_event);
        });

        let start_time = Instant::now();
        let command_result = match connection.send_raw_command(raw_cmd, request_id).await {
            Ok(response) => {
                async fn handle_response<T: Operation>(
                    client: &Client,
                    op: &T,
                    session: &mut Option<&mut ClientSession>,
                    is_sharded: bool,
                    response: RawCommandResponse,
                ) -> Result<RawCommandResponse> {
                    let raw_doc = RawDocument::new(response.as_bytes())?;

                    let ok = match raw_doc.get("ok")? {
                        Some(b) => crate::bson_util::get_int_raw(b).ok_or_else(|| {
                            ErrorKind::InvalidResponse {
                                message: format!(
                                    "expected ok value to be a number, instead got {:?}",
                                    b
                                ),
                            }
                        })?,
                        None => {
                            return Err(ErrorKind::InvalidResponse {
                                message: "missing 'ok' value in response".to_string(),
                            }
                            .into())
                        }
                    };

                    let cluster_time: Option<ClusterTime> = raw_doc
                        .get("$clusterTime")?
                        .and_then(RawBson::as_document)
                        .map(|d| bson::from_slice(d.as_bytes()))
                        .transpose()?;

                    let at_cluster_time = op.extract_at_cluster_time(raw_doc)?;

                    client
                        .update_cluster_time(cluster_time, at_cluster_time, session)
                        .await;

                    if let (Some(session), Some(ts)) = (
                        session.as_mut(),
                        raw_doc
                            .get("operationTime")?
                            .and_then(RawBson::as_timestamp),
                    ) {
                        session.advance_operation_time(ts);
                    }

                    if ok == 1 {
                        if let Some(ref mut session) = session {
                            if is_sharded && session.in_transaction() {
                                let recovery_token = raw_doc
                                    .get("recoveryToken")?
                                    .and_then(RawBson::as_document)
                                    .map(|d| bson::from_slice(d.as_bytes()))
                                    .transpose()?;
                                session.transaction.recovery_token = recovery_token;
                            }
                        }

                        Ok(response)
                    } else {
                        Err(response
                            .body::<CommandErrorBody>()
                            .map(|error_response| error_response.into())
                            .unwrap_or_else(|e| {
                                Error::from(ErrorKind::InvalidResponse {
                                    message: format!("error deserializing command error: {}", e),
                                })
                            }))
                    }
                }

                handle_response(self, op, session, is_sharded, response).await
            }
            Err(err) => Err(err),
        };

        let duration = start_time.elapsed();

        match command_result {
            Err(mut err) => {
                self.emit_command_event(|handler| {
                    let command_failed_event = CommandFailedEvent {
                        duration,
                        command_name: cmd_name,
                        failure: err.clone(),
                        request_id,
                        connection: connection_info,
                        service_id,
                    };

                    handler.handle_command_failed_event(command_failed_event);
                });

                if let Some(ref mut session) = session {
                    if err.is_network_error() {
                        session.mark_dirty();
                    }
                }

                err.add_labels_and_update_pin(Some(connection), session, Some(retryability))?;
                op.handle_error(err)
            }
            Ok(response) => {
                self.emit_command_event(|handler| {
                    let reply = if should_redact {
                        Document::new()
                    } else {
                        response
                            .body()
                            .unwrap_or_else(|e| doc! { "deserialization error": e.to_string() })
                    };

                    let command_succeeded_event = CommandSucceededEvent {
                        duration,
                        reply,
                        command_name: cmd_name.clone(),
                        request_id,
                        connection: connection_info,
                        service_id,
                    };
                    handler.handle_command_succeeded_event(command_succeeded_event);
                });

                match op.handle_response(response, connection.stream_description()?) {
                    Ok(response) => Ok(response),
                    Err(mut err) => {
                        err.add_labels_and_update_pin(
                            Some(connection),
                            session,
                            Some(retryability),
                        )?;
                        Err(err)
                    }
                }
            }
        }
    }

    /// Start an implicit session if the operation and write concern are compatible with sessions.
    async fn start_implicit_session<T: Operation>(&self, op: &T) -> Result<Option<ClientSession>> {
        match self.get_session_support_status().await? {
            SessionSupportStatus::Supported {
                logical_session_timeout,
            } if op.supports_sessions() && op.is_acknowledged() => Ok(Some(
                self.start_session_with_timeout(logical_session_timeout, None, true)
                    .await,
            )),
            _ => Ok(None),
        }
    }

    async fn select_data_bearing_server(&self) -> Result<()> {
        let topology_type = self.inner.topology.topology_type().await;
        let criteria = SelectionCriteria::Predicate(Arc::new(move |server_info| {
            let server_type = server_info.server_type();
            (matches!(topology_type, TopologyType::Single) && server_type.is_available())
                || server_type.is_data_bearing()
        }));
        let _: SelectedServer = self.select_server(Some(&criteria)).await?;
        Ok(())
    }

    /// Gets whether the topology supports sessions, and if so, returns the topology's logical
    /// session timeout. If it has yet to be determined if the topology supports sessions, this
    /// method will perform a server selection that will force that determination to be made.
    pub(crate) async fn get_session_support_status(&self) -> Result<SessionSupportStatus> {
        let initial_status = self.inner.topology.session_support_status().await;

        // Need to guarantee that we're connected to at least one server that can determine if
        // sessions are supported or not.
        match initial_status {
            SessionSupportStatus::Undetermined => {
                self.select_data_bearing_server().await?;
                Ok(self.inner.topology.session_support_status().await)
            }
            _ => Ok(initial_status),
        }
    }

    /// Gets whether the topology supports transactions. If it has yet to be determined if the
    /// topology supports transactions, this method will perform a server selection that will force
    /// that determination to be made.
    pub(crate) async fn transaction_support_status(&self) -> Result<TransactionSupportStatus> {
        let initial_status = self.inner.topology.transaction_support_status().await;

        // Need to guarantee that we're connected to at least one server that can determine if
        // sessions are supported or not.
        match initial_status {
            TransactionSupportStatus::Undetermined => {
                self.select_data_bearing_server().await?;
                Ok(self.inner.topology.transaction_support_status().await)
            }
            _ => Ok(initial_status),
        }
    }

    /// Returns the retryability level for the execution of this operation.
    async fn get_retryability<T: Operation>(
        &self,
        conn: &Connection,
        op: &T,
        session: &Option<&mut ClientSession>,
    ) -> Result<Retryability> {
        if !session
            .as_ref()
            .map(|session| session.in_transaction())
            .unwrap_or(false)
        {
            match op.retryability() {
                Retryability::Read if self.inner.options.retry_reads != Some(false) => {
                    return Ok(Retryability::Read);
                }
                Retryability::Write if conn.stream_description()?.supports_retryable_writes() => {
                    // commitTransaction and abortTransaction should be retried regardless of the
                    // value for retry_writes set on the Client
                    if op.name() == CommitTransaction::NAME
                        || op.name() == AbortTransaction::NAME
                        || self.inner.options.retry_writes != Some(false)
                    {
                        return Ok(Retryability::Write);
                    }
                }
                _ => {}
            }
        }
        Ok(Retryability::None)
    }

    async fn update_cluster_time(
        &self,
        cluster_time: Option<ClusterTime>,
        at_cluster_time: Option<Timestamp>,
        session: &mut Option<&mut ClientSession>,
    ) {
        if let Some(ref cluster_time) = cluster_time {
            self.inner.topology.advance_cluster_time(cluster_time).await;
            if let Some(ref mut session) = session {
                session.advance_cluster_time(cluster_time)
            }
        }

        if let Some(timestamp) = at_cluster_time {
            if let Some(ref mut session) = session {
                session.snapshot_time = Some(timestamp);
            }
        }
    }
}

async fn get_connection<T: Operation>(
    session: &Option<&mut ClientSession>,
    op: &T,
    pool: &ConnectionPool,
) -> Result<Connection> {
    let session_pinned = session
        .as_ref()
        .and_then(|s| s.transaction.pinned_connection());
    match (session_pinned, op.pinned_connection()) {
        (Some(c), None) | (None, Some(c)) => c.take_connection().await,
        (Some(session_handle), Some(op_handle)) => {
            // An operation executing in a transaction should be sharing the same pinned connection.
            debug_assert_eq!(session_handle.id(), op_handle.id());
            session_handle.take_connection().await
        }
        (None, None) => pool.check_out().await,
    }
}

impl Error {
    /// Adds the necessary labels to this Error, and unpins the session if needed.
    ///
    /// A TransientTransactionError label should be added if a transaction is in progress and the
    /// error is a network or server selection error.
    ///
    /// On a pre-4.4 connection, a RetryableWriteError label should be added to any write-retryable
    /// error. On a 4.4+ connection, a label should only be added to network errors. Regardless of
    /// server version, a label should only be added if the `retry_writes` client option is not set
    /// to `false`, the operation during which the error occured is write-retryable, and a
    /// TransientTransactionError label has not already been added.
    ///
    /// If the TransientTransactionError or UnknownTransactionCommitResult labels are added, the
    /// ClientSession should be unpinned.
    fn add_labels_and_update_pin(
        &mut self,
        conn: Option<&Connection>,
        session: &mut Option<&mut ClientSession>,
        retryability: Option<&Retryability>,
    ) -> Result<()> {
        let transaction_state = session.as_ref().map_or(&TransactionState::None, |session| {
            &session.transaction.state
        });
        let max_wire_version = if let Some(conn) = conn {
            conn.stream_description()?.max_wire_version
        } else {
            None
        };
        match transaction_state {
            TransactionState::Starting | TransactionState::InProgress => {
                if self.is_network_error() || self.is_server_selection_error() {
                    self.add_label(TRANSIENT_TRANSACTION_ERROR);
                }
            }
            TransactionState::Committed { .. } => {
                if let Some(max_wire_version) = max_wire_version {
                    if self.should_add_retryable_write_label(max_wire_version) {
                        self.add_label(RETRYABLE_WRITE_ERROR);
                    }
                }
                if self.should_add_unknown_transaction_commit_result_label() {
                    self.add_label(UNKNOWN_TRANSACTION_COMMIT_RESULT);
                }
            }
            TransactionState::Aborted => {
                if let Some(max_wire_version) = max_wire_version {
                    if self.should_add_retryable_write_label(max_wire_version) {
                        self.add_label(RETRYABLE_WRITE_ERROR);
                    }
                }
            }
            TransactionState::None => {
                if retryability == Some(&Retryability::Write) {
                    if let Some(max_wire_version) = max_wire_version {
                        if self.should_add_retryable_write_label(max_wire_version) {
                            self.add_label(RETRYABLE_WRITE_ERROR);
                        }
                    }
                }
            }
        }

        if let Some(ref mut session) = session {
            if self.contains_label(TRANSIENT_TRANSACTION_ERROR)
                || self.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
            {
                session.unpin();
            }
        }

        Ok(())
    }
}

struct ExecutionDetails<T: Operation> {
    output: ExecutionOutput<T>,
    implicit_session: Option<ClientSession>,
}

struct ExecutionOutput<T: Operation> {
    operation_output: T::O,
    connection: Connection,
}
