#[cfg(feature = "in-use-encryption")]
use crate::bson::RawDocumentBuf;
use crate::{
    bson::{doc, RawBsonRef, RawDocument, Timestamp},
    cursor::{CursorInformation, CursorSpecification},
    operation::{Feature, OperationTarget},
};
#[cfg(feature = "in-use-encryption")]
use futures_core::future::BoxFuture;
use serde::de::DeserializeOwned;
use std::{borrow::Cow, sync::LazyLock};

use std::{
    borrow::BorrowMut,
    collections::HashSet,
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use super::{options::ServerAddress, session::TransactionState, Client, ClientSession};
use crate::{
    bson::Document,
    change_stream::{
        event::ChangeStreamEvent,
        session::SessionChangeStream,
        ChangeStream,
        ChangeStreamData,
        WatchArgs,
    },
    cmap::{
        conn::{
            pooled::PooledConnection,
            wire::{next_request_id, Message},
            PinnedConnectionHandle,
        },
        ConnectionPool,
        RawCommandResponse,
        StreamDescription,
    },
    cursor::{session::SessionCursor, Cursor},
    error::{
        Error,
        ErrorKind,
        Result,
        RETRYABLE_WRITE_ERROR,
        TRANSIENT_TRANSACTION_ERROR,
        UNKNOWN_TRANSACTION_COMMIT_RESULT,
    },
    event::command::{
        CommandEvent,
        CommandFailedEvent,
        CommandStartedEvent,
        CommandSucceededEvent,
    },
    hello::LEGACY_HELLO_COMMAND_NAME_LOWERCASE,
    operation::{
        aggregate::change_stream::ChangeStreamAggregate,
        AbortTransaction,
        CommandErrorBody,
        CommitTransaction,
        ExecutionContext,
        Operation,
        Retryability,
    },
    options::{ChangeStreamOptions, SelectionCriteria},
    sdam::{HandshakePhase, ServerType, TopologyType, TransactionSupportStatus},
    selection_criteria::ReadPreference,
    tracking_arc::TrackingArc,
    ClusterTime,
};

pub(crate) static REDACTED_COMMANDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
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
});
pub(crate) static HELLO_COMMAND_NAMES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut hash_set = HashSet::new();
    hash_set.insert("hello");
    hash_set.insert(LEGACY_HELLO_COMMAND_NAME_LOWERCASE);
    hash_set
});

impl Client {
    /// Execute the given operation.
    ///
    /// Server selection will performed using the criteria specified on the operation, if any, and
    /// an implicit session will be created if the operation and write concern are compatible with
    /// sessions and an explicit session is not provided.
    pub(crate) async fn execute_operation<T: Operation>(
        &self,
        mut op: impl BorrowMut<T>,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<T::O> {
        self.execute_operation_with_details(op.borrow_mut(), session)
            .await
            .map(|details| details.output)
    }

    #[cfg(not(feature = "opentelemetry"))]
    async fn execute_operation_with_details<T: Operation>(
        &self,
        op: &mut T,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<ExecutionDetails<T>> {
        self.execute_operation_with_details_inner(op, session.into())
            .await
    }

    #[cfg(feature = "opentelemetry")]
    async fn execute_operation_with_details<T: Operation>(
        &self,
        op: &mut T,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<ExecutionDetails<T>> {
        use crate::otel::FutureExt as _;

        let session = session.into();
        let span = self.start_operation_span(op, session.as_deref());
        let result = self
            .execute_operation_with_details_inner(op, session)
            .with_span(&span)
            .await;
        span.record_error(&result);

        result
    }

    async fn execute_operation_with_details_inner<T: Operation>(
        &self,
        op: &mut T,
        mut session: Option<&mut ClientSession>,
    ) -> Result<ExecutionDetails<T>> {
        // Validate inputs that can be checked before server selection and connection
        // checkout.
        if self.inner.shutdown.executed.load(Ordering::SeqCst) {
            return Err(ErrorKind::Shutdown.into());
        }

        let op_target = op.target();
        if session.as_ref().map_or(false, |s| s.in_transaction()) {
            if op.read_concern().is_set() {
                return Err(Error::invalid_argument(
                    "Cannot set read concern after starting a transaction",
                ));
            }
            if op.write_concern().is_set() {
                return Err(Error::invalid_argument(
                    "Cannot set write concern after starting a transaction",
                ));
            }
        } else {
            // Apply write concern inheritance
            if op.write_concern().is_inherit() {
                if let Some(wc) = op_target.write_concern() {
                    op.set_write_concern(wc.clone());
                }
            }
        }

        if let Feature::Set(wc) = op.write_concern() {
            // TODO RUST-9: remove this validation
            if !wc.is_acknowledged() {
                return Err(ErrorKind::InvalidArgument {
                    message: "Unacknowledged write concerns are not supported".to_string(),
                }
                .into());
            }

            wc.validate()?;
        }

        let selection_criteria = match op.selection_criteria() {
            Feature::Set(s) => Some(s),
            Feature::NotSupported => None,
            Feature::Inherit => session
                .as_ref()
                .and_then(|s| {
                    if s.in_transaction() {
                        s.transaction
                            .options
                            .as_ref()
                            .and_then(|o| o.selection_criteria.as_ref())
                    } else {
                        None
                    }
                })
                .or_else(|| op_target.selection_criteria()),
        }
        .cloned();

        // Validate the session and update its transaction status if needed.
        if let Some(ref mut session) = session {
            if !TrackingArc::ptr_eq(&self.inner, &session.client().inner) {
                return Err(Error::invalid_argument(
                    "the session provided to an operation must be created from the same client as \
                     the collection/database on which the operation is being performed",
                ));
            }
            if selection_criteria
                .as_ref()
                .and_then(|sc| sc.as_read_pref())
                .is_some_and(|rp| rp != &ReadPreference::Primary)
                && session.in_transaction()
            {
                return Err(ErrorKind::Transaction {
                    message: "read preference in a transaction must be primary".into(),
                }
                .into());
            }
            // If the current transaction has been committed/aborted and it is not being
            // re-committed/re-aborted, reset the transaction's state to None.
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

        Box::pin(async {
            self.execute_operation_with_retry(op, selection_criteria, session)
                .await
        })
        .await
    }

    /// Execute the given operation, returning the cursor created by the operation.
    ///
    /// Server selection be will performed using the criteria specified on the operation, if any.
    pub(crate) async fn execute_cursor_operation<Op, C>(
        &self,
        op: &mut Op,
        mut session: Option<&mut ClientSession>,
    ) -> Result<C>
    where
        Op: Operation<O = CursorSpecification>,
        C: crate::cursor::NewCursor,
    {
        Box::pin(async {
            let mut details = self
                .execute_operation_with_details(op, session.as_deref_mut())
                .await?;
            let pinned = self.pin_connection_for_cursor(
                &details.output.info,
                &mut details.connection,
                session,
            )?;
            C::generic_new(
                self.clone(),
                details.output,
                details.implicit_session,
                pinned,
            )
        })
        .await
    }

    pub(crate) fn is_load_balanced(&self) -> bool {
        self.inner.options.load_balanced.unwrap_or(false)
    }

    pub(crate) fn pin_connection_for_cursor(
        &self,
        info: &CursorInformation,
        conn: &mut PooledConnection,
        session: Option<&mut ClientSession>,
    ) -> Result<Option<PinnedConnectionHandle>> {
        if let Some(handle) = session.and_then(|s| s.transaction.pinned_connection()) {
            // Cursor operations on a transaction share the same pinned connection.
            Ok(Some(handle.replicate()))
        } else if self.is_load_balanced() && info.id != 0 {
            // Cursor operations on load balanced topologies always pin connections.
            Ok(Some(conn.pin()?))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn execute_watch<T>(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: Option<ChangeStreamOptions>,
        target: OperationTarget,
        mut resume_data: Option<ChangeStreamData>,
    ) -> Result<ChangeStream<ChangeStreamEvent<T>>>
    where
        T: DeserializeOwned + Unpin + Send + Sync,
    {
        Box::pin(async {
            let pipeline: Vec<_> = pipeline.into_iter().collect();
            let args = WatchArgs {
                pipeline,
                target,
                options,
            };
            let mut implicit_session = resume_data
                .as_mut()
                .and_then(|rd| rd.implicit_session.take());
            let mut op = ChangeStreamAggregate::new(&args, resume_data)?;

            let mut details = self
                .execute_operation_with_details(&mut op, implicit_session.as_mut())
                .await?;
            if let Some(session) = implicit_session {
                details.implicit_session = Some(session);
            }
            let (cursor_spec, cs_data) = details.output;
            let pinned =
                self.pin_connection_for_cursor(&cursor_spec.info, &mut details.connection, None)?;
            let cursor = Cursor::new(self.clone(), cursor_spec, details.implicit_session, pinned)?;

            Ok(ChangeStream::new(cursor, args, cs_data))
        })
        .await
    }

    pub(crate) async fn execute_watch_with_session<T>(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: Option<ChangeStreamOptions>,
        target: OperationTarget,
        resume_data: Option<ChangeStreamData>,
        session: &mut ClientSession,
    ) -> Result<SessionChangeStream<ChangeStreamEvent<T>>>
    where
        T: DeserializeOwned + Unpin + Send + Sync,
    {
        Box::pin(async {
            let pipeline: Vec<_> = pipeline.into_iter().collect();
            let args = WatchArgs {
                pipeline,
                target,
                options,
            };
            let mut op = ChangeStreamAggregate::new(&args, resume_data)?;

            let mut details = self
                .execute_operation_with_details(&mut op, &mut *session)
                .await?;
            let (cursor_spec, cs_data) = details.output;
            let pinned = self.pin_connection_for_cursor(
                &cursor_spec.info,
                &mut details.connection,
                Some(session),
            )?;
            let cursor = SessionCursor::new(self.clone(), cursor_spec, pinned)?;

            Ok(SessionChangeStream::new(cursor, args, cs_data))
        })
        .await
    }

    /// Selects a server and executes the given operation on it, optionally using a provided
    /// session. Retries the operation upon failure if retryability is supported or after
    /// reauthenticating if reauthentication is required.
    async fn execute_operation_with_retry<T: Operation>(
        &self,
        op: &mut T,
        selection_criteria: Option<SelectionCriteria>,
        mut session: Option<&mut ClientSession>,
    ) -> Result<ExecutionDetails<T>> {
        let mut retry: Option<ExecutionRetry> = None;
        let mut implicit_session: Option<ClientSession> = None;

        loop {
            if retry.is_some() {
                op.update_for_retry();
            }

            let selection_criteria = session
                .as_ref()
                .and_then(|s| s.transaction.pinned_mongos())
                .or_else(|| selection_criteria.as_ref());

            let (server, effective_criteria) = match self
                .select_server(
                    selection_criteria,
                    &retry
                        .as_ref()
                        .map(|r| vec![&r.first_server])
                        .unwrap_or_default(),
                    (&*op).into(),
                )
                .await
            {
                Ok(out) => out,
                Err(mut err) => {
                    retry.first_error()?;

                    err.add_labels_and_update_pin(None, &mut session, None);
                    return Err(err);
                }
            };
            let server_addr = server.address.clone();

            let mut conn = match get_connection(&session, op, &server.pool).await {
                Ok(c) => c,
                Err(mut err) => {
                    retry.first_error()?;

                    err.add_labels_and_update_pin(None, &mut session, None);
                    if err.is_read_retryable() && self.inner.options.retry_writes != Some(false) {
                        err.add_label(RETRYABLE_WRITE_ERROR);
                    }

                    let retryability = op.retryability().with_options(self.options());
                    let can_retry = match retryability {
                        // Read-retryable operations should be retried on pool cleared errors during
                        // connection checkout regardless of transaction status.
                        Retryability::Read if err.is_pool_cleared() => true,
                        _ => {
                            retryability.can_retry_error(&err)
                                && !session
                                    .as_ref()
                                    .is_some_and(|session| session.in_transaction())
                        }
                    };
                    if can_retry {
                        retry = Some(ExecutionRetry {
                            prior_txn_number: None,
                            first_error: err,
                            first_server: server_addr.clone(),
                        });
                        continue;
                    } else {
                        return Err(err);
                    }
                }
            };

            if !conn.supports_sessions() && session.is_some() {
                return Err(ErrorKind::SessionsNotSupported.into());
            }

            if conn.supports_sessions()
                && session.is_none()
                && op.supports_sessions()
                && op.write_concern().is_acknowledged()
            {
                implicit_session = Some(ClientSession::new(self.clone(), None, true).await);
                session = implicit_session.as_mut();
            }

            let retryability = self.get_retryability(op, &session, conn.stream_description()?);
            if retryability == Retryability::None {
                retry.first_error()?;
            }

            let txn_number =
                if let Some(txn_number) = retry.as_ref().and_then(|r| r.prior_txn_number) {
                    Some(txn_number)
                } else {
                    session
                        .as_mut()
                        .and_then(|s| s.get_txn_number_for_operation(retryability))
                };

            let details = match self
                .execute_operation_on_connection(
                    op,
                    &mut conn,
                    &mut session,
                    txn_number,
                    retryability,
                    effective_criteria,
                )
                .await
            {
                Ok(output) => ExecutionDetails {
                    output,
                    connection: conn,
                    implicit_session,
                },
                Err(mut err) => {
                    err.wire_version = conn.stream_description()?.max_wire_version;

                    // Retryable writes are only supported by storage engines with document-level
                    // locking, so users need to disable retryable writes if using mmapv1.
                    if let ErrorKind::Command(ref mut command_error) = *err.kind {
                        if command_error.code == 20
                            && command_error.message.starts_with("Transaction numbers")
                        {
                            command_error.message = "This MongoDB deployment does not support \
                                                     retryable writes. Please add \
                                                     retryWrites=false to your connection string."
                                .to_string();
                        }
                    }

                    self.inner
                        .topology
                        .updater()
                        .handle_application_error(
                            server_addr.clone(),
                            err.clone(),
                            HandshakePhase::after_completion(&conn),
                        )
                        .await;
                    // release the connection to be processed by the connection pool
                    drop(conn);
                    // release the selected server to decrement its operation count
                    drop(server);

                    if let Some(r) = retry {
                        if (err.is_server_error()
                            || err.is_read_retryable()
                            || err.is_write_retryable())
                            && !err.contains_label("NoWritesPerformed")
                        {
                            return Err(err);
                        } else {
                            return Err(r.first_error);
                        }
                    } else if retryability.can_retry_error(&err) {
                        retry = Some(ExecutionRetry {
                            prior_txn_number: txn_number,
                            first_error: err,
                            first_server: server_addr.clone(),
                        });
                        continue;
                    } else {
                        return Err(err);
                    }
                }
            };
            return Ok(details);
        }
    }

    /// Executes an operation on a given connection, optionally using a provided session.
    pub(crate) async fn execute_operation_on_connection<Op: Operation>(
        &self,
        op: &mut Op,
        connection: &mut PooledConnection,
        session: &mut Option<&mut ClientSession>,
        txn_number: Option<i64>,
        retryability: Retryability,
        effective_criteria: SelectionCriteria,
    ) -> Result<Op::O> {
        loop {
            let cmd = self.build_command(
                op,
                connection,
                session,
                txn_number,
                effective_criteria.clone(),
            )?;

            let connection_info = connection.info();
            let service_id = connection.service_id();
            let request_id = next_request_id();
            let should_redact = cmd.should_redact();
            let cmd_name = cmd.name.clone();
            let target_db = cmd.target_db.clone();
            #[cfg(feature = "opentelemetry")]
            let cmd_attrs = crate::otel::CommandAttributes::new(&cmd);

            let mut message = Message::try_from(cmd)?;
            message.request_id = Some(request_id);

            #[cfg(feature = "opentelemetry")]
            let span = self.start_command_span(
                op,
                &connection_info,
                connection.stream_description()?,
                &message,
                cmd_attrs,
            );

            #[cfg(feature = "in-use-encryption")]
            {
                let guard = self.inner.csfle.read().await;
                if let Some(ref csfle) = *guard {
                    if csfle.opts().bypass_auto_encryption != Some(true) {
                        let encrypted_payload = self
                            .auto_encrypt(csfle, &message.document_payload, &target_db)
                            .await?;
                        message.document_payload = encrypted_payload;
                    }
                }
            }

            self.emit_command_event(|| {
                let command_body = if should_redact {
                    Document::new()
                } else {
                    message.get_command_document()
                };
                CommandEvent::Started(CommandStartedEvent {
                    command: command_body,
                    db: target_db.clone(),
                    command_name: cmd_name.clone(),
                    request_id,
                    connection: connection_info.clone(),
                    service_id,
                })
            })
            .await;

            let start_time = Instant::now();
            let command_result = match connection.send_message(message).await {
                Ok(response) => {
                    match self
                        .parse_response(op, session, connection.is_sharded()?, &response)
                        .await
                    {
                        Ok(()) => Ok(response),
                        Err(error) => Err(error.with_server_response(&response)),
                    }
                }
                Err(err) => Err(err),
            };

            let duration = start_time.elapsed();

            let result = match command_result {
                Err(mut err) => {
                    self.emit_command_event(|| {
                        let mut err = err.clone();
                        if should_redact {
                            err.redact();
                        }

                        CommandEvent::Failed(CommandFailedEvent {
                            duration,
                            command_name: cmd_name.clone(),
                            failure: err,
                            request_id,
                            connection: connection_info.clone(),
                            service_id,
                        })
                    })
                    .await;

                    if let Some(ref mut session) = session {
                        if err.is_network_error() {
                            session.mark_dirty();
                        }
                    }

                    err.add_labels_and_update_pin(
                        Some(connection.stream_description()?),
                        session,
                        Some(retryability),
                    );

                    op.handle_error(err)
                }
                Ok(response) => {
                    self.emit_command_event(|| {
                        let reply = if should_redact {
                            Document::new()
                        } else {
                            response
                                .body()
                                .unwrap_or_else(|e| doc! { "deserialization error": e.to_string() })
                        };

                        CommandEvent::Succeeded(CommandSucceededEvent {
                            duration,
                            reply,
                            command_name: cmd_name.clone(),
                            request_id,
                            connection: connection_info.clone(),
                            service_id,
                        })
                    })
                    .await;

                    #[cfg(feature = "in-use-encryption")]
                    let response = {
                        let guard = self.inner.csfle.read().await;
                        if let Some(ref csfle) = *guard {
                            let new_body = self.auto_decrypt(csfle, response.raw_body()).await?;
                            RawCommandResponse::new_raw(response.source, new_body)
                        } else {
                            response
                        }
                    };

                    let context = ExecutionContext {
                        connection,
                        session: session.as_deref_mut(),
                        effective_criteria: effective_criteria.clone(),
                    };

                    let handle_result = match Op::ZERO_COPY {
                        true => op.handle_response(Cow::Owned(response), context).await,
                        false => op
                            .handle_response(Cow::Borrowed(&response), context)
                            .await
                            .map_err(|e| e.with_server_response(&response)),
                    };
                    match handle_result {
                        Ok(op_out) => Ok(op_out),
                        Err(mut err) => {
                            err.add_labels_and_update_pin(
                                Some(connection.stream_description()?),
                                session,
                                Some(retryability),
                            );
                            Err(err)
                        }
                    }
                }
            };
            #[cfg(feature = "opentelemetry")]
            span.record_command_result::<Op>(&result);

            if result
                .as_ref()
                .err()
                .is_some_and(|e| e.is_reauthentication_required())
            {
                // This retry is done outside of the normal retry loop because all operations,
                // regardless of retryability, should be retried after reauthentication.
                self.reauthenticate_connection(connection).await?;
                continue;
            } else {
                return result;
            }
        }
    }

    fn build_command<T: Operation>(
        &self,
        op: &mut T,
        connection: &mut PooledConnection,
        session: &mut Option<&mut ClientSession>,
        txn_number: Option<i64>,
        effective_criteria: SelectionCriteria,
    ) -> Result<crate::cmap::Command> {
        let stream_description = connection.stream_description()?;
        let is_sharded = stream_description.initial_server_type == ServerType::Mongos;
        // Validate user-set read concern
        if session.as_ref().map_or(false, |s| s.in_transaction()) {
            if matches!(op.read_concern(), Feature::Set(_)) {
                return Err(Error::invalid_argument(
                    "Cannot set read concern after starting a transaction",
                ));
            }
        }
        let mut cmd = op.build(stream_description)?;
        // Clear inherited read concern when in a transaction
        if session.as_ref().map_or(false, |s| s.in_transaction()) {
            cmd.clear_read_concern();
        }
        self.inner.topology.watcher().update_command_with_read_pref(
            connection.address(),
            &mut cmd,
            &effective_criteria,
        );

        match session {
            Some(ref mut session)
                if op.supports_sessions() && op.write_concern().is_acknowledged() =>
            {
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
                else if session.causal_consistency()
                    && matches!(
                        session.transaction.state,
                        TransactionState::None | TransactionState::Starting
                    )
                    && op.read_concern().supported()
                {
                    cmd.set_after_cluster_time(session);
                }

                match session.transaction.state {
                    TransactionState::Starting => {
                        cmd.set_start_transaction();
                        cmd.set_autocommit();
                        if session.causal_consistency() {
                            cmd.set_after_cluster_time(session);
                        }

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
            Some(ref session)
                if !op.write_concern().is_acknowledged() && !session.is_implicit() =>
            {
                return Err(ErrorKind::InvalidArgument {
                    message: "Cannot use ClientSessions with unacknowledged write concern"
                        .to_string(),
                }
                .into());
            }
            _ => {}
        }

        let session_cluster_time = session.as_ref().and_then(|session| session.cluster_time());
        let client_cluster_time = self.inner.topology.watcher().cluster_time();
        let max_cluster_time = std::cmp::max(session_cluster_time, client_cluster_time.as_ref());
        if let Some(cluster_time) = max_cluster_time {
            cmd.set_cluster_time(cluster_time);
        }

        if let Some(ref server_api) = self.inner.options.server_api {
            cmd.set_server_api(server_api);
        }

        Ok(cmd)
    }

    #[cfg(feature = "in-use-encryption")]
    fn auto_encrypt<'a>(
        &'a self,
        csfle: &'a super::csfle::ClientState,
        command: &'a RawDocument,
        target_db: &'a str,
    ) -> BoxFuture<'a, Result<RawDocumentBuf>> {
        Box::pin(async move {
            let ctx = csfle
                .crypt()
                .ctx_builder()
                .build_encrypt(target_db, command)?;
            csfle.exec().run_ctx(ctx, Some(target_db)).await
        })
    }

    #[cfg(feature = "in-use-encryption")]
    fn auto_decrypt<'a>(
        &'a self,
        csfle: &'a super::csfle::ClientState,
        response: &'a RawDocument,
    ) -> BoxFuture<'a, Result<RawDocumentBuf>> {
        Box::pin(async move {
            let ctx = csfle.crypt().ctx_builder().build_decrypt(response)?;
            csfle.exec().run_ctx(ctx, None).await
        })
    }

    async fn reauthenticate_connection(&self, connection: &mut PooledConnection) -> Result<()> {
        let credential =
            self.inner
                .options
                .credential
                .as_ref()
                .ok_or_else(|| ErrorKind::Authentication {
                    message: "the connection requires reauthentication but no credential was set"
                        .to_string(),
                })?;
        let server_api = self.inner.options.server_api.as_ref();

        credential
            .mechanism
            .as_ref()
            .ok_or(ErrorKind::Authentication {
                message: "the connection requires reauthentication but no authentication \
                          mechanism was set"
                    .to_string(),
            })?
            .reauthenticate_stream(connection, credential, server_api)
            .await
    }

    async fn parse_response<T: Operation>(
        &self,
        op: &T,
        session: &mut Option<&mut ClientSession>,
        is_sharded: bool,
        response: &RawCommandResponse,
    ) -> Result<()> {
        let raw_doc = RawDocument::from_bytes(response.as_bytes())?;

        let ok = match raw_doc.get("ok")? {
            Some(b) => {
                crate::bson_util::get_int_raw(b).ok_or_else(|| ErrorKind::InvalidResponse {
                    message: format!("expected ok value to be a number, instead got {b:?}"),
                })?
            }
            None => {
                return Err(ErrorKind::InvalidResponse {
                    message: "missing 'ok' value in response".to_string(),
                }
                .into())
            }
        };

        let cluster_time: Option<ClusterTime> = raw_doc
            .get("$clusterTime")?
            .and_then(RawBsonRef::as_document)
            .map(|d| crate::bson_compat::deserialize_from_slice(d.as_bytes()))
            .transpose()?;

        let at_cluster_time = op.extract_at_cluster_time(raw_doc)?;

        self.update_cluster_time(cluster_time, at_cluster_time, session)
            .await;

        if let (Some(session), Some(ts)) = (
            session.as_mut(),
            raw_doc
                .get("operationTime")?
                .and_then(RawBsonRef::as_timestamp),
        ) {
            session.advance_operation_time(ts);
        }

        if ok == 1 {
            if let Some(ref mut session) = session {
                if is_sharded && session.in_transaction() {
                    let recovery_token = raw_doc
                        .get("recoveryToken")?
                        .and_then(RawBsonRef::as_document)
                        .map(|d| crate::bson_compat::deserialize_from_slice(d.as_bytes()))
                        .transpose()?;
                    session.transaction.recovery_token = recovery_token;
                }
            }

            Ok(())
        } else {
            Err(response
                .body::<CommandErrorBody>()
                .map(|error_response| error_response.into())
                .unwrap_or_else(|e| {
                    Error::from(ErrorKind::InvalidResponse {
                        message: format!("error deserializing command error: {e}"),
                    })
                }))
        }
    }

    async fn select_data_bearing_server(&self, operation_name: &str) -> Result<()> {
        let topology_type = self.inner.topology.watcher().topology_type();
        let criteria = SelectionCriteria::Predicate(Arc::new(move |server_info| {
            let server_type = server_info.server_type();
            (matches!(topology_type, TopologyType::Single) && server_type.is_available())
                || server_type.is_data_bearing()
        }));
        let _ = self
            .select_server(
                Some(&criteria),
                &[],
                crate::client::OpSelectionInfo::new(operation_name),
            )
            .await?;
        Ok(())
    }

    /// Gets whether the topology supports transactions. If it has yet to be determined if the
    /// topology supports transactions, this method will perform a server selection that will force
    /// that determination to be made.
    pub(crate) async fn transaction_support_status(&self) -> Result<TransactionSupportStatus> {
        let initial_status = self.inner.topology.watcher().transaction_support_status();

        // Need to guarantee that we're connected to at least one server that can determine if
        // sessions are supported or not.
        match initial_status {
            TransactionSupportStatus::Undetermined => {
                self.select_data_bearing_server("Check transactions support status")
                    .await?;
                Ok(self.inner.topology.watcher().transaction_support_status())
            }
            _ => Ok(initial_status),
        }
    }

    /// Returns the retryability level for the execution of this operation with the given session
    /// and connection stream description.
    fn get_retryability<T: Operation>(
        &self,
        op: &T,
        session: &Option<&mut ClientSession>,
        stream_description: &StreamDescription,
    ) -> Retryability {
        // commitTransaction and abortTransaction are always retried, regardless of the value of
        // retry_writes.
        if op.name() == CommitTransaction::NAME || op.name() == AbortTransaction::NAME {
            return Retryability::Write;
        }

        if session
            .as_ref()
            .is_some_and(|session| session.in_transaction())
        {
            return Retryability::None;
        }

        match op.retryability().with_options(self.options()) {
            Retryability::Write if stream_description.supports_retryable_writes() => {
                Retryability::Write
            }
            // All servers compatible with the driver support retryable reads.
            Retryability::Read => Retryability::Read,
            _ => Retryability::None,
        }
    }

    async fn update_cluster_time(
        &self,
        cluster_time: Option<ClusterTime>,
        at_cluster_time: Option<Timestamp>,
        session: &mut Option<&mut ClientSession>,
    ) {
        if let Some(ref cluster_time) = cluster_time {
            self.inner
                .topology
                .updater()
                .advance_cluster_time(cluster_time.clone())
                .await;
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
) -> Result<PooledConnection> {
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
        stream_description: Option<&StreamDescription>,
        session: &mut Option<&mut ClientSession>,
        retryability: Option<Retryability>,
    ) {
        let transaction_state = session.as_ref().map_or(&TransactionState::None, |session| {
            &session.transaction.state
        });
        let max_wire_version = stream_description.and_then(|sd| sd.max_wire_version);
        let server_type = stream_description.map(|sd| sd.initial_server_type);

        match transaction_state {
            TransactionState::Starting | TransactionState::InProgress => {
                if self.is_network_error() || self.is_server_selection_error() {
                    self.add_label(TRANSIENT_TRANSACTION_ERROR);
                }
            }
            TransactionState::Committed { .. } => {
                if let Some(max_wire_version) = max_wire_version {
                    if self.should_add_retryable_write_label(max_wire_version, server_type) {
                        self.add_label(RETRYABLE_WRITE_ERROR);
                    }
                }
                if self.should_add_unknown_transaction_commit_result_label() {
                    self.add_label(UNKNOWN_TRANSACTION_COMMIT_RESULT);
                }
            }
            TransactionState::Aborted => {
                if let Some(max_wire_version) = max_wire_version {
                    if self.should_add_retryable_write_label(max_wire_version, server_type) {
                        self.add_label(RETRYABLE_WRITE_ERROR);
                    }
                }
            }
            TransactionState::None => {
                if retryability == Some(Retryability::Write) {
                    if let Some(max_wire_version) = max_wire_version {
                        if self.should_add_retryable_write_label(max_wire_version, server_type) {
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
    }
}

struct ExecutionDetails<T: Operation> {
    output: T::O,
    connection: PooledConnection,
    implicit_session: Option<ClientSession>,
}

#[derive(Debug)]
struct ExecutionRetry {
    prior_txn_number: Option<i64>,
    first_error: Error,
    first_server: ServerAddress,
}

trait RetryHelper {
    fn first_error(&mut self) -> Result<()>;
}

impl RetryHelper for Option<ExecutionRetry> {
    fn first_error(&mut self) -> Result<()> {
        match self.take() {
            Some(r) => Err(r.first_error),
            None => Ok(()),
        }
    }
}

impl Feature<&crate::options::WriteConcern> {
    fn is_acknowledged(&self) -> bool {
        match self {
            Feature::Set(wc) => wc.is_acknowledged(),
            _ => true,
        }
    }
}
