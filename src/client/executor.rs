use super::{Client, ClientSession};

use std::{collections::HashSet, sync::Arc};

use lazy_static::lazy_static;
use std::time::Instant;

use crate::{
    bson::Document,
    cmap::Connection,
    error::{Error, ErrorKind, Result},
    event::command::{CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent},
    operation::{Operation, Retryability},
    options::SelectionCriteria,
    sdam::{HandshakePhase, SelectedServer, SessionSupportStatus},
};

lazy_static! {
    static ref REDACTED_COMMANDS: HashSet<&'static str> = {
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
        // TODO RUST-9: allow unacknowledged write concerns
        if !op.is_acknowledged() {
            return Err(ErrorKind::ArgumentError {
                message: "Unacknowledged write concerns are not supported".to_string(),
            }
            .into());
        }
        match session.into() {
            Some(session) => self.execute_operation_with_retry(op, Some(session)).await,
            None => {
                let mut implicit_session = self.start_implicit_session(&op).await?;
                self.execute_operation_with_retry(op, implicit_session.as_mut())
                    .await
            }
        }
    }

    /// Execute the given operation, returning the implicit session created for it if one was.
    ///
    /// Server selection be will performed using the criteria specified on the operation, if any.
    pub(crate) async fn execute_cursor_operation<T: Operation>(
        &self,
        op: T,
    ) -> Result<(T::O, Option<ClientSession>)> {
        let mut implicit_session = self.start_implicit_session(&op).await?;
        self.execute_operation_with_retry(op, implicit_session.as_mut())
            .await
            .map(|result| (result, implicit_session))
    }

    /// Selects a server and executes the given operation on it, optionally using a provided
    /// session. Retries the operation upon failure if retryability is supported.
    async fn execute_operation_with_retry<T: Operation>(
        &self,
        op: T,
        mut session: Option<&mut ClientSession>,
    ) -> Result<T::O> {
        let server = self.select_server(op.selection_criteria()).await?;

        let mut conn = server.pool.check_out().await?;

        let retryability = self.get_retryability(&conn, &op).await?;

        let txn_number = match session {
            Some(ref mut session) if retryability == Retryability::Write => {
                Some(session.get_and_increment_txn_number())
            }
            _ => None,
        };

        let first_error = match self
            .execute_operation_on_connection(&op, &mut conn, &mut session, txn_number)
            .await
        {
            Ok(result) => {
                return Ok(result);
            }
            Err(err) => {
                // For a pre-4.4 connection, an error label should be added to any write-retryable
                // error as long as the retry_writes client option is not set to false. For a 4.4+
                // connection, a label should be added only to network errors.
                let mut err = match retryability {
                    Retryability::Write => get_error_with_retryable_write_label(&conn, err).await?,
                    _ => err,
                };

                // Retryable writes are only supported by storage engines with document-level
                // locking, so users need to disable retryable writes if using mmapv1.
                if let ErrorKind::CommandError(ref mut command_error) = err.kind {
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

                // TODO RUST-90: Do not retry read if session is in a transaction
                if retryability == Retryability::Read && err.is_read_retryable()
                    || retryability == Retryability::Write && err.is_write_retryable()
                {
                    err
                } else {
                    return Err(err);
                }
            }
        };

        let server = match self.select_server(op.selection_criteria()).await {
            Ok(server) => server,
            Err(_) => {
                return Err(first_error);
            }
        };

        let mut conn = match server.pool.check_out().await {
            Ok(c) => c,
            Err(_) => return Err(first_error),
        };

        let retryability = self.get_retryability(&conn, &op).await?;
        if retryability == Retryability::None {
            return Err(first_error);
        }

        match self
            .execute_operation_on_connection(&op, &mut conn, &mut session, txn_number)
            .await
        {
            Ok(result) => Ok(result),
            Err(err) => {
                let err = match retryability {
                    Retryability::Write => get_error_with_retryable_write_label(&conn, err).await?,
                    _ => err,
                };
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
        op: &T,
        connection: &mut Connection,
        session: &mut Option<&mut ClientSession>,
        txn_number: Option<u64>,
    ) -> Result<T::O> {
        if let Some(wc) = op.write_concern() {
            wc.validate()?;
        }

        let mut cmd = op.build(connection.stream_description()?)?;
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
                session.update_last_use();
            }
            Some(ref session) if !op.supports_sessions() && !session.is_implicit() => {
                return Err(ErrorKind::ArgumentError {
                    message: format!("{} does not support sessions", cmd.name),
                }
                .into());
            }
            Some(ref session) if !op.is_acknowledged() && !session.is_implicit() => {
                return Err(ErrorKind::ArgumentError {
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
        let request_id = crate::cmap::conn::next_request_id();

        if let Some(ref server_api) = self.inner.options.server_api {
            cmd.set_server_api(server_api);
        }

        self.emit_command_event(|handler| {
            let should_redact = REDACTED_COMMANDS.contains(cmd.name.to_lowercase().as_str());

            let command_body = if should_redact {
                Document::new()
            } else {
                cmd.body.clone()
            };
            let command_started_event = CommandStartedEvent {
                command: command_body,
                db: cmd.target_db.clone(),
                command_name: cmd.name.clone(),
                request_id,
                connection: connection_info.clone(),
            };

            handler.handle_command_started_event(command_started_event);
        });

        let start_time = Instant::now();

        let response_result = match connection.send_command(cmd.clone(), request_id).await {
            Ok(response) => {
                if let Some(cluster_time) = response.cluster_time() {
                    self.inner.topology.advance_cluster_time(cluster_time).await;
                    if let Some(ref mut session) = session {
                        session.advance_cluster_time(cluster_time)
                    }
                }
                response.validate().map(|_| response)
            }
            err => err,
        };

        let duration = start_time.elapsed();

        match response_result {
            Err(error) => {
                self.emit_command_event(|handler| {
                    let command_failed_event = CommandFailedEvent {
                        duration,
                        command_name: cmd.name,
                        failure: error.clone(),
                        request_id,
                        connection: connection_info,
                    };

                    handler.handle_command_failed_event(command_failed_event);
                });

                if let Some(session) = session {
                    if error.is_network_error() {
                        session.mark_dirty();
                    }
                }

                op.handle_error(error)
            }
            Ok(response) => {
                self.emit_command_event(|handler| {
                    let should_redact =
                        REDACTED_COMMANDS.contains(cmd.name.to_lowercase().as_str());
                    let reply = if should_redact {
                        Document::new()
                    } else {
                        response.raw_response.clone()
                    };

                    let command_succeeded_event = CommandSucceededEvent {
                        duration,
                        reply,
                        command_name: cmd.name.clone(),
                        request_id,
                        connection: connection_info,
                    };
                    handler.handle_command_succeeded_event(command_succeeded_event);
                });

                op.handle_response(response, connection.stream_description()?)
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

    /// Gets whether the topology supports sessions, and if so, returns the topology's logical
    /// session timeout. If it has yet to be determined if the topology supports sessions, this
    /// method will perform a server selection that will force that determination to be made.
    pub(crate) async fn get_session_support_status(&self) -> Result<SessionSupportStatus> {
        let initial_status = self.inner.topology.session_support_status().await;

        // Need to guarantee that we're connected to at least one server that can determine if
        // sessions are supported or not.
        match initial_status {
            SessionSupportStatus::Undetermined => {
                let criteria = SelectionCriteria::Predicate(Arc::new(move |server_info| {
                    server_info.server_type().is_data_bearing()
                }));
                let _: SelectedServer = self.select_server(Some(&criteria)).await?;
                Ok(self.inner.topology.session_support_status().await)
            }
            _ => Ok(initial_status),
        }
    }

    /// Returns the retryability level for the execution of this operation.
    async fn get_retryability<T: Operation>(
        &self,
        conn: &Connection,
        op: &T,
    ) -> Result<Retryability> {
        match op.retryability() {
            Retryability::Read if self.inner.options.retry_reads != Some(false) => {
                Ok(Retryability::Read)
            }
            Retryability::Write
                if self.inner.options.retry_writes != Some(false)
                    && conn.stream_description()?.supports_retryable_writes() =>
            {
                Ok(Retryability::Write)
            }
            _ => Ok(Retryability::None),
        }
    }
}

/// Returns an Error with a "RetryableWriteError" label added if necessary. On a pre-4.4
/// connection, a label should be added to any write-retryable error. On a 4.4+ connection, a
/// label should only be added to network errors. Regardless of server version, a label should
/// only be added if the `retry_writes` client option is not set to `false`.
async fn get_error_with_retryable_write_label(conn: &Connection, err: Error) -> Result<Error> {
    if let Some(max_wire_version) = conn.stream_description()?.max_wire_version {
        if err.should_add_retryable_write_label(max_wire_version) {
            return Ok(err.with_label("RetryableWriteError"));
        }
    }
    Ok(err)
}
