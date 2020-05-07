use super::{Client, ClientSession};

use std::{collections::HashSet, sync::Arc};

use bson::Document;
use lazy_static::lazy_static;
use time::PreciseTime;

use crate::{
    cmap::Connection,
    error::{ErrorKind, Result},
    event::command::{CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent},
    operation::Operation,
    options::SelectionCriteria,
    sdam::{Server, SessionSupportStatus},
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
    /// sessions.
    pub(crate) async fn execute_operation<T: Operation>(&self, op: T) -> Result<T::O> {
        let mut implicit_session = self.start_implicit_session(&op).await?;
        self.select_server_and_execute_operation(op, implicit_session.as_mut())
            .await
    }

    /// Execute the given operation, returning the implicit session created for it if one was.
    ///
    /// Server selection be will performed using the criteria specified on the operation, if any.
    pub(crate) async fn execute_cursor_operation<T: Operation>(
        &self,
        op: T,
    ) -> Result<(T::O, Option<ClientSession>)> {
        let mut implicit_session = self.start_implicit_session(&op).await?;
        self.select_server_and_execute_operation(op, implicit_session.as_mut())
            .await
            .map(|result| (result, implicit_session))
    }

    /// Execute the given operation with the given session.
    /// Server selection will performed using the criteria specified on the operation, if any.
    pub(crate) async fn execute_operation_with_session<T: Operation>(
        &self,
        op: T,
        session: &mut ClientSession,
    ) -> Result<T::O> {
        self.select_server_and_execute_operation(op, Some(session))
            .await
    }

    /// Selects a server and executes the given operation on it, optionally using a provided
    /// session.
    ///
    /// TODO: RUST-128: replace this with `execute_operation_with_retry` when implemented.
    async fn select_server_and_execute_operation<T: Operation>(
        &self,
        op: T,
        session: Option<&mut ClientSession>,
    ) -> Result<T::O> {
        let server = self.select_server(op.selection_criteria()).await?;

        let mut conn = match server.checkout_connection().await {
            Ok(conn) => conn,
            Err(err) => {
                self.inner
                    .topology
                    .handle_pre_handshake_error(err.clone(), server.address.clone())
                    .await;
                return Err(err);
            }
        };

        match self
            .execute_operation_on_connection(op, &mut conn, session)
            .await
        {
            Ok(result) => Ok(result),
            Err(err) => {
                self.inner
                    .topology
                    .handle_post_handshake_error(err.clone(), conn, server)
                    .await;
                Err(err)
            }
        }
    }

    /// Executes an operation on a given connection, optionally using a provided session.
    async fn execute_operation_on_connection<T: Operation>(
        &self,
        op: T,
        connection: &mut Connection,
        mut session: Option<&mut ClientSession>,
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

        let start_time = PreciseTime::now();

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

        let end_time = PreciseTime::now();
        let duration = start_time.to(end_time).to_std()?;

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

                Err(error)
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

                op.handle_response(response)
            }
        }
    }

    /// Start an implicit session if the operation and write concern are compatible with sessions.
    async fn start_implicit_session<T: Operation>(&self, op: &T) -> Result<Option<ClientSession>> {
        match self.get_session_support_status().await? {
            SessionSupportStatus::Supported {
                logical_session_timeout,
            } if op.supports_sessions() && op.is_acknowledged() => Ok(Some(
                self.start_implicit_session_with_timeout(logical_session_timeout)
                    .await,
            )),
            _ => Ok(None),
        }
    }

    /// Gets whether the topology supports sessions, and if so, returns the topology's logical
    /// session timeout. If it has yet to be determined if the topology supports sessions, this
    /// method will perform a server selection that will force that determination to be made.
    async fn get_session_support_status(&self) -> Result<SessionSupportStatus> {
        let initial_status = self.inner.topology.session_support_status().await;

        // Need to guarantee that we're connected to at least one server that can determine if
        // sessions are supported or not.
        match initial_status {
            SessionSupportStatus::Undetermined => {
                let criteria = SelectionCriteria::Predicate(Arc::new(move |server_info| {
                    server_info.server_type().is_data_bearing()
                }));
                let _: Arc<Server> = self.select_server(Some(&criteria)).await?;
                Ok(self.inner.topology.session_support_status().await)
            }
            _ => Ok(initial_status),
        }
    }
}
