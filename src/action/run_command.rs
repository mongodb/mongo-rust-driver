use std::time::Duration;

use bson::{Bson, Document};

use crate::{
    client::session::TransactionState,
    coll::options::CursorType,
    db::options::{RunCommandOptions, RunCursorCommandOptions},
    error::{ErrorKind, Result},
    operation::{run_command, run_cursor_command},
    selection_criteria::SelectionCriteria,
    ClientSession,
    Cursor,
    Database,
    SessionCursor,
};

use super::{
    action_impl,
    deeplink,
    export_doc,
    option_setters,
    options_doc,
    ExplicitSession,
    ImplicitSession,
};

impl Database {
    /// Runs a database-level command.
    ///
    /// Note that no inspection is done on `doc`, so the command will not use the database's default
    /// read concern or write concern. If specific read concern or write concern is desired, it must
    /// be specified manually.
    /// Please note that run_command doesn't validate WriteConcerns passed into the body of the
    /// command document.
    ///
    /// `await` will return d[`Result<Document>`].
    #[deeplink]
    #[options_doc(run_command)]
    pub fn run_command(&self, command: Document) -> RunCommand {
        RunCommand {
            db: self,
            command,
            options: None,
            session: None,
        }
    }

    /// Runs a database-level command and returns a cursor to the response.
    ///
    /// `await` will return d[`Result<Cursor<Document>>`] or a
    /// d[`Result<SessionCursor<Document>>`] if a [`ClientSession`] is provided.
    #[deeplink]
    #[options_doc(run_cursor_command)]
    pub fn run_cursor_command(&self, command: Document) -> RunCursorCommand {
        RunCursorCommand {
            db: self,
            command,
            options: None,
            session: ImplicitSession,
        }
    }
}

#[cfg(feature = "sync")]
impl crate::sync::Database {
    /// Runs a database-level command.
    ///
    /// Note that no inspection is done on `doc`, so the command will not use the database's default
    /// read concern or write concern. If specific read concern or write concern is desired, it must
    /// be specified manually.
    /// Please note that run_command doesn't validate WriteConcerns passed into the body of the
    /// command document.
    ///
    /// [`run`](RunCommand::run) will return d[`Result<Document>`].
    #[deeplink]
    #[options_doc(run_command, sync)]
    pub fn run_command(&self, command: Document) -> RunCommand {
        self.async_database.run_command(command)
    }

    /// Runs a database-level command and returns a cursor to the response.
    ///
    /// [`run`](RunCursorCommand::run) will return d[`Result<crate::sync::Cursor<Document>>`] or a
    /// d[`Result<crate::sync::SessionCursor<Document>>`] if a [`ClientSession`] is provided.
    #[deeplink]
    #[options_doc(run_cursor_command, sync)]
    pub fn run_cursor_command(&self, command: Document) -> RunCursorCommand {
        self.async_database.run_cursor_command(command)
    }
}

/// Run a database-level command.  Create with [`Database::run_command`].
#[must_use]
pub struct RunCommand<'a> {
    db: &'a Database,
    command: Document,
    options: Option<RunCommandOptions>,
    session: Option<&'a mut ClientSession>,
}

#[option_setters(crate::db::options::RunCommandOptions)]
#[export_doc(run_command)]
impl<'a> RunCommand<'a> {
    /// Run the command using the provided [`ClientSession`].
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

#[action_impl]
impl<'a> Action for RunCommand<'a> {
    type Future = RunCommandFuture;

    async fn execute(self) -> Result<Document> {
        let mut selection_criteria = self.options.and_then(|o| o.selection_criteria);
        if let Some(session) = &self.session {
            match session.transaction.state {
                TransactionState::Starting | TransactionState::InProgress => {
                    if self.command.contains_key("readConcern") {
                        return Err(ErrorKind::InvalidArgument {
                            message: "Cannot set read concern after starting a transaction".into(),
                        }
                        .into());
                    }
                    selection_criteria = match selection_criteria {
                        Some(selection_criteria) => Some(selection_criteria),
                        None => {
                            if let Some(ref options) = session.transaction.options {
                                options.selection_criteria.clone()
                            } else {
                                None
                            }
                        }
                    };
                }
                _ => {}
            }
        }

        let operation = run_command::RunCommand::new(
            self.db.name().into(),
            self.command,
            selection_criteria,
            None,
        )?;
        self.db
            .client()
            .execute_operation(operation, self.session)
            .await
    }
}

/// Runs a database-level command and returns a cursor to the response.  Create with
/// [`Database::run_cursor_command`].
#[must_use]
pub struct RunCursorCommand<'a, Session = ImplicitSession> {
    db: &'a Database,
    command: Document,
    options: Option<RunCursorCommandOptions>,
    session: Session,
}

#[option_setters(crate::db::options::RunCursorCommandOptions)]
#[export_doc(run_cursor_command, extra = [session])]
impl<Session> RunCursorCommand<'_, Session> {}

impl<'a> RunCursorCommand<'a, ImplicitSession> {
    /// Run the command using the provided [`ClientSession`].
    pub fn session(
        self,
        value: impl Into<&'a mut ClientSession>,
    ) -> RunCursorCommand<'a, ExplicitSession<'a>> {
        RunCursorCommand {
            db: self.db,
            command: self.command,
            options: self.options,
            session: ExplicitSession(value.into()),
        }
    }
}

#[action_impl(sync = crate::sync::Cursor<Document>)]
impl<'a> Action for RunCursorCommand<'a, ImplicitSession> {
    type Future = RunCursorCommandFuture;

    async fn execute(self) -> Result<Cursor<Document>> {
        let selection_criteria = self
            .options
            .as_ref()
            .and_then(|options| options.selection_criteria.clone());
        let rcc = run_command::RunCommand::new(
            self.db.name().to_string(),
            self.command,
            selection_criteria,
            None,
        )?;
        let rc_command = run_cursor_command::RunCursorCommand::new(rcc, self.options)?;
        let client = self.db.client();
        client.execute_cursor_operation(rc_command).await
    }
}

#[action_impl(sync = crate::sync::SessionCursor<Document>)]
impl<'a> Action for RunCursorCommand<'a, ExplicitSession<'a>> {
    type Future = RunCursorCommandSessionFuture;

    async fn execute(mut self) -> Result<SessionCursor<Document>> {
        resolve_selection_criteria_with_session!(
            self.db,
            self.options,
            Some(&mut *self.session.0)
        )?;
        let selection_criteria = self
            .options
            .as_ref()
            .and_then(|options| options.selection_criteria.clone());
        let rcc = run_command::RunCommand::new(
            self.db.name().to_string(),
            self.command,
            selection_criteria,
            None,
        )?;
        let rc_command = run_cursor_command::RunCursorCommand::new(rcc, self.options)?;
        let client = self.db.client();
        client
            .execute_session_cursor_operation(rc_command, self.session.0)
            .await
    }
}
