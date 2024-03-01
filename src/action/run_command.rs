use bson::Document;

use crate::{
    client::session::TransactionState,
    db::options::{RunCommandOptions, RunCursorCommandOptions},
    error::{ErrorKind, Result},
    operation::{run_command, run_cursor_command},
    selection_criteria::SelectionCriteria,
    ClientSession,
    Cursor,
    Database,
    SessionCursor,
};

use super::{action_impl, option_setters, ExplicitSession, ImplicitSession};

impl Database {
    /// Runs a database-level command.
    ///
    /// Note that no inspection is done on `doc`, so the command will not use the database's default
    /// read concern or write concern. If specific read concern or write concern is desired, it must
    /// be specified manually.
    /// Please note that run_command doesn't validate WriteConcerns passed into the body of the
    /// command document.
    ///
    /// `await` will return `Result<Document>`.
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
    /// `await` will return `Result<`[`Cursor`]`<Document>>` or a
    /// `Result<`[`SessionCursor`]`<Document>>` if a [`ClientSession`] is provided.
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
    /// [`run`](RunCommand::run) will return `Result<Document>`.
    pub fn run_command(&self, command: Document) -> RunCommand {
        self.async_database.run_command(command)
    }

    /// Runs a database-level command and returns a cursor to the response.
    ///
    /// [`run`](RunCursorCommand::run) will return `Result<`[`Cursor`]`<Document>>` or a
    /// `Result<`[`SessionCursor`]`<Document>>` if a [`ClientSession`] is provided.
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

impl<'a> RunCommand<'a> {
    option_setters!(options: RunCommandOptions;
        selection_criteria: SelectionCriteria,
    );

    /// Run the command using the provided [`ClientSession`].
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

action_impl! {
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
            self.db.client().execute_operation(operation, self.session).await
        }
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

impl<'a, Session> RunCursorCommand<'a, Session> {
    option_setters!(options: RunCursorCommandOptions;
        selection_criteria: SelectionCriteria,
        cursor_type: crate::coll::options::CursorType,
        batch_size: u32,
        max_time: std::time::Duration,
        comment: bson::Bson,
    );
}

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

action_impl! {
    impl<'a> Action for RunCursorCommand<'a, ImplicitSession> {
        type Future = RunCursorCommandFuture;

        async fn execute(self) -> Result<Cursor<Document>> {
            let selection_criteria = self.options
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

        fn sync_wrap(out) -> Result<crate::sync::Cursor<Document>> {
            out.map(crate::sync::Cursor::new)
        }
    }
}

action_impl! {
    impl<'a> Action for RunCursorCommand<'a, ExplicitSession<'a>> {
        type Future = RunCursorCommandSessionFuture;

        async fn execute(mut self) -> Result<SessionCursor<Document>> {
            resolve_selection_criteria_with_session!(self.db, self.options, Some(&mut *self.session.0))?;
            let selection_criteria = self.options
                .as_ref()
                .and_then(|options| options.selection_criteria.clone());
            let rcc = run_command::RunCommand::new(self.db.name().to_string(), self.command, selection_criteria, None)?;
            let rc_command = run_cursor_command::RunCursorCommand::new(rcc, self.options)?;
            let client = self.db.client();
            client
                .execute_session_cursor_operation(rc_command, self.session.0)
                .await
        }

        fn sync_wrap(out) -> Result<crate::sync::SessionCursor<Document>> {
            out.map(crate::sync::SessionCursor::new)
        }
    }
}
