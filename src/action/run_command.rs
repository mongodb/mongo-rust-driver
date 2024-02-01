use bson::Document;

use crate::{
    client::session::TransactionState,
    error::{ErrorKind, Result},
    operation::run_command as op,
    selection_criteria::SelectionCriteria,
    ClientSession,
    Database,
};

use super::action_impl;

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
            selection_criteria: None,
            session: None,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
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
}

/// Run a database-level command.  Create with [`Database::run_command`] and execute with `await`
/// (or [`run`][`RunCommand::run`] if using the sync client).
#[must_use]
pub struct RunCommand<'a> {
    db: &'a Database,
    command: Document,
    selection_criteria: Option<SelectionCriteria>,
    session: Option<&'a mut ClientSession>,
}

impl<'a> RunCommand<'a> {
    /// If the value is `Some`, call the provided function on `self`.  Convenient for chained
    /// updates with values that need to be set conditionally.
    pub fn update_with<Value>(
        self,
        value: Option<Value>,
        f: impl FnOnce(Self, Value) -> Self,
    ) -> Self {
        match value {
            Some(value) => f(self, value),
            None => self,
        }
    }

    /// Set the selection criteria for the command.
    pub fn selection_criteria(mut self, value: SelectionCriteria) -> Self {
        self.selection_criteria = Some(value);
        self
    }

    /// Run the command using the provided [`ClientSession`].
    pub fn session(mut self, value: &'a mut ClientSession) -> Self {
        self.session = Some(value);
        self
    }
}

action_impl! {
    impl Action<'a> for RunCommand<'a> {
        type Future = RunCommandFuture;

        async fn execute(self) -> Result<Document> {
            let mut selection_criteria = self.selection_criteria;
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

            let operation = op::RunCommand::new(
                self.db.name().into(),
                self.command,
                selection_criteria,
                None,
            )?;
            self.db.client().execute_operation(operation, self.session).await
        }
    }
}
