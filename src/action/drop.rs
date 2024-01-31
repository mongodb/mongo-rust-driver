use crate::{
    db::options::DropDatabaseOptions,
    error::Result,
    operation::drop_database as op,
    options::WriteConcern,
    ClientSession,
    Database,
};

use super::{action_impl, option_setters};

impl Database {
    /// Drops the database, deleting all data, collections, and indexes stored in it.
    ///
    /// `await` will return `Result<()>`.
    pub fn drop(&self) -> DropDatabase {
        DropDatabase {
            db: self,
            options: None,
            session: None,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl crate::sync::Database {
    /// Drops the database, deleting all data, collections, and indexes stored in it.
    ///
    /// [`run`](DropDatabase::run) will return `Result<()>`.
    pub fn drop(&self) -> DropDatabase {
        self.async_database.drop()
    }
}

/// Drops the database, deleting all data, collections, and indexes stored in it.  Create by calling
/// [`Database::drop`] and execute with `await` (or [`run`](DropDatabase::run) if using the sync
/// client).
#[must_use]
pub struct DropDatabase<'a> {
    db: &'a Database,
    options: Option<DropDatabaseOptions>,
    session: Option<&'a mut ClientSession>,
}

impl<'a> DropDatabase<'a> {
    option_setters!(options: DropDatabaseOptions;
        write_concern: WriteConcern,
    );

    /// Runs the drop using the provided session.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

action_impl! {
    impl Action<'a> for DropDatabase<'a> {
        type Future = DropDatabaseFuture;

        async fn execute(mut self) -> Result<()> {
            resolve_options!(self.db, self.options, [write_concern]);
            let op = op::DropDatabase::new(self.db.name().to_string(), self.options);
            self.db.client()
                .execute_operation(op, self.session)
                .await
        }
    }
}
