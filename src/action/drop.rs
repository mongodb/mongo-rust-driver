use std::future::IntoFuture;

use futures_util::FutureExt;

use crate::BoxFuture;
use crate::{db::options::DropDatabaseOptions, options::WriteConcern, ClientSession, Database};
use crate::error::Result;

use super::option_setters;

impl Database {
    /// Drops the database, deleting all data, collections, and indexes stored in it.
    pub fn drop_2(&self) -> DropDatabase {
        DropDatabase { db: self, options: None, session: None }
    }
}

/// Drops the database, deleting all data, collections, and indexes stored in it.  Create by calling [`Database::drop`] and execute with `await` (or [`run`](DropDatabase::run) if using the sync client).
#[must_use]
pub struct DropDatabase<'a> {
    db: &'a Database,
    options: Option<DropDatabaseOptions>,
    session: Option<&'a mut ClientSession>,
}

impl<'a> DropDatabase<'a> {
    option_setters!(options: DropDatabaseOptions;
        /// The write concern for the operation.
        write_concern: WriteConcern,
    );

    /// Runs the drop using the provided session.
    pub fn session(mut self, value: impl Into<&'a mut ClientSession>) -> Self {
        self.session = Some(value.into());
        self
    }
}

impl<'a> IntoFuture for DropDatabase<'a> {
    type Output = Result<()>;
    type IntoFuture = DropDatabaseFuture<'a>;

    fn into_future(mut self) -> Self::IntoFuture {
        DropDatabaseFuture(async move {
            resolve_options!(self.db, self.options, [write_concern]);
            let op = crate::operation::DropDatabase::new(self.db.name().to_string(), self.options);
            self.db.client()
                .execute_operation(op, self.session)
                .await
        }.boxed())
    }
}

pub struct DropDatabaseFuture<'a>(BoxFuture<'a, Result<()>>);

impl<'a> std::future::Future for DropDatabaseFuture<'a> {
    type Output = Result<()>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}